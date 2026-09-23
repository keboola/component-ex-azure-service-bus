"""Entity reference, metadata and management helpers (Task 5, spec §4-L, §5.4, §6.7, §6.9, §6.11).

``EntityRef`` is the immutable, hashable identity of the row's source entity (queue / subscription,
with or without a sub-queue); it knows its SDK path, its derived output table name and how to open
its receiver. ``EntityInfo`` carries the management-derived (or heuristic-fallback) metadata the
receive loop needs -- ``requires_session``, partitioning, lock duration and max delivery count.
Management access is optional for a run: every read here that touches the management plane degrades
to defaults rather than failing the row, except the two explicit sync-action / pre-check helpers
(``list_entity_names``, ``probe_management``, ``describe_entity``) that ask the user for the metadata
directly and must report a denied or missing entity as an actionable error.
"""

import logging
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal, NamedTuple

from azure.core.exceptions import AzureError
from azure.servicebus import ServiceBusClient, ServiceBusReceiver, ServiceBusSubQueue
from azure.servicebus.management import ServiceBusAdministrationClient
from keboola.component.exceptions import UserException

from client import ServiceBusConnector, is_management_denied, redact_secrets, to_user_exception
from configuration import AuthType, EntityType, SourceConfig, SubQueue

logger = logging.getLogger(__name__)

_TABLE_NAME_INVALID_RE = re.compile(r"[^A-Za-z0-9_-]")

_SUB_QUEUE_SUFFIXES = {
    SubQueue.DEAD_LETTER: "/$DeadLetterQueue",
    SubQueue.TRANSFER_DEAD_LETTER: "/$Transfer/$DeadLetterQueue",
}
_SUB_QUEUE_SDK = {
    SubQueue.DEAD_LETTER: ServiceBusSubQueue.DEAD_LETTER,
    SubQueue.TRANSFER_DEAD_LETTER: ServiceBusSubQueue.TRANSFER_DEAD_LETTER,
}
_SUB_QUEUE_TABLE_SUFFIX = {
    SubQueue.DEAD_LETTER: "dead_letter",
    SubQueue.TRANSFER_DEAD_LETTER: "transfer_dead_letter",
}
# §6.7 heuristic fallback used both as EntityInfo's defaults and when the server unexpectedly omits a value.
_DEFAULT_LOCK_DURATION_SECONDS = 60.0
_DEFAULT_MAX_DELIVERY_COUNT = 10


def partition_of(sequence_number: int) -> int:
    """The partition id encoded in a partitioned entity's sequence number (top 16 bits, §6.7)."""
    return sequence_number >> 48


@dataclass(frozen=True)
class EntityRef:
    """The row's source entity identity: independent of auth, stable across a run."""

    entity_type: EntityType
    queue_name: str | None
    topic_name: str | None
    subscription_name: str | None
    sub_queue: SubQueue = SubQueue.NONE

    @classmethod
    def from_source(cls, source: SourceConfig) -> EntityRef:
        return cls(
            entity_type=source.entity_type,
            queue_name=source.queue_name,
            topic_name=source.topic_name,
            subscription_name=source.subscription_name,
            sub_queue=source.sub_queue,
        )

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> EntityRef:
        return cls(
            entity_type=EntityType(d["entity_type"]),
            queue_name=d.get("queue_name"),
            topic_name=d.get("topic_name"),
            subscription_name=d.get("subscription_name"),
            sub_queue=SubQueue(d.get("sub_queue", SubQueue.NONE)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "entity_type": self.entity_type.value,
            "queue_name": self.queue_name,
            "topic_name": self.topic_name,
            "subscription_name": self.subscription_name,
            "sub_queue": self.sub_queue.value,
        }

    @property
    def main_path(self) -> str:
        """The SDK entity path without any sub-queue suffix."""
        if self.entity_type is EntityType.QUEUE:
            return self.queue_name or ""
        return f"{self.topic_name}/Subscriptions/{self.subscription_name}"

    @property
    def is_sub_queue(self) -> bool:
        return self.sub_queue is not SubQueue.NONE

    @property
    def path(self) -> str:
        """The SDK entity path, e.g. ``orders``, ``orders/$DeadLetterQueue``,
        ``orders/$Transfer/$DeadLetterQueue`` or ``t/Subscriptions/s``."""
        return self.main_path + _SUB_QUEUE_SUFFIXES.get(self.sub_queue, "")

    def open_receiver(self, client: ServiceBusClient, **kwargs: Any) -> ServiceBusReceiver:
        """Open the configured receiver on ``client``. The only ``ValueError`` mapping outside
        ``ServiceBusConnector`` (§6.11): the connection string's ``EntityPath`` names another entity."""
        extra = dict(kwargs)
        if self.sub_queue is not SubQueue.NONE:
            extra["sub_queue"] = _SUB_QUEUE_SDK[self.sub_queue]
        try:
            if self.entity_type is EntityType.QUEUE:
                return client.get_queue_receiver(queue_name=self.queue_name or "", **extra)
            return client.get_subscription_receiver(
                topic_name=self.topic_name or "", subscription_name=self.subscription_name or "", **extra
            )
        except ValueError as e:
            # `str(e)` is the SDK's own client-side message (it names the mismatched EntityPath, never
            # echoes the connection string), so `redact_secrets` needs no `secrets=` list here -- unlike
            # `to_user_exception`, which redacts raw SDK-error text that can quote request details.
            raise UserException(
                f"The connection string is scoped to another entity (EntityPath) than '{self.path}'. Use a "
                f"namespace-level connection string or select the entity named in it. (details: {redact_secrets(str(e))})"
            ) from e

    def default_table_name(self) -> str:
        """queue -> ``<queue>``; subscription -> ``<topic>_<subscription>``; plus ``_dead_letter`` /
        ``_transfer_dead_letter`` for sub-queues; sanitised to ``[A-Za-z0-9_-]``, trimmed (§6.9)."""
        if self.entity_type is EntityType.QUEUE:
            parts = [self.queue_name or ""]
        else:
            parts = [self.topic_name or "", self.subscription_name or ""]
        if self.sub_queue in _SUB_QUEUE_TABLE_SUFFIX:
            parts.append(_SUB_QUEUE_TABLE_SUFFIX[self.sub_queue])
        name = _TABLE_NAME_INVALID_RE.sub("_", "_".join(parts)).strip("_-")
        return name or "messages"


@dataclass
class EntityInfo:
    """Management-derived (or heuristic-fallback) metadata the receive loop needs (§6.7)."""

    requires_session: bool | None = None
    partitioned: bool | None = None
    lock_duration_seconds: float = _DEFAULT_LOCK_DURATION_SECONDS
    max_delivery_count: int = _DEFAULT_MAX_DELIVERY_COUNT
    counts: dict[str, int] | None = None
    seen_partitioned: bool = False

    def note_sequence_number(self, seq: int) -> None:
        """Feed the heuristic fallback used when ``partitioned`` is unknown."""
        if partition_of(seq) != 0:
            self.seen_partitioned = True

    @property
    def is_partitioned(self) -> bool:
        return self.partitioned is True or self.seen_partitioned

    @property
    def orphan_guard_threshold(self) -> int:
        return self.max_delivery_count - 1


class _EntityMetadata(NamedTuple):
    """The management fields both ``load_entity_info`` and ``describe_entity`` need. Built by the
    entity-type-specific loader below so each stays responsible for its own runtime-properties shape
    (a subscription's has no ``scheduled_message_count``, §4-L)."""

    requires_session: bool | None
    partitioned: bool | None
    lock_duration_seconds: float
    max_delivery_count: int
    counts: dict[str, int]


def _lock_duration_seconds(lock_duration: timedelta | str | None) -> float:
    """The management client always hands back a ``timedelta`` for a value read from the server;
    the ``str`` arm of the SDK's type only matters for values a caller is about to *write*."""
    return lock_duration.total_seconds() if isinstance(lock_duration, timedelta) else _DEFAULT_LOCK_DURATION_SECONDS


def _count(value: int | None) -> int:
    """Every ``*_message_count`` on the SDK's runtime-properties types is ``Optional[int]``; a
    missing count reads as zero rather than widening every consumer's type to ``int | None``."""
    return value or 0


def _load_queue_metadata(admin: ServiceBusAdministrationClient, entity: EntityRef) -> _EntityMetadata:
    queue_name = entity.queue_name or ""
    props = admin.get_queue(queue_name)
    runtime = admin.get_queue_runtime_properties(queue_name)
    return _EntityMetadata(
        requires_session=props.requires_session,
        partitioned=props.enable_partitioning,
        lock_duration_seconds=_lock_duration_seconds(props.lock_duration),
        max_delivery_count=props.max_delivery_count or _DEFAULT_MAX_DELIVERY_COUNT,
        counts={
            "active": _count(runtime.active_message_count),
            "dead_letter": _count(runtime.dead_letter_message_count),
            "scheduled": _count(runtime.scheduled_message_count),
            "transfer_dead_letter": _count(runtime.transfer_dead_letter_message_count),
        },
    )


def _load_subscription_metadata(admin: ServiceBusAdministrationClient, entity: EntityRef) -> _EntityMetadata:
    topic_name, subscription_name = entity.topic_name or "", entity.subscription_name or ""
    props = admin.get_subscription(topic_name, subscription_name)
    topic = admin.get_topic(topic_name)
    runtime = admin.get_subscription_runtime_properties(topic_name, subscription_name)
    return _EntityMetadata(
        requires_session=props.requires_session,
        partitioned=topic.enable_partitioning,
        lock_duration_seconds=_lock_duration_seconds(props.lock_duration),
        max_delivery_count=props.max_delivery_count or _DEFAULT_MAX_DELIVERY_COUNT,
        counts={
            "active": _count(runtime.active_message_count),
            "dead_letter": _count(runtime.dead_letter_message_count),
            # SubscriptionRuntimeProperties has no scheduled_message_count (verified 7.14.3): a
            # scheduled message sits at the topic until it activates, so a subscription never holds one.
            "scheduled": 0,
            "transfer_dead_letter": _count(runtime.transfer_dead_letter_message_count),
        },
    )


def _load_metadata(admin: ServiceBusAdministrationClient, entity: EntityRef) -> _EntityMetadata:
    if entity.entity_type is EntityType.QUEUE:
        return _load_queue_metadata(admin, entity)
    return _load_subscription_metadata(admin, entity)


def load_entity_info(connector: ServiceBusConnector, entity: EntityRef) -> EntityInfo:
    """Pre-check metadata for the receive loop (L2). Management access is optional for a run: **any**
    exception (denied rights, an entity not yet visible, a transient management error, ...) falls back
    to the heuristic defaults instead of failing the row."""
    try:
        with connector.admin_client() as admin:
            metadata = _load_metadata(admin, entity)
            return EntityInfo(
                requires_session=metadata.requires_session,
                partitioned=metadata.partitioned,
                lock_duration_seconds=metadata.lock_duration_seconds,
                max_delivery_count=metadata.max_delivery_count,
                counts=metadata.counts,
            )
    except Exception as e:  # noqa: BLE001 -- management access is optional for a run; any failure (denied
        # rights, an entity not yet visible, a transient error, or a bug in the unpacking above) falls back
        # to the heuristic defaults rather than failing the row.
        logger.debug(
            "Could not load management metadata for '%s', falling back to defaults: %s",
            entity.path,
            redact_secrets(str(e), connector.secrets),
        )
        return EntityInfo()


def log_entity_counts(info: EntityInfo, entity: EntityRef) -> None:
    """L1: one INFO line with the entity's message counts; nothing when management access was denied
    or failed (``info.counts is None``)."""
    if info.counts is None:
        return
    counts = info.counts
    logger.info(
        "Entity '%s' holds %s active, %s dead-lettered, %s scheduled and %s transfer-dead-lettered message(s).",
        entity.path,
        counts["active"],
        counts["dead_letter"],
        counts["scheduled"],
        counts["transfer_dead_letter"],
    )


def list_entity_names(
    connector: ServiceBusConnector,
    kind: Literal["queues", "topics", "subscriptions"],
    topic_name: str | None = None,
) -> list[str]:
    """Sorted names for the ``list*`` sync actions (§5.4). A Listen-only SAS is denied management
    reads by design, so it returns an empty list (the creatable select still lets the user type a
    name) rather than failing; a service principal without the Data Receiver role is a real error."""
    if kind == "subscriptions" and not topic_name:
        raise UserException("Select a topic first.")
    try:
        with connector.admin_client() as admin:
            if kind == "queues":
                names = [item.name for item in admin.list_queues()]
            elif kind == "topics":
                names = [item.name for item in admin.list_topics()]
            else:
                names = [item.name for item in admin.list_subscriptions(topic_name or "")]
        return sorted(names)
    except AzureError as e:
        if is_management_denied(e) and connector.auth_type == AuthType.CONNECTION_STRING:
            return []
        raise to_user_exception(e, topic_name, connector.secrets) from e


def probe_management(connector: ServiceBusConnector) -> None:
    """The root ``testConnection`` (no source): lists the first queue to prove SP / Manage-SAS auth."""
    try:
        with connector.admin_client() as admin:
            next(iter(admin.list_queues()), None)
    except AzureError as e:
        if is_management_denied(e):
            if connector.auth_type == AuthType.CONNECTION_STRING:
                raise UserException(
                    "A connection string with only Listen rights can be tested only from a row that has a "
                    "source selected."
                ) from e
            raise UserException(
                "The service principal cannot read the namespace: grant it the 'Azure Service Bus Data Receiver' role."
            ) from e
        raise to_user_exception(e, None, connector.secrets) from e


def _rule_filter_text(rule_filter: Any) -> str:
    """``name: sql_expression`` for a SQL (or ``$Default``/``TrueRuleFilter``) rule; ``str(filter)``
    for anything else, e.g. a ``CorrelationRuleFilter``."""
    sql_expression = getattr(rule_filter, "sql_expression", None)
    return sql_expression if sql_expression is not None else str(rule_filter)


def describe_entity(connector: ServiceBusConnector, entity: EntityRef) -> str:
    """The ``entityInfo`` sync action: a markdown bullet list of the entity's management metadata."""
    try:
        with connector.admin_client() as admin:
            metadata = _load_metadata(admin, entity)
            rules = (
                list(admin.list_rules(entity.topic_name or "", entity.subscription_name or ""))
                if entity.entity_type is EntityType.SUBSCRIPTION
                else None
            )
    except AzureError as e:
        if is_management_denied(e):
            raise UserException(
                "Entity details need a connection string with Manage rights or a service principal with the "
                "'Azure Service Bus Data Receiver' role."
            ) from e
        raise to_user_exception(e, entity.path, connector.secrets) from e

    counts = metadata.counts
    lines = [
        f"- Requires session: {metadata.requires_session}",
        f"- Partitioned: {metadata.partitioned}",
        f"- Lock duration: {metadata.lock_duration_seconds} seconds",
        f"- Max delivery count: {metadata.max_delivery_count}",
        f"- Active messages: {counts['active']}",
        f"- Dead-lettered messages: {counts['dead_letter']}",
        f"- Scheduled messages: {counts['scheduled']}",
        f"- Transfer-dead-lettered messages: {counts['transfer_dead_letter']}",
    ]
    if rules is not None:
        lines.append("- Rules:")
        lines.extend(f"  - {rule.name}: {_rule_filter_text(rule.filter)}" for rule in rules)
    return "\n".join(lines)
