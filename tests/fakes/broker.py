"""``FakeBroker``: an in-repo double of the azure-servicebus 7.14.3 SDK surface this extractor uses.

Service Bus is AMQP, so ``vcrpy`` cannot record it; the unit tests and the functional suite run the
real component code against this fake instead (writer precedent). It models the broker behaviour
the spec and the research observed live (spec §6.2-§6.7, research §2.3, §3, §5.1, §7) plus the
SDK's client-side checks. Argument validation is delegated to real SDK objects built offline and
closed at once: the connection-string parser, the ``ServiceBusClient`` / ``get_*_receiver`` keyword
checks (``EntityPath`` mismatch, ``retry_total`` on a receiver, ``sub_queue`` with ``session_id``)
and ``ClientSecretCredential``'s argument checks. No socket is opened, no thread is started and
nothing sleeps: time moves only through :class:`FakeClock`.

Modelling notes beyond the Task-4 table:
- Partitioned sequence numbers count their low 48 bits per partition ("low bits restart at 1"
  [live]); sub-queues keep the original number.
- Locks lapse lazily: every operation first expires lapsed locks. A lapsed ACTIVE message gets
  ``delivery_count += 1`` and at ``max_delivery_count`` moves to the DLQ (``MaxDeliveryCountExceeded``).
  Message locks of a closed receiver stay until they lapse (7.14.3 does not release them on close);
  closing a session receiver, or letting its session lock lapse, releases the session together
  with its unsettled messages.
- Receivers open on ``__enter__`` or on their first operation, as the SDK does; that is where
  entity and session errors surface (``auth_failure`` also fails every operation of an open one).
- A lock is lapsed when ``locked_until <= now``, the SDK's client-side rule.
- Settles are pre-settled as on 7.14.3: the SDK's client-side checks raise; anything the broker
  would reject (a stale lock token, dead-lettering a sub-queue message) is silently ignored.
- A DATA or SEQUENCE ``body`` is a fresh generator of sections on every access, as in the SDK.
- ``active_message_count`` includes DEFERRED, locked and expired-not-purged messages [inferred];
  a session-enabled subscription dead-letters a message sent without ``session_id`` [live];
  ``get_subscription`` has no ``enable_partitioning`` (neither has ``SubscriptionProperties``;
  it is read from ``get_topic``).
"""

import copy
import itertools
import warnings
from collections import defaultdict, deque
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Any, ClassVar, Literal, Self
from uuid import UUID

import pytest
from azure.core.exceptions import ClientAuthenticationError, ResourceNotFoundError
from azure.identity import ClientSecretCredential as RealClientSecretCredential
from azure.servicebus import (
    NEXT_AVAILABLE_SESSION,
    ServiceBusMessageState,
    ServiceBusReceiveMode,
    ServiceBusSessionFilter,
    ServiceBusSubQueue,
)
from azure.servicebus import ServiceBusClient as RealServiceBusClient
from azure.servicebus.amqp import AmqpAnnotatedMessage, AmqpMessageBodyType
from azure.servicebus.exceptions import (
    MessageAlreadySettled,
    MessageLockLostError,
    MessageNotFoundError,
    MessagingEntityNotFoundError,
    OperationTimeoutError,
    ServiceBusAuthenticationError,
    ServiceBusError,
    SessionCannotBeLockedError,
    SessionLockLostError,
)
from azure.servicebus.management import RuleProperties, SqlRuleFilter, TrueRuleFilter
from azure.servicebus.management import ServiceBusAdministrationClient as RealAdministrationClient

import client as client_mod

DEFAULT_START = datetime(2026, 9, 23, 10, 0, tzinfo=UTC)
PEEK_PAGE_MAX = 250  # records per peek call
RAD_DEFERRED_MAX = 250  # sequence numbers per RECEIVE_AND_DELETE deferred receive [live]
FIRST_PARTITION_ID = 51  # partition n of a partitioned entity carries id 51 + n in the top 16 bits
PARTITION_COUNT = 16

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_PEEK_LOCK = ServiceBusReceiveMode.PEEK_LOCK
_RAD = ServiceBusReceiveMode.RECEIVE_AND_DELETE
_SUFFIXES = {  # transfer first: its suffix also ends with "/$DeadLetterQueue"
    ServiceBusSubQueue.TRANSFER_DEAD_LETTER: "/$Transfer/$DeadLetterQueue",
    ServiceBusSubQueue.DEAD_LETTER: "/$DeadLetterQueue",
}
_SHUTDOWN = "The handler has already been shutdown. Please use ServiceBusClient to create a new instance."
_REQUIRES_SESSIONS = (
    "It is not possible for an entity that requires sessions to create a non-sessionful message receiver"
)
# [inferred] wording of the reverse mismatch; the component matches it through "not require sessions"
_NOT_SESSIONFUL = (
    "It is not possible for an entity that does not require sessions to create a sessionful message receiver"
)
_NOT_FOUND = "Failed to lock one or more specified messages. Either the message is not found or it is locked."
_MIXED_PARTITIONS = (
    "ReceiveBatch of sequence numbers from different partitions is not supported for an entity with "
    "partitioning enabled."
)
_RAD_LIMIT = "ReceiveAndDelete only can process 250 deferred messages"
_UNAUTHORIZED = "CBS token authentication failed for '{}': unauthorized."

# Public ServiceBusReceivedMessage attributes (besides sequence_number / body / locks) and their defaults.
_ATTRIBUTES: dict[str, Any] = {
    "message_id": None,
    "enqueued_sequence_number": None,
    "enqueued_time_utc": None,
    "content_type": None,
    "correlation_id": None,
    "subject": None,
    "session_id": None,
    "reply_to": None,
    "reply_to_session_id": None,
    "to": None,
    "partition_key": None,
    "application_properties": None,
    "delivery_count": 0,
    "dead_letter_reason": None,
    "dead_letter_error_description": None,
    "dead_letter_source": None,
    "time_to_live": None,
    "expires_at_utc": None,
    "scheduled_enqueue_time_utc": None,
    "state": ServiceBusMessageState.ACTIVE,
}
# AMQP header / properties fields, reachable only through ``raw_amqp_message`` (as on the real class).
_AMQP_FIELDS = frozenset(
    {
        "durable",
        "priority",
        "first_acquirer",
        "user_id",
        "content_encoding",
        "creation_time",
        "absolute_expiry_time",
        "group_sequence",
    }
)
# Message attributes ``FakeEntity.send`` takes through ``**extra`` (the rest are parameters or broker-owned).
_SEND_EXTRAS = frozenset(
    {
        "reply_to",
        "reply_to_session_id",
        "to",
        "partition_key",
        "enqueued_sequence_number",
        "dead_letter_reason",
        "dead_letter_error_description",
        "dead_letter_source",
    }
)
_BODY_KEYWORDS = {
    AmqpMessageBodyType.DATA: "data_body",
    AmqpMessageBodyType.SEQUENCE: "sequence_body",
    AmqpMessageBodyType.VALUE: "value_body",
}


def _ms(value: datetime) -> int:
    return (value - _EPOCH) // timedelta(milliseconds=1)


def _ms_precision(value: datetime) -> datetime:
    """AMQP timestamps carry milliseconds; the SDK's datetimes are truncated the same way."""
    return _EPOCH + timedelta(milliseconds=_ms(value))


def _utf8(value: str | None) -> bytes | None:
    return None if value is None else value.encode()


def _amqp(value: Any) -> Any:
    """A value as pyamqp decodes it: strings become bytes (recursively), timestamps epoch-ms ints."""
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, datetime):
        return _ms(value)
    if isinstance(value, dict):
        return {_amqp(key): _amqp(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_amqp(item) for item in value]
    return value


def _body_kind(body_type: str | AmqpMessageBodyType) -> AmqpMessageBodyType:
    return body_type if isinstance(body_type, AmqpMessageBodyType) else AmqpMessageBodyType[body_type.upper()]


def _encode_body(body: Any, kind: AmqpMessageBodyType) -> Any:
    """DATA: bytes / str -> one section, a list of them -> several; VALUE: bytes-encoded value;
    SEQUENCE: a list of lists (a flat list is one section)."""
    if kind is AmqpMessageBodyType.DATA:
        sections = body if isinstance(body, list) else [body]
        if not all(isinstance(section, bytes | str) for section in sections):
            raise TypeError("a DATA body is bytes, str or a list of them")
        return [section.encode() if isinstance(section, str) else section for section in sections]
    if kind is AmqpMessageBodyType.SEQUENCE:
        if not isinstance(body, list):
            raise TypeError("a SEQUENCE body is a list of lists")
        sections = body if all(isinstance(section, list) for section in body) else [body]
        return [_amqp(section) for section in sections]
    return _amqp(body)


def _check_timeout(timeout: float | None) -> None:
    if timeout is not None and timeout <= 0:
        raise ValueError("The timeout must be greater than 0.")


def _warn_unsupported(kwargs: dict[str, Any]) -> None:
    if kwargs:
        warnings.warn(f"Unsupported keyword args: {kwargs}", stacklevel=3)


class FakeClock:
    """The only time source of the fake broker; tests move it explicitly."""

    def __init__(self, start: datetime = DEFAULT_START) -> None:
        self._now = start

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("the fake clock never goes back")
        self._now += timedelta(seconds=seconds)


class FakeReceivedMessage:
    """Duck-typed ``ServiceBusReceivedMessage`` with the real attribute names and value types
    (research §5.1): bytes keys / bytes strings in ``application_properties``, aware UTC datetimes,
    ``state`` / ``body_type`` as the SDK enums, a real ``AmqpAnnotatedMessage`` as ``raw_amqp_message``."""

    message_id: str | None
    sequence_number: int
    enqueued_sequence_number: int | None
    enqueued_time_utc: datetime | None
    content_type: str | None
    correlation_id: str | None
    subject: str | None
    session_id: str | None
    reply_to: str | None
    reply_to_session_id: str | None
    to: str | None
    partition_key: str | None
    application_properties: dict[Any, Any] | None
    delivery_count: int
    dead_letter_reason: str | None
    dead_letter_error_description: str | None
    dead_letter_source: str | None
    time_to_live: timedelta | None
    expires_at_utc: datetime | None
    scheduled_enqueue_time_utc: datetime | None
    state: ServiceBusMessageState
    body_type: AmqpMessageBodyType
    raw_amqp_message: AmqpAnnotatedMessage

    def __init__(
        self,
        body: Any,
        *,
        body_type: AmqpMessageBodyType,
        sequence_number: int,
        attrs: dict[str, Any],
        amqp: dict[str, Any] | None = None,
        annotations: dict[Any, Any] | None = None,
        locked_until: datetime | None = None,
        lock_token: UUID | None = None,
        receiver: FakeReceiver | None = None,
        peeked: bool = False,
        deferred: bool = False,
        settled: bool = False,
        body_error: Callable[[], bool] | None = None,
    ) -> None:
        unknown = set(attrs) - set(_ATTRIBUTES)
        if unknown:
            raise TypeError(f"unknown message attribute(s): {sorted(unknown)}")
        values = {**_ATTRIBUTES, **attrs}
        if isinstance(values["state"], str):
            values["state"] = ServiceBusMessageState[values["state"]]
        if values["application_properties"] is not None:
            values["application_properties"] = _amqp(values["application_properties"])
        if "expires_at_utc" not in attrs and values["enqueued_time_utc"] and values["time_to_live"]:
            values["expires_at_utc"] = values["enqueued_time_utc"] + values["time_to_live"]
        vars(self).update(values)
        self.sequence_number = sequence_number
        self.body_type = body_type
        self._body = body
        self._body_error = body_error
        self._locked_until = locked_until
        self._lock_token = lock_token
        self._receiver = receiver
        self._peeked = peeked
        self._deferred = deferred
        self._settled = settled
        self.raw_amqp_message = self._raw(amqp or {}, annotations)

    @property
    def body(self) -> Any:
        """DATA / SEQUENCE: a fresh generator of sections; VALUE: the bytes-encoded value."""
        if self._body_error is not None and self._body_error():
            raise TypeError("injected decode failure")
        if self.body_type is AmqpMessageBodyType.VALUE:
            return self._body
        return (section for section in self._body)

    @property
    def locked_until_utc(self) -> datetime | None:
        """``None`` once settled (RECEIVE_AND_DELETE included) and on session receivers, like the SDK."""
        if self._settled or (self._receiver is not None and self._receiver.session is not None):
            return None
        return self._locked_until

    @property
    def lock_token(self) -> UUID | None:
        return None if self._settled else self._lock_token

    def _lapsed(self, now: datetime) -> bool:  # the SDK: locked_until_utc <= utc_now()
        return self._locked_until is not None and self._locked_until <= now

    def _raw(self, amqp: dict[str, Any], annotations: dict[Any, Any] | None) -> AmqpAnnotatedMessage:
        ttl_ms = None if self.time_to_live is None else self.time_to_live // timedelta(milliseconds=1)
        created, expiry = amqp.get("creation_time"), amqp.get("absolute_expiry_time")
        if ttl_ms and self.enqueued_time_utc is not None and created is None:
            created = _ms(self.enqueued_time_utc)
            expiry = created + ttl_ms if expiry is None else expiry
        optional = {
            b"x-opt-enqueued-time": self.enqueued_time_utc and _ms(self.enqueued_time_utc),
            b"x-opt-locked-until": self._locked_until and _ms(self._locked_until),
            b"x-opt-scheduled-enqueue-time": self.scheduled_enqueue_time_utc and _ms(self.scheduled_enqueue_time_utc),
            b"x-opt-partition-key": _utf8(self.partition_key),
            b"x-opt-enqueue-sequence-number": self.enqueued_sequence_number,
            b"x-opt-deadletter-source": _utf8(self.dead_letter_source),
        }
        notes: dict[Any, Any] = {
            b"x-opt-sequence-number": self.sequence_number,
            b"x-opt-message-state": int(self.state),
        }
        notes |= {key: value for key, value in optional.items() if value is not None}
        notes |= _amqp(annotations or {})
        kwargs: dict[str, Any] = {
            "header": {
                "delivery_count": self.delivery_count,
                "time_to_live": ttl_ms,
                "first_acquirer": amqp.get("first_acquirer"),
                "durable": amqp.get("durable"),
                "priority": amqp.get("priority"),
            },
            "properties": {  # pyamqp hands AMQP strings and symbols back as bytes
                "message_id": _utf8(self.message_id),
                "user_id": amqp.get("user_id"),
                "to": _utf8(self.to),
                "subject": _utf8(self.subject),
                "reply_to": _utf8(self.reply_to),
                "correlation_id": _utf8(self.correlation_id),
                "content_type": _utf8(self.content_type),
                "content_encoding": amqp.get("content_encoding"),
                "creation_time": created,
                "absolute_expiry_time": expiry,
                "group_id": _utf8(self.session_id),
                "group_sequence": amqp.get("group_sequence"),
                "reply_to_group_id": _utf8(self.reply_to_session_id),
            },
            "application_properties": self.application_properties,
            "annotations": notes,
            _BODY_KEYWORDS[self.body_type]: self._body,
        }
        return AmqpAnnotatedMessage(**kwargs)

    def __repr__(self) -> str:  # never the body
        return f"FakeReceivedMessage(sequence_number={self.sequence_number}, state={self.state.name})"


def make_message(
    body: bytes | str | dict | list = b"",
    *,
    body_type: str | AmqpMessageBodyType = "DATA",
    sequence_number: int = 1,
    **attrs: Any,
) -> FakeReceivedMessage:
    """A standalone received message for unit tests (``FakeEntity.send`` encoding rules). ``attrs``
    override any attribute; also accepted: the AMQP header / properties fields (``durable``,
    ``user_id``, ...), ``annotations``, ``locked_until_utc``, ``lock_token`` and ``body_error=True``."""
    kind = _body_kind(body_type)
    body_error = bool(attrs.pop("body_error", False))
    locked_until = attrs.pop("locked_until_utc", None)
    lock_token = attrs.pop("lock_token", None)
    annotations = attrs.pop("annotations", None)
    amqp = {key: attrs.pop(key) for key in list(attrs) if key in _AMQP_FIELDS}
    attrs.setdefault("enqueued_time_utc", DEFAULT_START)
    return FakeReceivedMessage(
        _encode_body(body, kind),
        body_type=kind,
        sequence_number=sequence_number,
        attrs=attrs,
        amqp=amqp,
        annotations=annotations,
        locked_until=locked_until,
        lock_token=lock_token,
        body_error=(lambda: True) if body_error else None,
    )


@dataclass
class _Record:
    """A message as the broker stores it."""

    seq: int
    body: Any
    body_type: AmqpMessageBodyType
    attrs: dict[str, Any]
    amqp: dict[str, Any]
    annotations: dict[Any, Any] | None
    enqueued_at: datetime
    scheduled_at: datetime | None
    expires_at: datetime | None
    delivery_count: int = 0
    deferred: bool = False
    locked_until: datetime | None = None
    lock_token: UUID | None = None
    holder: FakeReceiver | None = None

    @property
    def session_id(self) -> str | None:
        return self.attrs.get("session_id")

    def state(self, now: datetime) -> ServiceBusMessageState:
        if self.deferred:
            return ServiceBusMessageState.DEFERRED
        if self.scheduled_at is not None and self.scheduled_at > now:
            return ServiceBusMessageState.SCHEDULED
        return ServiceBusMessageState.ACTIVE


class FakeEntity:
    """A queue or subscription, or one of its sub-queues (``dead_letter`` / ``transfer_dead_letter``);
    ``path`` is the SDK entity path. The ``*_existing`` helpers set up broker state directly."""

    def __init__(
        self,
        broker: FakeBroker,
        path: str,
        *,
        kind: Literal["queue", "subscription"],
        requires_session: bool = False,
        partitioned: bool = False,
        lock_seconds: float = 60,
        max_delivery_count: int = 10,
        parent: FakeEntity | None = None,
    ) -> None:
        self.path = path
        self.kind = kind
        self.requires_session = requires_session
        self.partitioned = partitioned
        self.lock_seconds = lock_seconds
        self.max_delivery_count = max_delivery_count
        self._broker = broker
        self._parent = parent
        self._records: dict[int, _Record] = {}
        self._locked: set[int] = set()
        self._sessions: dict[str, FakeReceiver] = {}  # session id -> the receiver holding it
        self._counters: dict[int, int] = defaultdict(int)
        self._last_partition = -1
        self._rules: list[RuleProperties] = []
        self._sub_queues: dict[ServiceBusSubQueue, FakeEntity] = {}
        if parent is None:
            for sub_queue, suffix in _SUFFIXES.items():
                self._sub_queues[sub_queue] = FakeEntity(
                    broker,
                    path + suffix,
                    kind=kind,
                    partitioned=partitioned,
                    lock_seconds=lock_seconds,
                    max_delivery_count=max_delivery_count,
                    parent=self,
                )

    @property
    def dead_letter(self) -> FakeEntity:
        return self._sub_queue(ServiceBusSubQueue.DEAD_LETTER)

    @property
    def transfer_dead_letter(self) -> FakeEntity:
        return self._sub_queue(ServiceBusSubQueue.TRANSFER_DEAD_LETTER)

    @property
    def is_sub_queue(self) -> bool:
        return self._parent is not None

    def send(
        self,
        body: bytes | str | dict | list = b"",
        *,
        body_type: str | AmqpMessageBodyType = "DATA",
        session_id: str | None = None,
        message_id: str | None = None,
        enqueued_at: datetime | None = None,
        scheduled_at: datetime | None = None,
        ttl_seconds: float | None = None,
        partition: int = 0,
        content_type: str | None = None,
        application_properties: dict | None = None,
        subject: str | None = None,
        correlation_id: str | None = None,
        **extra: Any,
    ) -> int:
        """Store a message and return its sequence number. A scheduled message's enqueue time is its
        scheduled time. ``extra``: ``delivery_count``, ``time_to_live``, ``expires_at_utc``,
        ``annotations``, the AMQP header / properties fields and ``to``, ``reply_to``, ``dead_letter_*``..."""
        root = self._parent or self
        if not 0 <= partition < (PARTITION_COUNT if root.partitioned else 1):
            raise ValueError(f"partition {partition} does not exist on '{root.path}'")
        if self.kind == "queue" and self.requires_session and session_id is None:
            raise ValueError("a session-enabled queue only accepts messages with a session_id")
        amqp = {key: extra.pop(key) for key in list(extra) if key in _AMQP_FIELDS}
        annotations = extra.pop("annotations", None)
        delivery_count = extra.pop("delivery_count", 0)
        ttl = extra.pop("time_to_live", None if ttl_seconds is None else timedelta(seconds=ttl_seconds))
        now = self._broker.clock.now()
        enqueued = _ms_precision(scheduled_at if scheduled_at is not None else enqueued_at or now)
        expires = extra.pop("expires_at_utc") if "expires_at_utc" in extra else None if ttl is None else enqueued + ttl
        unknown = set(extra) - _SEND_EXTRAS
        if unknown:
            raise TypeError(f"send() got unsupported attribute(s): {sorted(unknown)}")
        attrs = {
            "message_id": message_id,
            "session_id": session_id,
            "content_type": content_type,
            "subject": subject,
            "correlation_id": correlation_id,
            "application_properties": None if application_properties is None else _amqp(application_properties),
            "time_to_live": ttl,
            **extra,
        }
        seq = root._next_sequence_number(partition)
        scheduled = None if scheduled_at is None else _ms_precision(scheduled_at)
        kind = _body_kind(body_type)
        record = _Record(seq, _encode_body(body, kind), kind, attrs, amqp, annotations, enqueued, scheduled, expires)
        record.delivery_count = delivery_count
        if self.kind == "subscription" and self.requires_session and session_id is None:
            self._dead_letter(record, "Session id is null.", None)  # [live]
        else:
            self._records[seq] = record
        return seq

    def defer_existing(self, seq: int) -> None:
        record = self._get(seq)
        record.deferred = True
        self._unlock(record, redelivered=False)

    def lock_existing(self, seq: int, seconds: float) -> None:
        """Lock as if an earlier (e.g. interrupted) receiver held the message."""
        record = self._get(seq)
        record.locked_until = _ms_precision(self._broker.clock.now() + timedelta(seconds=seconds))
        record.lock_token, record.holder = self._broker._token(), None
        self._locked.add(seq)

    def dead_letter_existing(self, seq: int, reason: str | None, description: str | None) -> None:
        self._dead_letter(self._get(seq), reason, description)

    def state_of(self, seq: int) -> str | None:
        self._sweep()
        record = self._records.get(seq)
        return None if record is None else record.state(self._broker.clock.now()).name

    def sequence_numbers(self) -> list[int]:
        self._sweep()
        return sorted(self._records)

    def delivery_count(self, seq: int) -> int:
        return self._get(seq).delivery_count

    def add_rule(self, name: str, sql_expression: str) -> None:
        """Add a SQL rule; the first one replaces the implicit ``$Default`` (``1=1``) rule."""
        if self.kind != "subscription" or self._parent is not None:
            raise ValueError("rules exist on subscriptions only")
        self._rules.append(RuleProperties(name, filter=SqlRuleFilter(sql_expression), action=None, created_at_utc=None))

    # --- broker internals -------------------------------------------------------------------------

    def _sub_queue(self, sub_queue: ServiceBusSubQueue) -> FakeEntity:
        if self._parent is not None:
            raise ValueError(f"'{self.path}' is a sub-queue and has no sub-queues")
        return self._sub_queues[sub_queue]

    def _next_sequence_number(self, partition: int) -> int:
        self._counters[partition] += 1
        count = self._counters[partition]
        return ((FIRST_PARTITION_ID + partition) << 48) | count if self.partitioned else count

    def _get(self, seq: int) -> _Record:
        self._sweep()
        if seq not in self._records:
            raise KeyError(f"sequence number {seq} is not in '{self.path}'")
        return self._records[seq]

    def _deadline(self) -> datetime:
        return _ms_precision(self._broker.clock.now() + timedelta(seconds=self.lock_seconds))

    def _expired(self, record: _Record, now: datetime) -> bool:
        # the TTL is not enforced in sub-queues
        return self._parent is None and record.expires_at is not None and record.expires_at <= now

    def _receivable(self, record: _Record, now: datetime, session_id: str | None) -> bool:
        return (
            not record.deferred
            and record.lock_token is None
            and (record.scheduled_at is None or record.scheduled_at <= now)
            and not self._expired(record, now)
            and (session_id is None or record.session_id == session_id)
        )

    def _lock_holds(self, record: _Record, now: datetime) -> bool:
        holder = record.holder
        if holder is not None and holder.session is not None:  # a session message: the session lock
            return self._sessions.get(record.session_id or "") is holder
        return record.locked_until is not None and record.locked_until > now

    def _sweep(self) -> None:
        """Expire lapsed session and message locks (the broker's timers, observed lazily). A sub-queue
        sweeps its parent first: a lapse there can dead-letter into it (the parent never sweeps a child)."""
        if self._parent is not None:
            self._parent._sweep()
        now = self._broker.clock.now()
        for session_id, holder in list(self._sessions.items()):
            if holder.closed or holder.session is None or holder.session._lapsed(now):
                del self._sessions[session_id]
        for seq in sorted(self._locked):
            record = self._records.get(seq)
            if record is None:
                self._locked.discard(seq)
            elif not self._lock_holds(record, now):
                self._unlock(record, redelivered=True)

    def _lock(self, record: _Record, receiver: FakeReceiver) -> None:
        record.locked_until, record.lock_token, record.holder = self._deadline(), self._broker._token(), receiver
        self._locked.add(record.seq)

    def _unlock(self, record: _Record, *, redelivered: bool) -> None:
        record.locked_until, record.lock_token, record.holder = None, None, None
        self._locked.discard(record.seq)
        if redelivered and not record.deferred:  # a deferred message counts its deliveries on receive
            record.delivery_count += 1
            if self._parent is None and record.delivery_count >= self.max_delivery_count:
                description = f"Message could not be consumed after {record.delivery_count} delivery attempts."
                self._dead_letter(record, "MaxDeliveryCountExceeded", description)

    def _dead_letter(self, record: _Record, reason: str | None, description: str | None) -> None:
        """Move to the DLQ keeping sequence number, enqueue time and delivery count [live]."""
        self._records.pop(record.seq, None)
        self._locked.discard(record.seq)
        properties = dict(record.attrs.get("application_properties") or {})
        if reason is not None:
            properties[b"DeadLetterReason"] = reason.encode()
        if description is not None:
            properties[b"DeadLetterErrorDescription"] = description.encode()
        attrs = record.attrs | {
            "dead_letter_reason": reason,
            "dead_letter_error_description": description,
            "application_properties": properties or None,
        }
        moved = replace(record, attrs=attrs, deferred=False, locked_until=None, lock_token=None, holder=None)
        self.dead_letter._records[record.seq] = moved

    def _snapshot(
        self, record: _Record, receiver: FakeReceiver, *, peeked: bool = False, deferred: bool = False
    ) -> FakeReceivedMessage:
        attrs = record.attrs | {
            "delivery_count": record.delivery_count,
            "enqueued_time_utc": record.enqueued_at,
            "expires_at_utc": record.expires_at,
            "scheduled_enqueue_time_utc": record.scheduled_at,
            "state": record.state(self._broker.clock.now()),
        }
        return FakeReceivedMessage(
            copy.deepcopy(record.body),
            body_type=record.body_type,
            sequence_number=record.seq,
            attrs=attrs,
            amqp=record.amqp,
            annotations=record.annotations,
            locked_until=None if peeked else record.locked_until,
            lock_token=None if peeked else record.lock_token,
            receiver=receiver,
            peeked=peeked,
            deferred=deferred,
            settled=not peeked and receiver.receive_mode is _RAD,
            body_error=partial(self._broker._consume_body_error, record.seq),
        )

    def _delivery_order(self, ready: list[_Record]) -> list[_Record]:
        """Sequence order; a partitioned entity serves its partitions round-robin [live]."""
        if not self.partitioned:
            return ready
        lanes: dict[int, deque[_Record]] = defaultdict(deque)
        for record in ready:
            lanes[record.seq >> 48].append(record)
        ids = sorted(lanes)
        start = next((index for index, pid in enumerate(ids) if pid > self._last_partition), 0)
        ids = ids[start:] + ids[:start]
        ordered: list[_Record] = []
        while len(ordered) < len(ready):
            ordered.extend(lanes[pid].popleft() for pid in ids if lanes[pid])
        return ordered

    def _receive(self, receiver: FakeReceiver, count: int) -> list[FakeReceivedMessage]:
        self._sweep()
        now, session_id = self._broker.clock.now(), receiver._session_filter()
        ready = [record for _, record in sorted(self._records.items()) if self._receivable(record, now, session_id)]
        picked = self._delivery_order(ready)[:count]
        if picked and self.partitioned:
            self._last_partition = picked[-1].seq >> 48
        messages = []
        for record in picked:
            if receiver.receive_mode is _RAD:
                del self._records[record.seq]
            else:
                self._lock(record, receiver)
            messages.append(self._snapshot(record, receiver))
        return messages

    def _peek(self, receiver: FakeReceiver, start: int, count: int) -> list[FakeReceivedMessage]:
        """Everything stored with ``seq >= start`` -- deferred, locked and expired-not-purged included;
        a subscription does not show scheduled messages (they wait at the topic [live])."""
        self._sweep()
        now, session_id = self._broker.clock.now(), receiver._session_filter()
        hide_scheduled = self.kind == "subscription" and self._parent is None
        messages: list[FakeReceivedMessage] = []
        for seq, record in sorted(self._records.items()):
            if len(messages) >= count:
                break
            if seq < start or (session_id is not None and record.session_id != session_id):
                continue
            if hide_scheduled and record.state(now) is ServiceBusMessageState.SCHEDULED:
                continue
            messages.append(self._snapshot(record, receiver, peeked=True))
        return messages

    def _receive_deferred(self, receiver: FakeReceiver, seqs: list[int]) -> list[FakeReceivedMessage]:
        self._sweep()
        now, session_id = self._broker.clock.now(), receiver._session_filter()
        if self.partitioned and len({seq >> 48 for seq in seqs}) > 1:
            raise ServiceBusError(_MIXED_PARTITIONS)
        if receiver.receive_mode is _RAD and len(seqs) > RAD_DEFERRED_MAX:
            raise ServiceBusError(_RAD_LIMIT)  # nothing is deleted [live]
        records = []
        for seq in sorted(set(seqs)):
            record = self._records.get(seq)
            if record is not None and record.deferred and self._expired(record, now):
                del self._records[seq]  # a deferred message's TTL is checked only now [docs + live]
                record = None
            if (
                record is None
                or not record.deferred
                or record.lock_token is not None
                or (session_id is not None and record.session_id != session_id)
            ):
                raise MessageNotFoundError(message=_NOT_FOUND)  # the whole call fails [live]
            records.append(record)
        messages = []
        for record in records:
            if receiver.receive_mode is _RAD:
                del self._records[record.seq]
                messages.append(self._snapshot(record, receiver, deferred=True))
            else:  # locked and counted as a delivery; the state stays DEFERRED [live]
                self._lock(record, receiver)
                messages.append(self._snapshot(record, receiver, deferred=True))
                record.delivery_count += 1
        return messages

    def _settle(
        self,
        receiver: FakeReceiver,
        message: FakeReceivedMessage,
        operation: str,
        reason: str | None,
        detail: str | None,
    ) -> None:
        self._sweep()
        record = self._records.get(message.sequence_number)
        if record is None or record.lock_token is None or record.lock_token != message._lock_token:
            return  # the broker rejects a stale lock; 7.14.3 settles pre-settled and never hears of it
        if not message._deferred and record.holder is not receiver:
            return  # a link-received message settles only on the link that received it
        if operation == "complete_message":
            del self._records[record.seq]
            self._locked.discard(record.seq)
        elif operation == "abandon_message":
            self._unlock(record, redelivered=True)
        elif operation == "defer_message":
            record.deferred = True
            self._unlock(record, redelivered=False)
        elif self._parent is None:  # dead-lettering a sub-queue message is rejected (silently on 7.14.3)
            self._dead_letter(record, reason, detail)

    def _renew(self, message: FakeReceivedMessage) -> datetime:
        self._sweep()
        record = self._records.get(message.sequence_number)
        if record is None or record.lock_token is None or record.lock_token != message._lock_token:
            raise MessageLockLostError(
                message="The lock supplied is invalid. Either the lock expired, or the message has already been "
                "removed from the queue."
            )
        record.locked_until = self._deadline()
        return record.locked_until

    def _accept_session(self, receiver: FakeReceiver, session: FakeSession) -> None:
        self._sweep()
        now = self._broker.clock.now()
        if session.session_id is NEXT_AVAILABLE_SESSION:  # deferred-only sessions are never handed out [live]
            ready = {
                record.session_id
                for record in self._records.values()
                if record.session_id is not None
                and record.session_id not in self._sessions
                and self._receivable(record, now, record.session_id)
            }
            if not ready:
                raise OperationTimeoutError()  # raised at once instead of after max_wait_time
            session_id = min(ready)
        else:
            session_id = str(session.session_id)
            if session_id in self._sessions:
                raise SessionCannotBeLockedError(
                    message=f"The requested session '{session_id}' cannot be accepted. It may be locked by another "
                    "receiver."
                )
        self._sessions[session_id] = receiver
        session._session_id, session._locked_until = session_id, self._deadline()

    def _renew_session(self, receiver: FakeReceiver, session: FakeSession) -> datetime:
        self._sweep()
        if self._sessions.get(str(session.session_id)) is not receiver:
            raise SessionLockLostError()
        until = self._deadline()
        session._locked_until = until
        return until

    def _release(self, receiver: FakeReceiver) -> None:
        for session_id, holder in list(self._sessions.items()):
            if holder is receiver:
                del self._sessions[session_id]
        self._sweep()

    def _runtime_properties(self, name: str) -> FakeRuntimeProperties:
        self._sweep()
        now = self._broker.clock.now()
        scheduled = sum(1 for r in self._records.values() if r.state(now) is ServiceBusMessageState.SCHEDULED)
        return FakeRuntimeProperties(
            name=name,
            active_message_count=len(self._records) - scheduled,
            dead_letter_message_count=len(self.dead_letter.sequence_numbers()),
            scheduled_message_count=scheduled,
            transfer_dead_letter_message_count=len(self.transfer_dead_letter.sequence_numbers()),
        )


class FakeSession:
    """``receiver.session``: ``session_id`` is the requested value (``NEXT_AVAILABLE_SESSION`` too)
    until the receiver opens, then the accepted id; ``locked_until_utc`` is set on open."""

    def __init__(self, receiver: FakeReceiver, session_id: str | ServiceBusSessionFilter) -> None:
        self._receiver = receiver
        self._session_id = session_id
        self._locked_until: datetime | None = None

    @property
    def session_id(self) -> str | ServiceBusSessionFilter:
        return self._session_id

    @property
    def locked_until_utc(self) -> datetime | None:
        return self._locked_until

    def _lapsed(self, now: datetime) -> bool:  # the SDK: locked_until_utc <= utc_now()
        return self._locked_until is not None and self._locked_until <= now

    def renew_lock(self, *, timeout: float | None = None, **kwargs: Any) -> datetime:
        receiver = self._receiver
        receiver._record("session_renew_lock", timeout=timeout)
        _warn_unsupported(kwargs)
        receiver._check_live()
        _check_timeout(timeout)
        return receiver._open()._renew_session(receiver, self)


class FakeReceiver:
    """Stand-in for ``ServiceBusReceiver``. ``kwargs`` holds exactly what ``get_*_receiver`` got;
    ``operations`` records every data-plane call with its arguments."""

    def __init__(
        self, client: FakeServiceBusClient, main_path: str, kwargs: dict[str, Any], *, subscription: bool
    ) -> None:
        sub_queue = ServiceBusSubQueue(kwargs["sub_queue"]) if kwargs.get("sub_queue") else None
        requested = kwargs.get("session_id")
        self.client = client
        self.kwargs = dict(kwargs)
        self.receive_mode = ServiceBusReceiveMode(kwargs.get("receive_mode", _PEEK_LOCK))
        self.sub_queue = sub_queue
        self.prefetch_count: int = kwargs.get("prefetch_count", 0)
        self.max_wait_time: float | None = kwargs.get("max_wait_time")
        self.main_path = main_path
        self.entity_path = main_path + (_SUFFIXES[sub_queue] if sub_queue else "")
        self.closed = False
        self.operations: list[tuple[str, dict[str, Any]]] = []
        self._subscription = subscription
        self._session = None if requested is None else FakeSession(self, requested)
        self._broker = client._broker
        self._entity: FakeEntity | None = None
        self._cursor = 0  # last received or peeked sequence number: the SDK's peek cursor
        self._client_identifier: str = kwargs.get("client_identifier") or f"receiver-{len(self._broker.receivers)}"
        self._broker.receivers.append(self)
        client._receivers.append(self)

    @property
    def session(self) -> FakeSession | None:
        return self._session

    @property
    def client_identifier(self) -> str:
        return self._client_identifier

    def __enter__(self) -> Self:
        if self.closed:
            raise ValueError(_SHUTDOWN)
        self._open()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            if self._entity is not None:
                self._entity._release(self)

    def receive_messages(
        self, max_message_count: int | None = 1, max_wait_time: float | None = None
    ) -> list[FakeReceivedMessage]:
        """Never waits: an empty entity returns ``[]`` at once."""
        self._record("receive_messages", max_message_count=max_message_count, max_wait_time=max_wait_time)
        self._check_live()
        if max_wait_time is not None and max_wait_time <= 0:
            raise ValueError("The max_wait_time must be greater than 0.")
        if max_message_count is not None and max_message_count <= 0:
            raise ValueError("The max_message_count must be greater than 0")
        entity = self._open()
        self._broker._raise_injected("receive")
        messages = entity._receive(self, max_message_count or self.prefetch_count)
        self._cursor = messages[-1].sequence_number if messages else self._cursor
        return messages

    def peek_messages(
        self, max_message_count: int = 1, *, sequence_number: int = 0, timeout: float | None = None, **kwargs: Any
    ) -> list[FakeReceivedMessage]:
        self._record("peek_messages", max_message_count=max_message_count, sequence_number=sequence_number)
        _warn_unsupported(kwargs)
        self._check_live()
        _check_timeout(timeout)
        start = sequence_number or self._cursor + 1  # 0: the SDK's cursor mode (last received / peeked + 1)
        if int(max_message_count) < 0:
            raise ValueError("max_message_count must be 1 or greater.")
        entity = self._open()
        self._broker._raise_injected("peek")
        messages = entity._peek(self, start, min(max_message_count, PEEK_PAGE_MAX))
        self._cursor = messages[-1].sequence_number if messages else self._cursor
        return messages

    def receive_deferred_messages(
        self, sequence_numbers: int | list[int], *, timeout: float | None = None, **kwargs: Any
    ) -> list[FakeReceivedMessage]:
        seqs = [sequence_numbers] if isinstance(sequence_numbers, int) else list(sequence_numbers)
        self._record("receive_deferred_messages", sequence_numbers=seqs, timeout=timeout)
        _warn_unsupported(kwargs)
        self._check_live()
        _check_timeout(timeout)
        if not seqs:
            return []
        entity = self._open()
        if self.receive_mode is _RAD and self._broker._commit_errors:
            raise self._broker._commit_errors.popleft()
        return entity._receive_deferred(self, seqs)

    def complete_message(self, message: FakeReceivedMessage) -> None:
        self._settle("complete_message", "complete", message)

    def abandon_message(self, message: FakeReceivedMessage) -> None:
        self._settle("abandon_message", "abandon", message)

    def defer_message(self, message: FakeReceivedMessage) -> None:
        self._settle("defer_message", "defer", message)

    def dead_letter_message(
        self, message: FakeReceivedMessage, reason: str | None = None, error_description: str | None = None
    ) -> None:
        self._settle("dead_letter_message", "dead-letter", message, reason, error_description)

    def renew_message_lock(
        self, message: FakeReceivedMessage, *, timeout: float | None = None, **kwargs: Any
    ) -> datetime:
        self._record("renew_message_lock", sequence_number=message.sequence_number)
        _warn_unsupported(kwargs)
        if self._session is not None:
            raise TypeError(
                "Renewing message lock is an invalid operation when working with sessions."
                "Please renew the session lock instead."
            )
        self._check_live()
        self._check_message_alive(message, "renew")
        if not message.lock_token:
            raise ValueError("Unable to renew lock - no lock token found.")
        _check_timeout(timeout)
        until = self._open()._renew(message)
        message._locked_until = until
        return until

    # --- SDK client-side behaviour ----------------------------------------------------------------

    def _record(self, operation: str, **details: Any) -> None:
        self._broker.calls.append((operation, self.entity_path))
        self.operations.append((operation, details))

    def _check_live(self) -> None:
        if self.closed:
            raise ValueError(_SHUTDOWN)
        if self._broker.auth_failure:  # also on an open link: set mid-run, the next operation fails
            raise ServiceBusAuthenticationError(message=_UNAUTHORIZED.format(self.entity_path))
        if self._session is not None and self._session._lapsed(self._broker.clock.now()):
            raise SessionLockLostError()

    def _open(self) -> FakeEntity:
        """Attach the link: resolve the entity, authenticate and accept the session (once)."""
        if self._entity is not None:
            return self._entity
        entity = self._broker._resolve(self)
        if entity.requires_session and self._session is None:
            raise ServiceBusError(_REQUIRES_SESSIONS)
        if self._session is not None:
            if not entity.requires_session:
                raise ServiceBusError(_NOT_SESSIONFUL)
            entity._accept_session(self, self._session)
        self._entity = entity
        return entity

    def _session_filter(self) -> str | None:
        return None if self._session is None else str(self._session.session_id)

    def _check_message_alive(self, message: FakeReceivedMessage, action: str) -> None:
        if message._peeked:
            raise ValueError(
                f"The operation {action} is not supported for peeked messages."
                "Only messages received using receive methods in PEEK_LOCK mode can be settled."
            )
        if self.receive_mode is _RAD:
            raise ValueError(f"The operation {action} is not supported in 'RECEIVE_AND_DELETE' receive mode.")
        if message._settled:
            raise MessageAlreadySettled(action=action)
        if self._entity is None:
            raise ValueError(f"Failed to {action} the message as the handler has already been shutdown.")

    def _settle(
        self,
        operation: str,
        action: str,
        message: FakeReceivedMessage,
        reason: str | None = None,
        detail: str | None = None,
    ) -> None:
        self._record(operation, sequence_number=getattr(message, "sequence_number", None))
        self._check_live()
        if not isinstance(message, FakeReceivedMessage):
            raise TypeError("Parameter 'message' must be of type ServiceBusReceivedMessage")
        self._check_message_alive(message, action)
        if self._session is None and message._lapsed(self._broker.clock.now()):
            raise MessageLockLostError(message="The lock on the message lock has expired.")
        self._open()._settle(self, message, operation, reason, detail)
        message._settled = True


def _installed(owner: type[FakeServiceBusClient | FakeAdminClient | FakeCredential]) -> FakeBroker:
    broker = owner.broker
    if broker is None:
        raise RuntimeError(f"{owner.__name__} is not installed: use the `broker` fixture or install()")
    return broker


class FakeServiceBusClient:
    """Stand-in for ``ServiceBusClient``, recorded in ``broker.clients``. A real client is built from
    the same arguments (offline) and validates the connection string and every receiver's kwargs."""

    broker: ClassVar[FakeBroker | None] = None
    kwargs: dict[str, Any]
    credential: Any
    fully_qualified_namespace: str
    entity_name: str | None
    auth_kind: Literal["sas", "credential"]
    closed: bool

    def __init__(self, fully_qualified_namespace: str, credential: Any, **kwargs: Any) -> None:
        self._bind(RealServiceBusClient(fully_qualified_namespace, credential, **kwargs), credential, kwargs)

    @classmethod
    def from_connection_string(cls, conn_str: str, **kwargs: Any) -> FakeServiceBusClient:
        real = RealServiceBusClient.from_connection_string(conn_str=conn_str, **kwargs)  # ValueError on garbage
        client = cls.__new__(cls)
        client._bind(real, None, kwargs)
        return client

    def _bind(self, real: RealServiceBusClient, credential: Any, kwargs: dict[str, Any]) -> None:
        self._broker = _installed(FakeServiceBusClient)
        self._real = real
        self._receivers: list[FakeReceiver] = []
        self.kwargs = dict(kwargs)
        self.credential = credential
        self.fully_qualified_namespace = real.fully_qualified_namespace
        self.entity_name = real._entity_name  # the connection string's EntityPath
        self.auth_kind = "sas" if credential is None else "credential"
        self.closed = False
        self._broker.clients.append(self)

    def get_queue_receiver(self, queue_name: str, **kwargs: Any) -> FakeReceiver:
        self._real.get_queue_receiver(queue_name, **kwargs).close()  # the SDK's own argument checks
        return FakeReceiver(self, queue_name, kwargs, subscription=False)

    def get_subscription_receiver(self, topic_name: str, subscription_name: str, **kwargs: Any) -> FakeReceiver:
        self._real.get_subscription_receiver(topic_name, subscription_name, **kwargs).close()
        return FakeReceiver(self, f"{topic_name}/Subscriptions/{subscription_name}", kwargs, subscription=True)

    def close(self) -> None:
        """Closes every receiver it created, like the SDK."""
        for receiver in self._receivers:
            receiver.close()
        self.closed = True
        self._real.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


@dataclass(frozen=True)
class FakeQueueProperties:
    name: str
    requires_session: bool
    enable_partitioning: bool
    lock_duration: timedelta
    max_delivery_count: int


@dataclass(frozen=True)
class FakeTopicProperties:
    name: str
    enable_partitioning: bool


@dataclass(frozen=True)
class FakeSubscriptionProperties:  # no enable_partitioning, like SubscriptionProperties
    name: str
    requires_session: bool
    lock_duration: timedelta
    max_delivery_count: int


@dataclass(frozen=True)
class FakeRuntimeProperties:
    name: str
    active_message_count: int
    dead_letter_message_count: int
    scheduled_message_count: int
    transfer_dead_letter_message_count: int


@dataclass
class _Topic:
    name: str
    partitioned: bool
    subscriptions: dict[str, FakeEntity] = field(default_factory=dict)


class FakeAdminClient:
    """Stand-in for ``ServiceBusAdministrationClient``; calls go to ``broker.admin_calls``. ``list_*``
    are lazy like ``ItemPaged``: the request, and any error, happens on iteration."""

    broker: ClassVar[FakeBroker | None] = None
    kwargs: dict[str, Any]
    credential: Any
    fully_qualified_namespace: str
    closed: bool

    def __init__(self, fully_qualified_namespace: str, credential: Any, **kwargs: Any) -> None:
        RealAdministrationClient(fully_qualified_namespace, credential, **kwargs).close()
        self._bind(fully_qualified_namespace, credential, kwargs)

    @classmethod
    def from_connection_string(cls, conn_str: str, **kwargs: Any) -> FakeAdminClient:
        real = RealAdministrationClient.from_connection_string(conn_str, **kwargs)  # ValueError on garbage
        admin = cls.__new__(cls)
        admin._bind(real.fully_qualified_namespace, None, kwargs)
        real.close()
        return admin

    def _bind(self, fully_qualified_namespace: str, credential: Any, kwargs: dict[str, Any]) -> None:
        self._broker = _installed(FakeAdminClient)
        self.kwargs = dict(kwargs)
        self.credential = credential
        self.fully_qualified_namespace = fully_qualified_namespace
        self.closed = False
        self._broker.admin_clients.append(self)

    def _call(self, operation: str, path: str) -> None:
        self._broker.admin_calls.append((operation, path))
        if self._broker.management_denied:
            raise ClientAuthenticationError(message="Unauthorized access. 'Manage,EntityRead' claims required.")
        if self._broker.management_error is not None:
            raise self._broker.management_error

    def _paged[T](self, operation: str, path: str, produce: Callable[[], list[T]]) -> Iterator[T]:
        self._call(operation, path)
        yield from produce()

    def list_queues(self, **kwargs: Any) -> Iterator[FakeQueueProperties]:
        return self._paged("list_queues", "", lambda: [_queue_properties(q) for q in self._broker._queues()])

    def list_topics(self, **kwargs: Any) -> Iterator[FakeTopicProperties]:
        topics = self._broker._topics
        return self._paged(
            "list_topics", "", lambda: [FakeTopicProperties(t.name, t.partitioned) for _, t in sorted(topics.items())]
        )

    def list_subscriptions(self, topic_name: str, **kwargs: Any) -> Iterator[FakeSubscriptionProperties]:
        def produce() -> list[FakeSubscriptionProperties]:
            topic = self._broker._find_topic(topic_name)
            return [_subscription_properties(name, entity) for name, entity in sorted(topic.subscriptions.items())]

        return self._paged("list_subscriptions", topic_name, produce)

    def list_rules(self, topic_name: str, subscription_name: str, **kwargs: Any) -> Iterator[RuleProperties]:
        def produce() -> list[RuleProperties]:
            entity = self._broker._find_subscription(topic_name, subscription_name)
            return entity._rules or [
                RuleProperties("$Default", filter=TrueRuleFilter(), action=None, created_at_utc=None)
            ]

        return self._paged("list_rules", f"{topic_name}/Subscriptions/{subscription_name}", produce)

    def get_queue(self, queue_name: str, **kwargs: Any) -> FakeQueueProperties:
        self._call("get_queue", queue_name)
        return _queue_properties(self._broker._find_queue(queue_name))

    def get_topic(self, topic_name: str, **kwargs: Any) -> FakeTopicProperties:
        self._call("get_topic", topic_name)
        topic = self._broker._find_topic(topic_name)
        return FakeTopicProperties(topic.name, topic.partitioned)

    def get_subscription(self, topic_name: str, subscription_name: str, **kwargs: Any) -> FakeSubscriptionProperties:
        self._call("get_subscription", f"{topic_name}/Subscriptions/{subscription_name}")
        return _subscription_properties(
            subscription_name, self._broker._find_subscription(topic_name, subscription_name)
        )

    def get_queue_runtime_properties(self, queue_name: str, **kwargs: Any) -> FakeRuntimeProperties:
        self._call("get_queue_runtime_properties", queue_name)
        return self._broker._find_queue(queue_name)._runtime_properties(queue_name)

    def get_subscription_runtime_properties(
        self, topic_name: str, subscription_name: str, **kwargs: Any
    ) -> FakeRuntimeProperties:
        self._call("get_subscription_runtime_properties", f"{topic_name}/Subscriptions/{subscription_name}")
        return self._broker._find_subscription(topic_name, subscription_name)._runtime_properties(subscription_name)

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def _queue_properties(entity: FakeEntity) -> FakeQueueProperties:
    return FakeQueueProperties(
        name=entity.path,
        requires_session=entity.requires_session,
        enable_partitioning=entity.partitioned,
        lock_duration=timedelta(seconds=entity.lock_seconds),
        max_delivery_count=entity.max_delivery_count,
    )


def _subscription_properties(name: str, entity: FakeEntity) -> FakeSubscriptionProperties:
    return FakeSubscriptionProperties(
        name=name,
        requires_session=entity.requires_session,
        lock_duration=timedelta(seconds=entity.lock_seconds),
        max_delivery_count=entity.max_delivery_count,
    )


class FakeCredential:
    """Stand-in for ``ClientSecretCredential`` (real argument checks, offline); never fetches a token."""

    broker: ClassVar[FakeBroker | None] = None

    def __init__(self, tenant_id: str, client_id: str, client_secret: str, **kwargs: Any) -> None:
        RealClientSecretCredential(tenant_id, client_id, client_secret, **kwargs).close()
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.kwargs = dict(kwargs)
        self._client_secret = client_secret
        _installed(FakeCredential).credentials.append(self)

    def get_token(self, *scopes: str, **kwargs: Any) -> Any:
        raise AssertionError("FakeCredential never fetches a token: the tests are offline")

    def close(self) -> None:
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


class FakeBroker:
    """One fake Service Bus namespace: entities, fault injection and a record of every client call."""

    def __init__(self, clock: FakeClock | None = None) -> None:
        self.clock = clock or FakeClock()
        self.auth_failure = False  # data-plane receivers raise ServiceBusAuthenticationError (open or not)
        self.management_denied = False  # admin calls raise ClientAuthenticationError
        self.management_error: Exception | None = None  # admin calls raise this
        self.calls: list[tuple[str, str]] = []  # (data-plane operation, entity path)
        self.admin_calls: list[tuple[str, str]] = []
        self.clients: list[FakeServiceBusClient] = []
        self.admin_clients: list[FakeAdminClient] = []
        self.receivers: list[FakeReceiver] = []
        self.credentials: list[FakeCredential] = []
        self._entities: dict[str, FakeEntity] = {}  # queues and subscriptions by main path
        self._topics: dict[str, _Topic] = {}
        self._injections: dict[str, dict[int, Exception]] = {"receive": {}, "peek": {}}
        self._call_numbers: dict[str, int] = {"receive": 0, "peek": 0}
        self._commit_errors: deque[Exception] = deque()
        self._body_errors: dict[int, int] = defaultdict(int)
        self._tokens = itertools.count(1)

    def add_queue(
        self,
        name: str,
        *,
        sessions: bool = False,
        partitioned: bool = False,
        lock_seconds: float = 60,
        max_delivery_count: int = 10,
    ) -> FakeEntity:
        if name in self._entities or name in self._topics:
            raise ValueError(f"'{name}' already exists")
        entity = FakeEntity(
            self,
            name,
            kind="queue",
            requires_session=sessions,
            partitioned=partitioned,
            lock_seconds=lock_seconds,
            max_delivery_count=max_delivery_count,
        )
        self._entities[name] = entity
        return entity

    def add_topic(self, name: str, *, partitioned: bool = False) -> None:
        """Only needed for a topic without subscriptions; ``add_subscription`` creates its topic."""
        if name in self._entities or name in self._topics:
            raise ValueError(f"'{name}' already exists")
        self._topics[name] = _Topic(name, partitioned)

    def add_subscription(
        self,
        topic: str,
        name: str,
        *,
        sessions: bool = False,
        partitioned: bool = False,
        lock_seconds: float = 60,
        max_delivery_count: int = 10,
    ) -> FakeEntity:
        """``partitioned`` is the topic's setting, so every subscription of a topic must agree."""
        if topic not in self._topics:
            self.add_topic(topic, partitioned=partitioned)
        owner = self._topics[topic]
        if owner.partitioned != partitioned or name in owner.subscriptions:
            raise ValueError(f"subscription '{name}' conflicts with topic '{topic}'")
        path = f"{topic}/Subscriptions/{name}"
        entity = FakeEntity(
            self,
            path,
            kind="subscription",
            requires_session=sessions,
            partitioned=partitioned,
            lock_seconds=lock_seconds,
            max_delivery_count=max_delivery_count,
        )
        self._entities[path] = owner.subscriptions[name] = entity
        return entity

    def entity(self, path: str) -> FakeEntity:
        """``"q"``, ``"q/$DeadLetterQueue"``, ``"q/$Transfer/$DeadLetterQueue"``, ``"t/Subscriptions/s"``..."""
        for sub_queue, suffix in _SUFFIXES.items():
            if path.endswith(suffix):
                return self.entity(path.removesuffix(suffix))._sub_queue(sub_queue)
        if path not in self._entities:
            raise KeyError(f"no entity '{path}' in the fake broker")
        return self._entities[path]

    def inject_receive_error(self, error: Exception, *, on_call: int) -> None:
        """The ``on_call``-th ``receive_messages`` that reaches the broker (1-based, across all receivers)
        raises ``error`` once."""
        self._injections["receive"][on_call] = error

    def inject_peek_error(self, error: Exception, *, on_call: int) -> None:
        """The ``on_call``-th ``peek_messages`` that reaches the broker (1-based, across all receivers)
        raises ``error`` once."""
        self._injections["peek"][on_call] = error

    def inject_body_error(self, sequence_number: int, *, times: int) -> None:
        """The next ``times`` reads of ``.body`` of that sequence number raise ``TypeError``."""
        self._body_errors[sequence_number] += times

    def inject_commit_errors(self, errors: list[Exception]) -> None:
        """Successive RECEIVE_AND_DELETE ``receive_deferred_messages`` calls raise these first."""
        self._commit_errors.extend(errors)

    def _raise_injected(self, kind: Literal["receive", "peek"]) -> None:
        """Count a call that reached the broker (client-side rejections and failed opens do not count,
        so an injection is never lost) and raise the injection registered for its number."""
        self._call_numbers[kind] += 1
        injected = self._injections[kind].pop(self._call_numbers[kind], None)
        if injected is not None:
            raise injected

    def _consume_body_error(self, sequence_number: int) -> bool:
        if self._body_errors[sequence_number] <= 0:
            return False
        self._body_errors[sequence_number] -= 1
        return True

    def _token(self) -> UUID:
        return UUID(int=next(self._tokens))

    def _resolve(self, receiver: FakeReceiver) -> FakeEntity:
        if self.auth_failure:
            raise ServiceBusAuthenticationError(message=_UNAUTHORIZED.format(receiver.entity_path))
        entity = self._entities.get(receiver.main_path)
        if entity is None or (entity.kind == "subscription") != receiver._subscription:
            if receiver.client.auth_kind == "sas" and not receiver._subscription:
                # a missing queue via SAS reads as unauthorized [live]
                raise ServiceBusAuthenticationError(message=_UNAUTHORIZED.format(receiver.entity_path))
            raise MessagingEntityNotFoundError(
                message=f"The messaging entity '{receiver.entity_path}' could not be found."
            )
        return entity._sub_queue(receiver.sub_queue) if receiver.sub_queue else entity

    def _queues(self) -> list[FakeEntity]:
        return [entity for path, entity in sorted(self._entities.items()) if entity.kind == "queue"]

    def _find_queue(self, name: str) -> FakeEntity:
        entity = self._entities.get(name)
        if entity is None or entity.kind != "queue":
            raise ResourceNotFoundError(message=f"Queue '{name}' does not exist.")
        return entity

    def _find_topic(self, name: str) -> _Topic:
        if name not in self._topics:
            raise ResourceNotFoundError(message=f"Topic '{name}' does not exist.")
        return self._topics[name]

    def _find_subscription(self, topic: str, name: str) -> FakeEntity:
        subscriptions = self._find_topic(topic).subscriptions
        if name not in subscriptions:
            raise ResourceNotFoundError(message=f"Subscription '{topic}/Subscriptions/{name}' does not exist.")
        return subscriptions[name]


def install(monkeypatch: pytest.MonkeyPatch, broker: FakeBroker) -> None:
    """Point ``client.ServiceBusClient`` / ``ServiceBusAdministrationClient`` / ``ClientSecretCredential``
    at the fakes bound to ``broker`` (undone by ``monkeypatch`` after the test)."""
    for fake in (FakeServiceBusClient, FakeAdminClient, FakeCredential):
        monkeypatch.setattr(fake, "broker", broker)
    monkeypatch.setattr(client_mod, "ServiceBusClient", FakeServiceBusClient)
    monkeypatch.setattr(client_mod, "ServiceBusAdministrationClient", FakeAdminClient)
    monkeypatch.setattr(client_mod, "ClientSecretCredential", FakeCredential)
