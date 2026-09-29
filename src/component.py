"""Azure Service Bus extractor (spec §6.1, §5.4, §7).

One config row extracts one Service Bus entity -- a queue, a subscription or one of their
dead-letter sub-queues -- into one Storage table. ``run()`` is a thin orchestrator over the modules
that own the logic: ``commit.py`` deletes the previous defer-commit run's pending set and reconciles
deferrals, ``receiver.py`` drives the destructive receive loop and ``peek.py`` the peek pager, ``settlement.py``
writes each batch before settling it, ``output.py`` streams the CSV and its manifest, ``state.py``
holds the row state. The sync actions (spec §5.4) read the management plane or peek the entity; they
never settle a message, and each gives up before the platform's 30-second limit.
"""

import logging
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property, wraps
from typing import Any, Literal

from azure.servicebus import NEXT_AVAILABLE_SESSION, ServiceBusReceiveMode
from azure.servicebus.exceptions import OperationTimeoutError, ServiceBusError
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import MessageType, SelectElement, ValidationResult

from body import FlattenRegistry
from client import ServiceBusConnector, configure_logging, redact_secrets, throttled_requests, to_user_exception
from columns import format_timestamp, metadata_column_names, render_preview, utc_now
from commit import ForeignDeferralProbe, OrphanScanner, PendingCommitter
from configuration import (
    AuthConfiguration,
    BodyFormat,
    Configuration,
    EntityConfiguration,
    FetchMode,
    SettlementMode,
    SyncActionConfiguration,
)
from entity import (
    EntityInfo,
    EntityRef,
    list_entity_names,
    load_entity_info,
    log_entity_counts,
    probe_management,
)
from output import OutputTable
from peek import PeekPager
from receiver import SESSION_ACCEPT_WAIT_SECONDS, ReceiveLoop
from settlement import BatchProcessor, UnreadableHandler, make_settler
from state import ExtractorState, PeekCursor, PendingSetBuilder
from stats import RunStats, log_effective_settings

logger = logging.getLogger(__name__)

LEGACY_QUEUE_WARNING = (
    "This project runs on the legacy job queue, which does not support write_always: if this job fails after "
    "messages were deleted, the rows written so far are not uploaded and those messages are lost. Use "
    "defer_commit for failure-safe consumption on this project."
)
PREVIEW_MESSAGE_COUNT = 10

# The platform stops a sync action after 30 seconds (image pull excluded) and then answers a generic
# HTTP 500 "Internal Server Error" instead of the action's message [docs: help.keboola.com/extend/
# common-interface/actions]. The SDK's own retries (three, exponential backoff, 60-second operation
# and auth timeouts) can outlast that on a busy or throttled namespace [live: a concurrent drain of
# the same Standard-tier namespace slowed one testConnection from ~1.5 s to 8-9 s], so every action
# gives up first, leaving the container start-up and the reply their share of the 30 seconds.
SYNC_ACTION_DEADLINE_SECONDS = 20.0
SYNC_ACTION_TIMEOUT_MESSAGE = (
    "Azure Service Bus did not respond within {seconds:g} seconds, so the action was stopped before the "
    "platform's 30-second limit for UI actions. The namespace may be busy or throttled (for example while "
    "a large extraction runs on it) or unreachable. Try again in a minute."
)

# The modes that arm write_always (spec §6.9): the legacy job queue drops that safety net for them.
_WRITE_ALWAYS_MODES = frozenset({SettlementMode.COMPLETE, SettlementMode.RECEIVE_AND_DELETE})


def _within_deadline[**P, R](action: Callable[P, R]) -> Callable[P, R]:
    """Run a sync action in a daemon worker thread and give up after ``SYNC_ACTION_DEADLINE_SECONDS``
    with a ``UserException`` -- exit 1, the action's own message in the UI -- instead of letting the
    platform's 30-second limit turn a slow namespace into an opaque HTTP 500. The result or the error
    of an action that finishes in time is handed back unchanged. The worker is a daemon (and so is
    every thread the SDK starts from it), so a call still blocked in the SDK never delays the exit."""

    @wraps(action)
    def bounded(*args: P.args, **kwargs: P.kwargs) -> R:
        outcome: list[tuple[bool, Any]] = []

        def work() -> None:
            try:
                outcome.append((True, action(*args, **kwargs)))
            except BaseException as error:  # noqa: BLE001 -- handed to the calling thread and re-raised there
                outcome.append((False, error))

        name = getattr(action, "__name__", "action")
        worker = threading.Thread(target=work, name=f"sync-action-{name}", daemon=True)
        worker.start()
        worker.join(SYNC_ACTION_DEADLINE_SECONDS)
        if not outcome:
            raise UserException(SYNC_ACTION_TIMEOUT_MESSAGE.format(seconds=SYNC_ACTION_DEADLINE_SECONDS))
        finished, value = outcome[0]
        if not finished:
            raise value
        return value

    return bounded


@dataclass
class RunContext:
    """The collaborators of one run, built by ``_start_run`` and handed from step to step of ``run()``.
    ``output`` is set once the output table is open; ``peek_cursor`` starts as the input state's and is
    replaced by the peek pager's result."""

    config: Configuration
    entity: EntityRef
    state: ExtractorState
    stats: RunStats
    info: EntityInfo
    registry: FlattenRegistry | None
    pending: PendingSetBuilder
    t0: datetime
    peek_cursor: PeekCursor | None = None
    output: OutputTable | None = None


class Component(ComponentBase):
    """Extractor for Azure Service Bus queues, subscriptions and dead-letter queues."""

    @cached_property
    def _connector(self) -> ServiceBusConnector:
        """The row's connector, built on first use -- by ``run()`` or inside a sync action -- so an
        invalid auth block surfaces as that action's ``UserException`` (the sync-action wrapper
        reports it on stderr) instead of failing in ``__init__``, outside it. Only the auth block is
        parsed: the list actions and the root ``testConnection`` carry no row fields (spec §7).
        Building it installs the redacting log filter with the row's secrets; before that no secret
        is known, and ``run()`` builds it before any step logs."""
        connector = ServiceBusConnector(AuthConfiguration(**self.configuration.parameters), self._client_identifier())
        configure_logging(connector.secrets, debug=logging.getLogger().isEnabledFor(logging.DEBUG))
        return connector

    def run(self) -> None:
        """Extract one Service Bus entity per row into one Storage table (spec §6.1)."""
        config = self._load_run_config()
        run = self._start_run(config)
        with self._open_output(config, run) as output:
            processor = self._build_processor(config, run, output)
            if config.source.settlement_mode.is_destructive:
                self._commit_pending(config, run)
                self._reconcile_deferrals(config, run, processor)
                self._consume(config, run, processor)
            else:
                self._peek(config, run, processor)
        self._finish(config, run)

    # --- run steps ------------------------------------------------------------------------------------

    def _client_identifier(self) -> str:
        """``kbc-<config id or local>-<row id or root>``, at most 64 characters (spec §6.12). An
        inline-config job's config id is a hash -- fine, it is only a label."""
        env = self.environment_variables
        return f"kbc-{env.config_id or 'local'}-{env.config_row_id or 'root'}"[:64]

    def _load_run_config(self) -> Configuration:
        """The whole row, validated. The connector is built first, so auth errors are reported before
        row errors (as ever) and the log redaction is in place before any run step logs."""
        _ = self._connector
        return Configuration(**self.configuration.parameters)

    def _start_run(self, config: Configuration) -> RunContext:
        """T0, the row state, the metadata pre-check with its counts line and the effective-settings
        line (spec §6.1 steps 2-3)."""
        t0 = utc_now()
        entity = EntityRef.from_source(config.source)
        state = ExtractorState.load(self.get_state_file())
        stats = RunStats(mode=config.source.settlement_mode.value)
        info = load_entity_info(self._connector, entity)
        log_entity_counts(info, entity)
        self._check_sessions(config, entity, info)
        log_effective_settings(config, entity)
        if config.source.settlement_mode in _WRITE_ALWAYS_MODES and self.is_legacy_queue:
            # Amendment 3: keboola-component omits write_always on the legacy queue on purpose; respect it.
            stats.warn("legacy_queue_write_always", LEGACY_QUEUE_WARNING)
        registry = None
        if config.body.body_format is BodyFormat.JSON_FLATTEN:
            registry = FlattenRegistry(state.flatten_columns, reserved=metadata_column_names())
        return RunContext(
            config=config,
            entity=entity,
            state=state,
            stats=stats,
            info=info,
            registry=registry,
            pending=PendingSetBuilder(),
            t0=t0,
            peek_cursor=state.peek_cursor,
        )

    @staticmethod
    def _check_sessions(config: Configuration, entity: EntityRef, info: EntityInfo) -> None:
        """Fail fast when the management metadata shows the row's Sessions setting does not match the
        entity. A sub-queue has no sessions, and unknown metadata (no management access) is left to the
        receive-time mapping (spec §6.11)."""
        required = info.requires_session
        if required is None or entity.is_sub_queue or required == config.source.session_enabled:
            return
        if required:
            raise UserException(f"The entity '{entity.path}' requires sessions: enable Sessions in the row.")
        raise UserException(f"The entity '{entity.path}' does not use sessions: disable Sessions in the row.")

    def _open_output(self, config: Configuration, run: RunContext) -> OutputTable:
        """The output table; entering it writes the CSV header and the manifest (``write_always``
        false) before anything is settled, and leaving it flushes and closes the CSV (spec §6.9).
        Manifests go through the library's ``write_manifest`` only -- never post-processed."""
        return OutputTable(
            table_name=config.destination.table_name or run.entity.default_table_name(),
            destination=config.destination,
            body_format=config.body.body_format,
            registry=run.registry,
            create_definition=self.create_out_table_definition,
            write_manifest=self.write_manifest,
        )

    @staticmethod
    def _build_processor(config: Configuration, run: RunContext, output: OutputTable) -> BatchProcessor:
        """The write-then-settle pipeline shared by the receive loop, the orphan recovery and the peek
        pager. Only the C1 settler arms ``write_always`` (before its first complete)."""
        run.output = output
        mode = config.source.settlement_mode
        return BatchProcessor(
            entity=run.entity,
            mode=mode,
            body_format=config.body.body_format,
            sink=output,
            registry=run.registry,
            settler=make_settler(
                mode, pending=run.pending, entity=run.entity, stats=run.stats, arm=output.arm_write_always
            ),
            unreadable=UnreadableHandler(
                policy=config.body.unreadable_body, mode=mode, is_sub_queue=run.entity.is_sub_queue, stats=run.stats
            ),
            stats=run.stats,
            clock=utc_now,
        )

    def _commit_pending(self, config: Configuration, run: RunContext) -> None:
        """H5: delete the previous defer-commit run's pending set before any receive (spec §6.3); the
        session-locked groups it could not delete are carried into this run's pending set."""
        committer = PendingCommitter(self._connector, configured=run.entity, stats=run.stats)
        run.pending.carry(committer.commit(run.state.pending_commit))

    def _reconcile_deferrals(self, config: Configuration, run: RunContext, processor: BatchProcessor) -> None:
        """H3 (spec §6.4): C2 recovers the deferrals no state owns; C1 / C3 only report them."""
        source = config.source
        if source.settlement_mode is SettlementMode.DEFER_COMMIT:
            OrphanScanner(
                self._connector,
                entity=run.entity,
                info=run.info,
                session_enabled=source.session_enabled,
                batch_size=config.advanced.batch_size,
                prefetch_count=config.advanced.prefetch_count,
                processor=processor,
                stats=run.stats,
                clock=utc_now,
            ).scan()
        else:
            ForeignDeferralProbe(
                self._connector,
                entity=run.entity,
                info=run.info,
                session_enabled=source.session_enabled,
                stats=run.stats,
            ).probe()

    def _consume(self, config: Configuration, run: RunContext, processor: BatchProcessor) -> None:
        """The destructive receive loop (spec §6.5). The C2 state budget measures the whole projected
        output state; the loop arms ``write_always`` itself in C3 (the C1 settler arms it in C1)."""
        output = run.output
        assert output is not None, "_build_processor sets run.output before any run step"
        if config.source.settlement_mode is SettlementMode.RECEIVE_AND_DELETE:
            run.stats.warn(
                "at_most_once",
                "receive_and_delete deletes messages on delivery: a failure before the rows reach Storage loses them.",
            )
        prefetch = config.advanced.prefetch_count
        if prefetch > 1:
            run.stats.warn(
                "prefetch",
                f"prefetch_count is {prefetch}: up to {prefetch} messages wait in the local buffer with their locks "
                "already running, so a slow batch can lose locks (those messages redeliver and the primary key "
                "deduplicates their rows) and an interrupted connection leaves more messages locked until their locks "
                "expire. The default of 1 is recommended.",
            )
        ReceiveLoop(
            connector=self._connector,
            entity=run.entity,
            info=run.info,
            config=config,
            processor=processor,
            stats=run.stats,
            t0=run.t0,
            state_size=lambda: self._projected_state(run).size_bytes(),
            arm_write_always=output.arm_write_always,
            clock=utc_now,
        ).run()

    def _peek(self, config: Configuration, run: RunContext, processor: BatchProcessor) -> None:
        """C4 (spec §6.7): export by peeking; a pending set left by an earlier C2 period is carried
        forward untouched (spec §6.1)."""
        carried = sum(group.count for entity in run.state.pending_commit for group in entity.groups)
        if carried:
            run.stats.warn(
                "pending_carried",
                f"{carried} message(s) deferred by an earlier defer-commit run are still pending; they are deleted "
                "by the next run in a destructive mode.",
            )
        run.peek_cursor = PeekPager(
            connector=self._connector,
            entity=run.entity,
            info=run.info,
            config=config,
            processor=processor,
            stats=run.stats,
            cursor=run.state.peek_cursor,
            t0=run.t0,
            clock=utc_now,
        ).run()

    @staticmethod
    def _projected_state(run: RunContext) -> ExtractorState:
        """The whole output state this run would write (merge rule, spec §6.8): destructive modes own
        ``pending_commit``, peek with incremental fetch owns ``peek_cursor``, flatten owns
        ``flatten_columns``; every other key is carried unchanged from the input state."""
        state = run.state.model_copy(deep=True)
        source = run.config.source
        if source.settlement_mode.is_destructive:
            state.pending_commit = run.pending.build(format_timestamp(utc_now()))
        elif source.fetch_mode is FetchMode.INCREMENTAL_FETCH:
            state.peek_cursor = run.peek_cursor
        if run.registry is not None:
            state.flatten_columns = run.registry.to_state()
        return state

    def _finish(self, config: Configuration, run: RunContext) -> None:
        """Write the new row state once, at the end (spec §6.8), and log the run summary (§6.12)."""
        self.write_state_file(self._projected_state(run).to_dict())
        run.stats.note_throttled(throttled_requests())
        run.stats.log_summary()

    # --- sync actions (spec §5.4) ---------------------------------------------------------------------

    @sync_action("testConnection")
    @_within_deadline
    def test_connection(self) -> ValidationResult:
        """The root **Test Connection**: a management listing proves service-principal / Manage-SAS
        auth. Only the auth block is read -- a row's **Preview Messages** proves entity access."""
        probe_management(self._connector)
        return ValidationResult("Connected to the Service Bus namespace.", MessageType.SUCCESS)

    @sync_action("listQueues")
    @_within_deadline
    def list_queues(self) -> list[SelectElement]:
        return self._select_elements("queues")

    @sync_action("listTopics")
    @_within_deadline
    def list_topics(self) -> list[SelectElement]:
        return self._select_elements("topics")

    @sync_action("listSubscriptions")
    @_within_deadline
    def list_subscriptions(self) -> list[SelectElement]:
        topic_name = self._sync_configuration().topic_name
        return self._select_elements("subscriptions", topic_name)  # no topic -> "Select a topic first."

    @sync_action("previewMessages")
    @_within_deadline
    def preview_messages(self) -> ValidationResult:
        """A markdown table of up to ten peeked messages; nothing is locked or settled."""
        messages = self._peek_row_entity(PREVIEW_MESSAGE_COUNT)
        if not messages:
            return ValidationResult("The entity has no messages to preview.", MessageType.INFO)
        return ValidationResult(render_preview(messages), MessageType.TABLE)

    def _sync_configuration(self) -> SyncActionConfiguration:
        """The partial model of ``listSubscriptions``: auth plus the selected topic of a row that may
        still be half filled in."""
        return SyncActionConfiguration(**self.configuration.parameters)

    def _entity_configuration(self) -> EntityConfiguration:
        """The partial model of ``previewMessages``, which opens the row's entity: auth + source only
        (spec §5.4), so an unrelated half-edited field never blocks it."""
        return EntityConfiguration(**self.configuration.parameters)

    def _select_elements(
        self, kind: Literal["queues", "topics", "subscriptions"], topic_name: str | None = None
    ) -> list[SelectElement]:
        return [SelectElement(value=name, label=name) for name in list_entity_names(self._connector, kind, topic_name)]

    def _peek_row_entity(self, max_message_count: int) -> list[Any] | None:
        """Peek the row's entity from its first message on a PEEK_LOCK receiver with the thread-free
        profile (spec §6.2); a session entity takes the next available session for a moment. ``None``
        when a session entity has no session with an available message."""
        config = self._entity_configuration()
        entity = EntityRef.from_source(config.source)
        sessions = config.source.session_enabled
        kwargs: dict[str, Any] = {
            "receive_mode": ServiceBusReceiveMode.PEEK_LOCK,
            "prefetch_count": 1,
            "keep_alive": 0,
            "client_identifier": self._connector.client_identifier,
        }
        if sessions:
            kwargs |= {"session_id": NEXT_AVAILABLE_SESSION, "max_wait_time": SESSION_ACCEPT_WAIT_SECONDS}
        # A session accept that finds no session must fail after one wait, not after four (spec §5.4).
        client_factory = self._connector.session_probe_client if sessions else self._connector.receive_client
        try:
            with client_factory() as client, entity.open_receiver(client, **kwargs) as receiver:
                return receiver.peek_messages(max_message_count)
        except OperationTimeoutError as e:
            if sessions:
                return None
            raise to_user_exception(e, entity.path, self._connector.secrets) from e
        except ServiceBusError as e:
            raise to_user_exception(e, entity.path, self._connector.secrets) from e


if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as e:
        logger.error(redact_secrets(str(e)))
        sys.exit(1)
    except Exception:
        logger.exception("Component failed with an unexpected error")
        sys.exit(2)
