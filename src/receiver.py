"""Receive loop (C1 / C2 / C3), peek pager (C4) and connection-recovery bookkeeping
(Tasks 14-15, spec §6.2, §6.5, §6.6, §6.7, §6.11).

``ReceiveLoop`` drives one destructive run: batches of ``receive_messages`` go through the shared
``BatchProcessor`` (write first, then settle) until a stop rule fires; then the stop drain empties the
local receive buffer -- C1 / C2 abandon the drained messages so they are available again at once, C3
writes them because RECEIVE_AND_DELETE has already deleted them on the broker. Session entities loop
``NEXT_AVAILABLE_SESSION`` receivers, one session at a time.

``PeekPager`` exports C4 by peeking -- nothing is locked, settled or deleted: incremental fetch pages
from the stored cursor, full fetch from the start (cursor mode on partitioned entities, one held
receiver per session on session entities). Expired messages and scheduled messages pending
activation are skipped and counted; a scheduled message is exported once active.

Any failure that is neither the processor's own ``UserException`` (``fail`` policy, flatten cap, abort
share -- the run ends, after the stop drain in the receive loop) nor a configuration / auth / entity
error closes the receiver and its client and opens fresh ones. ``RecoveryTracker`` caps those
connection recoveries (5 per run, and never twice without progress in between); the unreadable-body
retry recycles the connection on its own budget and never counts as a recovery. Every SDK call runs
on the main thread; message bodies are never logged.
"""

import logging
import time
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime
from enum import StrEnum
from functools import partial
from typing import Any

from azure.servicebus import NEXT_AVAILABLE_SESSION, ServiceBusClient, ServiceBusMessageState, ServiceBusReceiveMode
from azure.servicebus.exceptions import (
    MessagingEntityDisabledError,
    MessagingEntityNotFoundError,
    OperationTimeoutError,
    ServiceBusAuthenticationError,
    ServiceBusAuthorizationError,
    ServiceBusError,
    SessionCannotBeLockedError,
)
from keboola.component.exceptions import UserException

from client import ServiceBusConnector, is_session_mismatch, redact_secrets, to_user_exception
from configuration import Configuration, FetchMode, SettlementMode
from entity import EntityInfo, EntityRef
from settlement import LOCK_RENEW_MARGIN, BatchProcessor, BatchResult, safe_settle
from state import STATE_BUDGET_BYTES, PeekCursor
from stats import RunStats

logger = logging.getLogger(__name__)

MAX_RECOVERIES = 5
# The accept wait of every session receiver outside the destructive receive loop (C4 peek,
# testConnection, previewMessages): idle_timeout_seconds is hidden -- and so ignored -- there.
SESSION_ACCEPT_WAIT_SECONDS = 5
DRAIN_WAIT_SECONDS = 1
CATCH_UP_POLL_SECONDS = 1
PEEK_PAGE_SIZE = 250  # records per peek call: the broker's cap (read at call time; tests monkeypatch it)

_PARTITIONED_INCREMENTAL = "Incremental Fetch cannot page a partitioned entity reliably; use Full Fetch."

# Errors a fresh connection cannot fix (spec §6.11); plus the session mismatch (``is_session_mismatch``).
FATAL_ERRORS = (
    ServiceBusAuthenticationError,
    ServiceBusAuthorizationError,
    MessagingEntityNotFoundError,
    MessagingEntityDisabledError,
)

_RECEIVE_MODES = {
    SettlementMode.COMPLETE: ServiceBusReceiveMode.PEEK_LOCK,
    SettlementMode.DEFER_COMMIT: ServiceBusReceiveMode.PEEK_LOCK,
    SettlementMode.RECEIVE_AND_DELETE: ServiceBusReceiveMode.RECEIVE_AND_DELETE,
}


class StopReason(StrEnum):
    IDLE = "idle"
    MAX_MESSAGES = "max_messages"
    MAX_DURATION = "max_duration"
    WATERMARK = "watermark"
    STATE_BUDGET = "state_budget"
    NO_MORE_SESSIONS = "no_more_sessions"
    SESSION_REVISITED = "session_revisited"
    END_OF_ENTITY = "end_of_entity"


def is_fatal(error: BaseException) -> bool:
    """A configuration, auth or entity error (or the session mismatch): mapped, never recycled."""
    return isinstance(error, FATAL_ERRORS) or is_session_mismatch(error)


class RecoveryTracker:
    """Connection-recovery budget and the no-progress guard (spec §6.5).

    ``generation`` identifies the current connection: it grows on every reopen -- a connection
    recovery or an unreadable-body retry -- so the unreadable handler can tell a second failure on a
    fresh connection from the first. ``count`` counts connection recoveries only.
    """

    def __init__(
        self, max_recoveries: int = MAX_RECOVERIES, *, entity_path: str | None = None, secrets: Iterable[str] = ()
    ) -> None:
        self.max_recoveries = max_recoveries
        self.generation = 0
        self.count = 0
        self._entity_path = entity_path
        self._secrets = tuple(secrets)
        self._progressed = True  # no connection failure yet: the first one is always recycled

    def progress(self) -> None:
        """A row was written or an unreadable body was finally disposed of."""
        self._progressed = True

    def unreadable_retry(self) -> None:
        """The processor asked for a fresh connection for an unreadable body -- its own budget, not a
        connection failure: neither ``count`` nor the progress flag changes."""
        self.generation += 1

    def failure(self, error: Exception) -> None:
        """Account for a connection failure, or raise when it must end the run.

        A ``UserException`` (the processor's ``fail`` policy / flatten cap / abort share) is re-raised
        unchanged and never counted; a fatal error is mapped to a ``UserException``; a failure with no
        progress since the previous one, or beyond ``max_recoveries``, ends the run -- as a
        ``UserException`` for a ``ServiceBusError``, anything else unchanged (exit 2)."""
        if isinstance(error, UserException):
            raise error
        if isinstance(error, ServiceBusError) and is_fatal(error):
            raise to_user_exception(error, self._entity_path, self._secrets) from error
        if self.count >= self.max_recoveries or not self._progressed:
            if isinstance(error, ServiceBusError):
                raise UserException(
                    f"Lost the connection to Service Bus {self.count + 1} times in this run (limit "
                    f"{self.max_recoveries}, or twice without writing a row or disposing an unreadable message in "
                    f"between); last error: {redact_secrets(str(error), self._secrets)}. Messages that were not "
                    "settled redeliver on the next run."
                ) from error
            raise error
        self.count += 1
        self.generation += 1
        self._progressed = False


def _receiver_profile(
    connector: ServiceBusConnector,
    receive_mode: ServiceBusReceiveMode,
    prefetch_count: int,
    *,
    session_wait: float | None,
) -> dict[str, Any]:
    """The §6.2 receiver kwargs: ``prefetch_count >= 1``, no keep-alive thread, the row's client
    identifier; ``session_wait`` set = a ``NEXT_AVAILABLE_SESSION`` receiver waiting that long."""
    kwargs: dict[str, Any] = {
        "receive_mode": receive_mode,
        "prefetch_count": prefetch_count,
        "keep_alive": 0,
        "client_identifier": connector.client_identifier,
    }
    if session_wait is not None:
        kwargs |= {"session_id": NEXT_AVAILABLE_SESSION, "max_wait_time": session_wait}
    return kwargs


def _enter(receiver: Any) -> Any:
    """Open (attach) ``receiver`` so entity, auth and session errors surface here, inside the caller's
    recovery ``try``; a receiver that fails to open is closed before the error propagates."""
    try:
        return receiver.__enter__()
    except Exception:
        _close_quietly(receiver)
        raise


def _close_quietly(handler: Any) -> None:
    """Close a receiver or client; a broken link may fail to close, which must never mask the error
    that ended the run (the next open starts afresh anyway)."""
    try:
        handler.close()
    except Exception as error:  # noqa: BLE001 -- best-effort cleanup
        logger.debug("Closing a Service Bus handler failed: %s", type(error).__name__)


def _renew_session_if_needed(receiver: Any, now: datetime) -> None:
    """Renew the session lock from the main thread when it lapses within ``LOCK_RENEW_MARGIN`` (D10);
    session receivers hold no message locks (``locked_until_utc`` is ``None`` on their messages)."""
    session = receiver.session
    locked_until = session.locked_until_utc if session is not None else None
    if locked_until is not None and locked_until - now < LOCK_RENEW_MARGIN:
        session.renew_lock()


def _after_watermark(message: Any, t0: datetime) -> bool:
    """Enqueued at or after T0. ``SCHEDULED``-state messages never count -- harmless either way: a
    pending one is skipped in C4 (``is_pending_activation``), and a message activated from a schedule
    can still report ``SCHEDULED`` on 7.14.3 [live, Phase 7], so ignoring it at most delays a stop."""
    enqueued = message.enqueued_time_utc
    return message.state != ServiceBusMessageState.SCHEDULED and enqueued is not None and enqueued >= t0


def is_pending_activation(message: Any) -> bool:
    """A scheduled message that has not activated yet: C4 skips it and exports its activated copy.

    Phase-7 probe [live]: activation re-enqueues a scheduled message under a new sequence number with
    the activation time as its ``enqueued_time_utc`` (``scheduled_enqueue_time_utc`` survives), while a
    peeked pending one reports its send time -- before its schedule. A received activated message can
    still report ``SCHEDULED`` on 7.14.3 [live]; that a peek of one can too is [inferred]. So a
    ``SCHEDULED`` message enqueued at or after its schedule is the activated copy, never pending."""
    if message.state != ServiceBusMessageState.SCHEDULED:
        return False
    scheduled, enqueued = message.scheduled_enqueue_time_utc, message.enqueued_time_utc
    return scheduled is None or enqueued is None or enqueued < scheduled


def _describe(error: BaseException, secrets: Iterable[str]) -> str:
    return redact_secrets(f"{type(error).__name__}: {error}", secrets)


def _log_recovery(error: BaseException, tracker: RecoveryTracker, secrets: Iterable[str]) -> None:
    logger.warning(
        "The Service Bus connection failed (%s); reopening it (recovery %d of %d). Messages of an interrupted "
        "batch redeliver once their lock expires.",
        _describe(error, secrets),
        tracker.count,
        tracker.max_recoveries,
    )


class ReceiveLoop:
    """One destructive run (C1 / C2 / C3) over the row's entity (spec §6.5). ``processor`` and
    ``arm_write_always`` are public attributes (the tests replace them)."""

    def __init__(
        self,
        *,
        connector: ServiceBusConnector,
        entity: EntityRef,
        info: EntityInfo,
        config: Configuration,
        processor: BatchProcessor,
        stats: RunStats,
        t0: datetime,
        state_size: Callable[[], int],
        arm_write_always: Callable[[], None],
        clock: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] | None = None,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        mode = config.source.settlement_mode
        if mode not in _RECEIVE_MODES:
            raise ValueError("the receive loop runs the destructive settlement modes; peek mode uses PeekPager")
        self._connector = connector
        self._entity = entity
        self._info = info
        self._config = config
        self.processor = processor
        self._stats = stats
        self._t0 = t0
        self._state_size = state_size
        self.arm_write_always = arm_write_always
        self._clock = clock or (lambda: datetime.now(UTC))
        self._monotonic = monotonic or time.monotonic
        self._sleep = sleep or time.sleep
        self._mode = mode
        self._sessions = config.source.session_enabled
        self._tracker = RecoveryTracker(entity_path=entity.path, secrets=connector.secrets)
        self._client: ServiceBusClient | None = None
        self._receiver: Any = None
        # The receive link is attached with no credit: nothing is buffered before its first receive [source:
        # pyamqp ReceiveClient._client_ready / _client_run], so a receiver that never received needs no drain.
        self._received_on_receiver = False
        self._session_id: str | None = None  # the session the open receiver holds
        self._seen_sessions: set[str] = set()
        self._taken = 0  # messages handled for max_messages; a retried body counts once it comes back
        self._armed = False
        self._deadline = 0.0
        self._wait: float = config.source.idle_timeout_seconds  # 1 s while catching up

    def run(self) -> StopReason:
        self._deadline = self._monotonic() + self._config.limits.max_duration_seconds
        try:
            reason = self._consume(catch_up_until=None)
            if self._catch_up_due(reason):
                reason = self._catch_up()
        finally:
            self._close()
        self._stats.stop_reason = reason.value
        return reason

    # --- the loop -------------------------------------------------------------------------------------

    def _consume(self, *, catch_up_until: float | None) -> StopReason:
        """Steps until a stop. Every receiver is opened inside this ``try``, so an error at open
        (auth, missing entity, session mismatch) is mapped or recycled like one on a call."""
        while True:
            try:
                reason = self._step(catch_up_until)
            except UserException:
                raise  # the processor's own failure (already drained) or a configuration problem
            except Exception as error:  # noqa: BLE001 -- a recycle candidate: the tracker re-raises what ends the run
                if self._sessions and isinstance(error, SessionCannotBeLockedError):
                    logger.info("A session could not be locked (another receiver holds it); skipping it.")
                    self._finish_session()
                else:
                    self._recover(error)
                continue
            if reason is not None:
                return reason

    def _step(self, catch_up_until: float | None) -> StopReason | None:
        """Open the next receiver, or receive and process one batch. ``None``: keep going."""
        receiver = self._receiver
        stop = self._stop_check(catch_up_until)
        if stop is not None:
            if receiver is not None:
                self._stop_drain(receiver, failing=False)
            return stop
        if receiver is None:
            return self._open_next(catch_up_until)
        if self._sessions:
            _renew_session_if_needed(receiver, self._clock())
        batch = self._receive(receiver, self._batch_count(), self._wait)
        if not batch:
            return self._on_empty(catch_up_until)
        result = self._process(receiver, batch)
        if self._config.limits.stop_at_job_start and self._is_watermark_batch(batch):
            return self._on_watermark(receiver)
        if result.needs_recycle:
            # A fresh connection for the unreadable bodies (they were abandoned and redeliver at once);
            # the unreadable budget pays for it, the connection-recovery budget does not.
            self._close(interrupted=True)
            self._tracker.unreadable_retry()
            self._stats.unreadable_recycles += 1
        return None

    def _stop_check(self, catch_up_until: float | None) -> StopReason | None:
        limit = self._config.limits.max_messages
        if limit > 0 and self._taken >= limit:
            return StopReason.MAX_MESSAGES
        now = self._monotonic()
        if now >= self._deadline:
            return StopReason.MAX_DURATION
        if self._mode is SettlementMode.DEFER_COMMIT and self._state_size() >= STATE_BUDGET_BYTES:
            self._stats.warn(
                "state_budget",
                f"The state this defer-commit run would save reached the {STATE_BUDGET_BYTES // 1024} KiB state "
                "budget (Keboola keeps about 1 MB of state per configuration), so the run stopped receiving early; "
                "the next run deletes this run's deferrals first and continues.",
            )
            return StopReason.STATE_BUDGET
        if catch_up_until is not None and now >= catch_up_until:
            return StopReason.NO_MORE_SESSIONS if self._sessions else StopReason.IDLE
        return None

    def _batch_count(self) -> int:
        batch_size, limit = self._config.advanced.batch_size, self._config.limits.max_messages
        return min(batch_size, limit - self._taken) if limit > 0 else batch_size

    def _open_next(self, catch_up_until: float | None) -> StopReason | None:
        if self._client is None:
            self._client = self._connector.receive_client()
        if not self._sessions:
            self._receiver = self._open_receiver(self._client, session=False)
            return None
        try:
            receiver = self._open_receiver(self._client, session=True)
        except OperationTimeoutError:  # no session with an available message
            if catch_up_until is None:
                return StopReason.NO_MORE_SESSIONS
            self._sleep(CATCH_UP_POLL_SECONDS)
            return None
        self._receiver = receiver
        session_id = str(receiver.session.session_id)
        if session_id in self._seen_sessions:
            # Handed out a second time: it still holds messages enqueued after T0 -- end the session loop.
            self._stop_drain(receiver, failing=False)
            return StopReason.SESSION_REVISITED
        self._seen_sessions.add(session_id)
        self._session_id = session_id
        return None

    def _open_receiver(self, client: ServiceBusClient, *, session: bool) -> Any:
        """The §6.2 profile: the mode's receive mode, ``prefetch_count``, no keep-alive thread, the
        row's client identifier; session receivers wait ``idle_timeout_seconds`` for a session."""
        kwargs = _receiver_profile(
            self._connector,
            _RECEIVE_MODES[self._mode],
            self._config.advanced.prefetch_count,
            session_wait=self._wait if session else None,
        )
        receiver = _enter(self._entity.open_receiver(client, **kwargs))
        self._received_on_receiver = False
        return receiver

    def _receive(self, receiver: Any, count: int, wait: float) -> list[Any]:
        self._received_on_receiver = True
        batch = receiver.receive_messages(max_message_count=count, max_wait_time=wait)
        for message in batch:
            self._info.note_sequence_number(message.sequence_number)
        if batch and self._mode is SettlementMode.RECEIVE_AND_DELETE and not self._armed:
            # C3: the broker deleted these on receive -- arm before the first of them is written (§6.9).
            self.arm_write_always()
            self._armed = True
        return batch

    def _process(self, receiver: Any, batch: Sequence[Any]) -> BatchResult:
        written = self._stats.written
        try:
            result = self.processor.process(receiver, batch, self._tracker.generation)
        except UserException:
            # Write first, fail after (amendments 1 + 2): the batch's readable rows are written and
            # settled. Never a connection failure -- drain the still-open receiver, then re-raise.
            self._stop_drain(receiver, failing=True)
            raise
        finally:
            # Rows written before a settle-time connection error still count as progress.
            if self._stats.written > written:
                self._tracker.progress()
        if result.progressed:
            self._tracker.progress()
        self._taken += len(batch) - len(result.retried)
        return result

    def _is_watermark_batch(self, batch: Sequence[Any]) -> bool:
        considered = [message for message in batch if message.state != ServiceBusMessageState.SCHEDULED]
        return bool(considered) and all(_after_watermark(message, self._t0) for message in considered)

    def _on_empty(self, catch_up_until: float | None) -> StopReason | None:
        if self._sessions:
            self._finish_session()  # this session is drained: on to the next one
            return None
        if catch_up_until is None:
            return StopReason.IDLE
        self._sleep(CATCH_UP_POLL_SECONDS)
        return None

    def _on_watermark(self, receiver: Any) -> StopReason | None:
        if self._info.is_partitioned:
            self._stats.warn(
                "watermark_approximate",
                f"'{self._entity.path}' is partitioned, so messages are not received in enqueue order: the stop at "
                "the job start time is approximate (it can end early or late; nothing is lost).",
            )
        self._stop_drain(receiver, failing=False)
        if self._sessions:
            self._finish_session()  # a per-session watermark: continue with the next session
            return None
        return StopReason.WATERMARK

    # --- stop drain, recovery, catch-up ---------------------------------------------------------------

    def _stop_drain(self, receiver: Any, *, failing: bool) -> None:
        """Empty the local receive buffer at a stop (C8): C1 / C2 abandon the drained messages, C3
        writes them (already deleted on the broker), even past ``max_messages``. ``failing``: the run
        is about to raise the processor's ``UserException`` -- a further one from the drained batch is
        swallowed (its readable rows are written) and nothing here may mask the original. A receiver
        that never received (a session just handed out again) has nothing buffered: no drain."""
        if not self._received_on_receiver:
            return
        try:
            if self._sessions:
                _renew_session_if_needed(receiver, self._clock())
            drained = self._receive(receiver, self._config.advanced.prefetch_count + 1, DRAIN_WAIT_SECONDS)
            if not drained:
                return
            if self._mode is SettlementMode.RECEIVE_AND_DELETE:
                self.processor.process(receiver, drained, self._tracker.generation)
                return
            for message in drained:
                safe_settle(partial(receiver.abandon_message, message), self._stats)
            logger.info("Abandoned %d buffered message(s) at the stop; they are available again at once.", len(drained))
        except UserException as error:
            if not failing:
                raise
            logger.warning(
                "The messages drained before the run fails raised a further error (their readable rows were "
                "written): %s",
                redact_secrets(str(error), self._connector.secrets),
            )
        except Exception as error:  # noqa: BLE001 -- the drain is best-effort; it must not change the run's outcome
            if self._mode is SettlementMode.RECEIVE_AND_DELETE:
                consequence = "messages already in the local buffer were deleted on receive and may be lost"
            else:
                consequence = "buffered messages stay locked until their lock expires, then redeliver"
            logger.warning("The stop drain failed (%s); %s.", _describe(error, self._connector.secrets), consequence)

    def _recover(self, error: Exception) -> None:
        """Close the receiver and its client, then let the tracker decide; the next step reopens."""
        self._close(interrupted=True)
        self._tracker.failure(error)
        self._stats.recoveries = self._tracker.count
        _log_recovery(error, self._tracker, self._connector.secrets)

    def _catch_up_due(self, reason: StopReason) -> bool:
        return (
            self._config.advanced.recovery_wait_seconds > 0
            and self._tracker.count > 0
            and reason in (StopReason.IDLE, StopReason.NO_MORE_SESSIONS)
        )

    def _catch_up(self) -> StopReason:
        """D6: keep polling (1 s waits, 1 s sleep after each empty poll) for up to
        ``recovery_wait_seconds``, bounded by ``max_duration_seconds``, to collect the messages of
        interrupted batches once their locks lapse."""
        wait = self._config.advanced.recovery_wait_seconds
        until = min(self._monotonic() + wait, self._deadline)
        logger.info(
            "Collecting messages redelivered after %d connection recovery(ies) for up to %d s.",
            self._tracker.count,
            wait,
        )
        self._wait = CATCH_UP_POLL_SECONDS
        return self._consume(catch_up_until=until)

    def _finish_session(self) -> None:
        """Close the session's receiver (releasing the session lock); the client stays open."""
        receiver, self._receiver, self._session_id = self._receiver, None, None
        if receiver is not None:
            _close_quietly(receiver)

    def _close(self, *, interrupted: bool = False) -> None:
        """Close the receiver and its client. ``interrupted``: the open session was not finished, so
        being handed it again continues it rather than counting as a revisit."""
        if interrupted and self._session_id is not None:
            self._seen_sessions.discard(self._session_id)
        receiver, client = self._receiver, self._client
        self._receiver = self._client = self._session_id = None
        for handler in (receiver, client):
            if handler is not None:
                _close_quietly(handler)


class PeekPager:
    """C4 (spec §6.7): export the entity by peeking. Incremental fetch pages explicitly from the
    stored cursor and returns the new one; full fetch reads everything from the start and returns
    ``None`` -- explicit paging on plain entities, cursor mode on partitioned ones, one held
    ``NEXT_AVAILABLE_SESSION`` receiver per session on session entities. ``processor`` and ``config``
    are public attributes (the tests adjust / replace them); ``config`` is read when ``run`` starts."""

    def __init__(
        self,
        *,
        connector: ServiceBusConnector,
        entity: EntityRef,
        info: EntityInfo,
        config: Configuration,
        processor: BatchProcessor,
        stats: RunStats,
        cursor: PeekCursor | None,
        t0: datetime,
        clock: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self._connector = connector
        self._entity = entity
        self._info = info
        self.config = config
        self.processor = processor
        self._stats = stats
        self._cursor = cursor
        self._t0 = t0
        self._clock = clock or (lambda: datetime.now(UTC))
        self._monotonic = monotonic or time.monotonic
        self._tracker = RecoveryTracker(entity_path=entity.path, secrets=connector.secrets)
        self._client: ServiceBusClient | None = None
        self._receiver: Any = None  # the plain (non-session) receiver
        self._held: list[Any] = []  # session receivers, closed only when the pager finishes
        # Sequence numbers processed (written, skipped as unreadable or expired): dropped when a page is
        # peeked again. Full fetch keeps them for the run (a cursor-mode pass restarts from the start);
        # incremental fetch only for the current page (explicit paging never goes back further).
        self._processed: set[int] = set()
        self._high: int | None = None  # the highest processed sequence number
        self._outstanding: int | None = None  # the first unreadable body still waiting for its re-peek
        self._taken = 0
        self._deadline = 0.0

    def run(self) -> PeekCursor | None:
        source = self.config.source
        incremental = source.fetch_mode is FetchMode.INCREMENTAL_FETCH
        if incremental and self._info.partitioned is True:
            raise UserException(_PARTITIONED_INCREMENTAL)
        start = self._start() if incremental else 1
        self._deadline = self._monotonic() + self.config.limits.max_duration_seconds
        expired, scheduled = self._stats.expired_skipped, self._stats.skipped_scheduled
        unreadable = self.processor.unreadable
        retries_enabled = unreadable.retries_enabled
        try:
            if source.session_enabled:
                reason = self._run_sessions()
            else:
                reason = self._run_pages(start, incremental=incremental)
        finally:
            unreadable.retries_enabled = retries_enabled
            self._close()
        self._stats.stop_reason = reason.value
        if self._stats.expired_skipped > expired:
            logger.info(
                "Skipped %d expired message(s) that Service Bus has not purged yet.",
                self._stats.expired_skipped - expired,
            )
        if self._stats.skipped_scheduled > scheduled:
            logger.info(
                "Skipped %d scheduled message(s) that are not active yet; each is exported once Service Bus "
                "activates it, under the new sequence number it gets then.",
                self._stats.skipped_scheduled - scheduled,
            )
        return self._new_cursor(start) if incremental else None

    def _start(self) -> int:
        cursor = self._cursor
        if cursor is None:
            return 1
        if cursor.entity_path == self._entity.path:
            return cursor.last_sequence_number + 1
        logger.info(
            "The stored peek cursor belongs to '%s', not '%s': peeking from the first message.",
            cursor.entity_path,
            self._entity.path,
        )
        return 1

    def _new_cursor(self, start: int) -> PeekCursor | None:
        """The highest processed sequence number -- never past a body still waiting for its retry;
        the stored cursor unchanged when nothing new was processed."""
        high = self._high
        if high is not None and self._outstanding is not None:
            high = min(high, self._outstanding - 1)
        if high is None or high < start:
            return self._cursor
        return PeekCursor(entity_path=self._entity.path, last_sequence_number=high)

    # --- plain entities: explicit paging, or cursor mode on partitioned ones ----------------------------

    def _run_pages(self, start: int, *, incremental: bool) -> StopReason:
        # Cursor mode (sequence_number=0) on partitioned entities: explicit paging missed partitions,
        # cursor mode visited all of them [live]. It cannot resume at a sequence number, so a first
        # unreadable failure is final there (the next full fetch reads the message again).
        cursor_mode = not incremental and self._info.is_partitioned
        if cursor_mode:
            self.processor.unreadable.retries_enabled = False
        next_seq = start
        while True:
            try:
                stop = self._stop_check()
                if stop is not None:
                    return stop
                if self._receiver is None:
                    self._receiver = self._open(session=False)
                receiver = self._receiver
                page = receiver.peek_messages(PEEK_PAGE_SIZE, sequence_number=0 if cursor_mode else next_seq)
                if not page:
                    return StopReason.END_OF_ENTITY
                if not cursor_mode and self._reveals_partitions(page):
                    if incremental:
                        raise UserException(_PARTITIONED_INCREMENTAL)  # nothing of this page is written
                    logger.info(
                        "'%s' is partitioned: reading it again from the start in cursor mode; messages already "
                        "exported in this run are skipped.",
                        self._entity.path,
                    )
                    cursor_mode = True
                    self.processor.unreadable.retries_enabled = False
                    self._close_receiver()  # a fresh receiver: its peek cursor starts at the beginning
                    continue
                stop, retried = self._export(receiver, page)
                if retried:
                    # Explicit paging: re-peek from the first unreadable body on a fresh connection; the
                    # page's processed messages are dropped from the re-peeked page (no duplicate rows).
                    self._close()
                    self._tracker.unreadable_retry()
                    self._stats.unreadable_recycles += 1
                    next_seq = min(retried)
                    continue
                if stop is not None:
                    return stop
                next_seq = page[-1].sequence_number + 1
                if incremental:
                    self._processed.clear()
            except UserException:
                raise  # the processor's own failure or a configuration problem: peek deleted nothing
            except Exception as error:  # noqa: BLE001 -- a recycle candidate: the tracker re-raises what ends the run
                self._recover(error)

    def _reveals_partitions(self, page: Sequence[Any]) -> bool:
        """The sequence-number heuristic (§6.7) for an entity whose partitioning is unknown."""
        for message in page:
            self._info.note_sequence_number(message.sequence_number)
        return self._info.is_partitioned

    # --- session entities (full fetch only) -------------------------------------------------------------

    def _run_sessions(self) -> StopReason:
        # A per-session peek cannot resume at a sequence number on a fresh connection either: a first
        # unreadable failure is final (the next full fetch reads the message again).
        self.processor.unreadable.retries_enabled = False
        self._stats.warn(
            "peek_sessions",
            f"Full Fetch on the session entity '{self._entity.path}' reads only the sessions Service Bus hands out: "
            "sessions locked by other consumers are skipped and sessions holding only deferred messages are not "
            "visible. Each visited session stays locked until the peek finishes.",
        )
        seen: set[str] = set()
        while True:
            try:
                stop = self._stop_check()
                if stop is not None:
                    return stop
                try:
                    receiver = self._open(session=True)
                except OperationTimeoutError:  # no further session with an available message
                    return StopReason.NO_MORE_SESSIONS
                # Held until the pager finishes: a peeked session keeps its messages, so once released it
                # would be handed out again -- ending the loop as revisited before later sessions are seen.
                self._held.append(receiver)
                session_id = str(receiver.session.session_id)
                if session_id in seen:
                    return StopReason.SESSION_REVISITED
                seen.add(session_id)
                stop = self._peek_session(receiver)
                if stop is not None:
                    return stop
            except UserException:
                raise
            except SessionCannotBeLockedError:
                logger.info("A session could not be locked (another receiver holds it); skipping it.")
            except Exception as error:  # noqa: BLE001 -- a recycle candidate: the tracker re-raises what ends the run
                self._recover(error)
                seen.clear()  # every held session was released: peek them again (processed ones are dropped)

    def _peek_session(self, receiver: Any) -> StopReason | None:
        """Peek one session from its first message. ``None``: the session is done (or reached T0)."""
        next_seq = 1
        while True:
            stop = self._stop_check()
            if stop is not None:
                return stop
            _renew_session_if_needed(receiver, self._clock())
            page = receiver.peek_messages(PEEK_PAGE_SIZE, sequence_number=next_seq)
            if not page:
                return None
            stop, _ = self._export(receiver, page)
            if stop is StopReason.WATERMARK:
                return None  # this session reached T0: on to the next one
            if stop is not None:
                return stop
            next_seq = page[-1].sequence_number + 1

    # --- shared -------------------------------------------------------------------------------------

    def _stop_check(self) -> StopReason | None:
        limit = self.config.limits.max_messages
        if limit > 0 and self._taken >= limit:
            return StopReason.MAX_MESSAGES
        if self._monotonic() >= self._deadline:
            return StopReason.MAX_DURATION
        return None

    def _export(self, receiver: Any, page: Sequence[Any]) -> tuple[StopReason | None, list[int]]:
        """Write one page: drop what this run already processed, skip the scheduled messages pending
        activation and the expired ones, stop before the first message at or after T0 or at
        ``max_messages``. Returns the stop (if any) and the sequence numbers that asked for a
        fresh-connection retry."""
        limits = self.config.limits
        now = self._clock()
        selected: list[Any] = []
        stop: StopReason | None = None
        for message in page:
            seq = message.sequence_number
            if seq in self._processed:
                continue
            if limits.max_messages > 0 and self._taken + len(selected) >= limits.max_messages:
                stop = StopReason.MAX_MESSAGES
                break
            if is_pending_activation(message):
                # Processed, so the cursor moves past it: its activated copy gets a new, higher sequence
                # number. Checked before expiry: a pending message's expires_at_utc counts from its send
                # time (the SDK adds time_to_live to enqueued_time_utc [source]).
                self._stats.skipped_scheduled += 1
                self._mark_processed(seq)
                continue
            expires_at = message.expires_at_utc
            if expires_at is not None and expires_at < now:  # expired, not purged yet: peek still returns it [live]
                self._stats.expired_skipped += 1
                self._mark_processed(seq)
                continue
            if limits.stop_at_job_start and _after_watermark(message, self._t0):
                stop = StopReason.WATERMARK
                break
            selected.append(message)
        retried = self._process(receiver, selected) if selected else []
        self._outstanding = min(retried, default=None)
        if stop is StopReason.WATERMARK and (self._info.is_partitioned or self.config.source.session_enabled):
            self._stats.warn(
                "watermark_approximate",
                f"'{self._entity.path}' is partitioned or uses sessions, so messages are not peeked in enqueue "
                "order: the stop at the job start time is approximate (it can end early or late; nothing is lost).",
            )
        return stop, retried

    def _process(self, receiver: Any, messages: Sequence[Any]) -> list[int]:
        """Hand the page to the processor (its ``UserException`` propagates unchanged: never a recycle
        or a re-peek); returns the sequence numbers that asked for a retry."""
        written = self._stats.written
        try:
            result = self.processor.process(receiver, messages, self._tracker.generation)
        finally:
            if self._stats.written > written:
                self._tracker.progress()
        if result.progressed:
            self._tracker.progress()
        retried = set(result.retried)
        for message in messages:
            if message.sequence_number not in retried:
                self._mark_processed(message.sequence_number)
                self._taken += 1
        return result.retried

    def _mark_processed(self, seq: int) -> None:
        self._processed.add(seq)
        self._high = seq if self._high is None else max(self._high, seq)

    def _open(self, *, session: bool) -> Any:
        """The §6.2 profile in PEEK_LOCK. A peek never locks anything: the receive link gets no credit
        before a receive [source]. Session receivers wait ``SESSION_ACCEPT_WAIT_SECONDS`` for a session
        (``idle_timeout_seconds`` is hidden, and so ignored, in peek mode)."""
        if self._client is None:
            self._client = self._connector.receive_client()
        kwargs = _receiver_profile(
            self._connector,
            ServiceBusReceiveMode.PEEK_LOCK,
            self.config.advanced.prefetch_count,
            session_wait=SESSION_ACCEPT_WAIT_SECONDS if session else None,
        )
        return _enter(self._entity.open_receiver(self._client, **kwargs))

    def _recover(self, error: Exception) -> None:
        self._close()
        self._tracker.failure(error)
        self._stats.recoveries = self._tracker.count
        _log_recovery(error, self._tracker, self._connector.secrets)

    def _close_receiver(self) -> None:
        receiver, self._receiver = self._receiver, None
        if receiver is not None:
            _close_quietly(receiver)

    def _close(self) -> None:
        handlers = [self._receiver, *self._held, self._client]
        self._receiver, self._held, self._client = None, [], None
        for handler in handlers:
            if handler is not None:
                _close_quietly(handler)
