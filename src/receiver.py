"""Receive loop (C1 / C2 / C3) and the connection-recovery bookkeeping and receiver helpers it shares
with the C4 peek pager in ``peek.py`` (Task 14, spec §6.2, §6.5, §6.6, §6.11).

``ReceiveLoop`` drives one destructive run: batches of ``receive_messages`` go through the shared
``BatchProcessor`` (write first, then settle) until a stop rule fires; then the stop drain empties the
local receive buffer -- C1 / C2 abandon the drained messages so they are available again at once, C3
writes them because RECEIVE_AND_DELETE has already deleted them on the broker. Session entities loop
``NEXT_AVAILABLE_SESSION`` receivers, one session at a time.

An empty receive is not trusted as "drained" (Phase 8, live: a 1M-message C1 run stopped idle after
77k messages with 927k still active -- a throttled Standard-tier namespace or a stalled link-credit
refill hands out nothing without an error). The loop peeks past the highest processed sequence number
and stops idle only when nothing receivable is left; otherwise it reopens the connection, backs off
and continues, up to ``MAX_EMPTY_RECEIVE_RETRIES`` consecutive times. A run that stops while
messages remain says so in a WARNING with the remaining count.

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

from client import ServiceBusConnector, is_credential_failure, is_session_mismatch, redact_secrets, to_user_exception
from configuration import Configuration, SettlementMode
from entity import EntityInfo, EntityRef, load_entity_info
from settlement import LOCK_RENEW_MARGIN, BatchProcessor, BatchResult, safe_settle
from state import STATE_BUDGET_BYTES
from stats import RunStats

logger = logging.getLogger(__name__)

MAX_RECOVERIES = 5
# The accept wait of every session receiver outside the destructive receive loop (C4 peek,
# testConnection, previewMessages): idle_timeout_seconds is hidden -- and so ignored -- there.
SESSION_ACCEPT_WAIT_SECONDS = 5
DRAIN_WAIT_SECONDS = 1
CATCH_UP_POLL_SECONDS = 1
# An empty receive while receivable messages remain (module doc): reopen and back off, this many times in a
# row at most; the drain check peeks one page (the broker's cap) past the highest processed sequence number.
MAX_EMPTY_RECEIVE_RETRIES = 5
EMPTY_RECEIVE_BACKOFF_SECONDS = (2, 4, 8, 16, 30)
DRAIN_CHECK_PEEK_COUNT = 250

# Errors a fresh connection cannot fix (spec §6.11); plus the session mismatch (``is_session_mismatch``)
# and rejected service-principal credentials (``is_credential_failure``, a plain ServiceBusError).
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
    RECEIVE_STALLED = "receive_stalled"


# Stops that can leave receivable messages behind: the run warns with the remaining count (module doc).
_PARTIAL_STOPS = frozenset({StopReason.MAX_MESSAGES, StopReason.MAX_DURATION, StopReason.RECEIVE_STALLED})


def is_fatal(error: BaseException) -> bool:
    """A configuration, auth or entity error (or the session mismatch): mapped, never recycled."""
    return isinstance(error, FATAL_ERRORS) or is_session_mismatch(error) or is_credential_failure(error)


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


def receiver_profile(
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


def enter_receiver(receiver: Any) -> Any:
    """Open (attach) ``receiver`` so entity, auth and session errors surface here, inside the caller's
    recovery ``try``; a receiver that fails to open is closed before the error propagates."""
    try:
        return receiver.__enter__()
    except Exception:
        close_quietly(receiver)
        raise


def close_quietly(handler: Any) -> None:
    """Close a receiver or client; a broken link may fail to close, which must never mask the error
    that ended the run (the next open starts afresh anyway)."""
    try:
        handler.close()
    except Exception as error:  # noqa: BLE001 -- best-effort cleanup
        logger.debug("Closing a Service Bus handler failed: %s", type(error).__name__)


def renew_session_if_needed(receiver: Any, now: datetime) -> None:
    """Renew the session lock from the main thread when it lapses within ``LOCK_RENEW_MARGIN`` (D10);
    session receivers hold no message locks (``locked_until_utc`` is ``None`` on their messages)."""
    session = receiver.session
    locked_until = session.locked_until_utc if session is not None else None
    if locked_until is not None and locked_until - now < LOCK_RENEW_MARGIN:
        session.renew_lock()


def after_watermark(message: Any, t0: datetime) -> bool:
    """Enqueued at or after T0. ``SCHEDULED``-state messages never count -- harmless either way: a
    pending one is skipped in C4 (``is_pending_activation``), and a message activated from a schedule
    can still report ``SCHEDULED`` on 7.14.3 [live, Phase 7], so ignoring it at most delays a stop."""
    enqueued = message.enqueued_time_utc
    return message.state != ServiceBusMessageState.SCHEDULED and enqueued is not None and enqueued >= t0


def is_pending_activation(message: Any) -> bool:
    """A scheduled message that has not activated yet: C4 skips it and exports its activated copy, and
    the receive loop's drain check ignores it.

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


def log_recovery(error: BaseException, tracker: RecoveryTracker, secrets: Iterable[str]) -> None:
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
        self._highest_processed: int | None = None  # the drain check peeks past it on non-partitioned entities
        self._empty_retries = 0  # consecutive empty receives while receivable messages remained

    def run(self) -> StopReason:
        self._deadline = self._monotonic() + self._config.advanced.max_duration_seconds
        try:
            reason = self._consume(catch_up_until=None)
            if self._catch_up_due(reason):
                reason = self._catch_up()
            self._warn_if_messages_remain(reason)
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
            renew_session_if_needed(receiver, self._clock())
        batch = self._receive(receiver, self._batch_count(), self._wait)
        if not batch:
            return self._on_empty(catch_up_until)
        self._empty_retries = 0
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
        kwargs = receiver_profile(
            self._connector,
            _RECEIVE_MODES[self._mode],
            self._config.advanced.prefetch_count,
            session_wait=self._wait if session else None,
        )
        receiver = enter_receiver(self._entity.open_receiver(client, **kwargs))
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
        # Not the stop drain's messages: C1 / C2 abandon those, so they are available again below this mark.
        self._highest_processed = max(self._highest_processed or 0, *(message.sequence_number for message in batch))
        return result

    def _is_watermark_batch(self, batch: Sequence[Any]) -> bool:
        considered = [message for message in batch if message.state != ServiceBusMessageState.SCHEDULED]
        return bool(considered) and all(after_watermark(message, self._t0) for message in considered)

    def _on_empty(self, catch_up_until: float | None) -> StopReason | None:
        if self._sessions:
            self._finish_session()  # this session is drained: on to the next one
            return None
        if catch_up_until is not None:
            self._sleep(CATCH_UP_POLL_SECONDS)
            return None
        remaining = len(self._receivable_messages())
        if not remaining:
            return StopReason.IDLE  # verified: nothing a receive would hand out is left
        return self._retry_empty_receive(remaining)

    def _retry_empty_receive(self, remaining: int) -> StopReason | None:
        """An empty receive although ``remaining`` receivable messages were peeked: reopen the connection
        (a fresh link and connection clear a stalled credit refill) after a backoff that lets a throttled
        namespace recover, at most ``MAX_EMPTY_RECEIVE_RETRIES`` times in a row; ``max_duration`` still
        bounds the run (the next step's stop check)."""
        self._empty_retries += 1
        self._stats.empty_receive_retries += 1
        if self._empty_retries > MAX_EMPTY_RECEIVE_RETRIES:
            return StopReason.RECEIVE_STALLED
        backoff = EMPTY_RECEIVE_BACKOFF_SECONDS[min(self._empty_retries, len(EMPTY_RECEIVE_BACKOFF_SECONDS)) - 1]
        logger.info(
            "A receive returned no messages although %s receivable message(s) are still in '%s' (a throttled "
            "namespace or a stalled link); reopening the connection in %d s (retry %d of %d).",
            f"at least {remaining}" if remaining >= DRAIN_CHECK_PEEK_COUNT else remaining,
            self._entity.path,
            backoff,
            self._empty_retries,
            MAX_EMPTY_RECEIVE_RETRIES,
        )
        # No stop drain: the receive just returned empty, so the local buffer is empty, and with no
        # keep-alive thread nothing arrives before the next call [source: pyamqp works only inside calls].
        self._close()
        self._sleep(min(backoff, max(0.0, self._deadline - self._monotonic())))
        return None

    def _receivable_messages(self) -> list[Any]:
        """One peek page of what a receive would still hand out: past the highest processed sequence
        number (from the start in cursor mode on a fresh receiver on partitioned entities, whose
        sequence numbers are per partition), keeping messages that are neither deferred (C2's own and
        foreign deferrals stay in the entity), nor pending activation, nor expired, nor -- with
        ``stop_at_job_start`` -- enqueued at or after T0. Peeking locks nothing."""
        if self._client is None:
            self._client = self._connector.receive_client()
        now = self._clock()
        if self._info.is_partitioned or self._receiver is None:
            kwargs = receiver_profile(self._connector, ServiceBusReceiveMode.PEEK_LOCK, 1, session_wait=None)
            peeker = enter_receiver(self._entity.open_receiver(self._client, **kwargs))
            start = 0 if self._info.is_partitioned else (self._highest_processed or 0) + 1
            try:
                page = peeker.peek_messages(DRAIN_CHECK_PEEK_COUNT, sequence_number=start)
            finally:
                close_quietly(peeker)
        else:
            start = (self._highest_processed or 0) + 1
            page = self._receiver.peek_messages(DRAIN_CHECK_PEEK_COUNT, sequence_number=start)
        watermark = self._config.limits.stop_at_job_start
        return [
            message
            for message in page
            if message.state != ServiceBusMessageState.DEFERRED
            and not is_pending_activation(message)
            and (message.expires_at_utc is None or message.expires_at_utc >= now)
            and not (watermark and after_watermark(message, self._t0))
        ]

    def _warn_if_messages_remain(self, reason: StopReason) -> None:
        """A partial extraction must never look like a complete drain: after a stop that can leave
        messages behind (and, on session entities without the job-start stop, after the session loop
        ran out of sessions) a WARNING names what is left -- peeked on plain entities, from the
        management counts on session entities (a peek there needs a session). Best effort: a failed
        count never changes the run's outcome."""
        try:
            if self._sessions:
                text = self._remaining_by_count(reason)
            elif reason in _PARTIAL_STOPS:
                remaining = len(self._receivable_messages())
                text = None
                if remaining:
                    shown = f"at least {remaining}" if remaining >= DRAIN_CHECK_PEEK_COUNT else str(remaining)
                    text = f"{shown} receivable message(s) are still in '{self._entity.path}'"
            else:
                return
        except Exception as error:  # noqa: BLE001 -- the count is informative only
            logger.debug("Could not count the messages left: %s", _describe(error, self._connector.secrets))
            return
        if text is None:
            return
        hints = {
            StopReason.MAX_MESSAGES: "Max Messages was reached",
            StopReason.MAX_DURATION: "Max Duration was reached (Advanced options)",
            StopReason.RECEIVE_STALLED: (
                f"Service Bus returned no messages to {MAX_EMPTY_RECEIVE_RETRIES + 1} receives in a row although "
                "messages were available -- typically a throttled namespace (see the throttling warning, if any)"
            ),
            StopReason.NO_MORE_SESSIONS: (
                "no further session was handed out -- they may be locked by another consumer, or the namespace "
                "was throttled"
            ),
        }
        self._stats.warn(
            "messages_remaining",
            f"The run stopped before the entity was drained: {text}. Stop reason: {reason.value} "
            f"({hints.get(reason, 'see the stop reason')}). The next run continues with them.",
        )

    def _remaining_by_count(self, reason: StopReason) -> str | None:
        """Session entities: the management count of active messages (``None`` without management
        access). Skipped in C2, whose own deferrals count as active, and after the session loop ran out
        of sessions with ``stop_at_job_start`` on, when newer messages are expected to remain."""
        if self._mode is SettlementMode.DEFER_COMMIT:
            return None
        if reason is StopReason.NO_MORE_SESSIONS and self._config.limits.stop_at_job_start:
            return None
        if reason not in _PARTIAL_STOPS and reason is not StopReason.NO_MORE_SESSIONS:
            return None
        counts = load_entity_info(self._connector, self._entity).counts
        if not counts or not counts["active"]:
            return None
        return f"Service Bus reports {counts['active']} active message(s) in '{self._entity.path}'"

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
                renew_session_if_needed(receiver, self._clock())
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
        log_recovery(error, self._tracker, self._connector.secrets)

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
            close_quietly(receiver)

    def _close(self, *, interrupted: bool = False) -> None:
        """Close the receiver and its client. ``interrupted``: the open session was not finished, so
        being handed it again continues it rather than counting as a revisit."""
        if interrupted and self._session_id is not None:
            self._seen_sessions.discard(self._session_id)
        receiver, client = self._receiver, self._client
        self._receiver = self._client = self._session_id = None
        for handler in (receiver, client):
            if handler is not None:
                close_quietly(handler)
