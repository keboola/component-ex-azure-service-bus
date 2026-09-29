"""C4 peek pager (Task 15, spec §6.7): export an entity by peeking -- nothing is locked, settled or
deleted.

``PeekPager`` pages incremental fetch from the stored cursor and full fetch from the start (cursor
mode on partitioned entities, one held receiver per session on session entities). Expired messages
and scheduled messages pending activation are skipped and counted; a scheduled message is exported
once active. The connection-recovery bookkeeping (``RecoveryTracker``), the stop reasons and the
receiver helpers are shared with the destructive receive loop in ``receiver.py``. Every SDK call
runs on the main thread; message bodies are never logged.
"""

import logging
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any, NamedTuple

from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode
from azure.servicebus.exceptions import OperationTimeoutError, SessionCannotBeLockedError
from keboola.component.exceptions import UserException

from client import ServiceBusConnector
from configuration import Configuration, FetchMode
from entity import EntityInfo, EntityRef
from receiver import (
    SESSION_ACCEPT_WAIT_SECONDS,
    RecoveryTracker,
    StopReason,
    after_watermark,
    close_quietly,
    enter_receiver,
    is_pending_activation,
    log_recovery,
    receiver_profile,
    renew_session_if_needed,
)
from settlement import BatchProcessor
from state import PeekCursor
from stats import RunStats

logger = logging.getLogger(__name__)

PEEK_PAGE_SIZE = 250  # records per peek call: the broker's cap (read at call time; tests monkeypatch it)

_PARTITIONED_INCREMENTAL = "Incremental Fetch cannot page a partitioned entity reliably; use Full Fetch."


class _PageResult(NamedTuple):
    """What exporting one page ended with: the stop (if any) and the sequence numbers that asked for
    a fresh-connection retry."""

    stop: StopReason | None
    retried: list[int]


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
        self._deadline = self._monotonic() + self.config.advanced.max_duration_seconds
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
                result = self._export(receiver, page)
                if result.retried:
                    # Explicit paging: re-peek from the first unreadable body on a fresh connection; the
                    # page's processed messages are dropped from the re-peeked page (no duplicate rows).
                    self._close()
                    self._tracker.unreadable_retry()
                    self._stats.unreadable_recycles += 1
                    next_seq = min(result.retried)
                    continue
                if result.stop is not None:
                    return result.stop
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
            renew_session_if_needed(receiver, self._clock())
            page = receiver.peek_messages(PEEK_PAGE_SIZE, sequence_number=next_seq)
            if not page:
                return None
            stop = self._export(receiver, page).stop
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

    def _export(self, receiver: Any, page: Sequence[Any]) -> _PageResult:
        """Write one page: drop what this run already processed, skip the scheduled messages pending
        activation and the expired ones, stop before the first message at or after T0 or at
        ``max_messages``."""
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
            if limits.stop_at_job_start and after_watermark(message, self._t0):
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
        return _PageResult(stop, retried)

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
        kwargs = receiver_profile(
            self._connector,
            ServiceBusReceiveMode.PEEK_LOCK,
            self.config.advanced.prefetch_count,
            session_wait=SESSION_ACCEPT_WAIT_SECONDS if session else None,
        )
        return enter_receiver(self._entity.open_receiver(self._client, **kwargs))

    def _recover(self, error: Exception) -> None:
        self._close()
        self._tracker.failure(error)
        self._stats.recoveries = self._tracker.count
        log_recovery(error, self._tracker, self._connector.secrets)

    def _close_receiver(self) -> None:
        receiver, self._receiver = self._receiver, None
        if receiver is not None:
            close_quietly(receiver)

    def _close(self) -> None:
        handlers = [self._receiver, *self._held, self._client]
        self._receiver, self._held, self._client = None, [], None
        for handler in handlers:
            if handler is not None:
                close_quietly(handler)
