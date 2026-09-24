"""Per-mode settlers, the unreadable-body policy and the write-then-settle batch processor
(Task 11, spec §6.5, §6.6, §6.9).

``BatchProcessor.process`` is the one pipeline the receive loop, the orphan recovery and the peek
pager share: every message of a batch is decoded and mapped to a row; unreadable bodies go to the
``UnreadableHandler`` (retry on a fresh connection, dead-letter, leave, skip or fail); the readable
rows are written to the sink in one call **before** anything is settled; then each readable message
is settled by the mode's ``Settler`` (lock renewed first when it is about to lapse). Only after the
batch is written and settled may the processor raise its own ``UserException`` -- the ``fail``
policy, the flatten column cap, the unreadable abort share ("write first, fail after", §6.6).

Settlement failures caused by a lost lock or an already-settled message are counted, never fatal
(J5): the message redelivers and the primary key deduplicates its row. Message bodies are never
logged -- only sequence number and message id.
"""

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol

from azure.servicebus.exceptions import MessageAlreadySettled, MessageLockLostError
from keboola.component.exceptions import UserException

from body import MAX_FLATTEN_COLUMNS, BodyDecodeError, BodyTooLargeError, FlattenRegistry, NotJsonError, encode_body
from columns import message_metadata
from configuration import BodyFormat, SettlementMode, UnreadablePolicy
from entity import EntityRef
from output import OutputRow, RowSink
from state import PendingSetBuilder
from stats import RunStats

logger = logging.getLogger(__name__)

LOCK_RENEW_MARGIN = timedelta(seconds=10)
UNREADABLE_ABORT_SHARE = 0.10
UNREADABLE_ABORT_MIN = 10
MAX_UNREADABLE_RECYCLES = 50

UNREADABLE_ERRORS = (BodyDecodeError, NotJsonError, BodyTooLargeError)

# C1 / C2 hold a PEEK_LOCK lock on every received message; C3 (already deleted) and C4 (peeked) do not.
_LOCK_HOLDING_MODES = frozenset({SettlementMode.COMPLETE, SettlementMode.DEFER_COMMIT})


class Settler(Protocol):
    def settle(self, receiver: Any, message: Any, body_bytes: int) -> None: ...


class CompleteSettler:
    """C1: ``complete_message``. Arms ``write_always`` exactly once, immediately before the first
    ``complete_message`` of the run -- the first point where a message is deleted (spec §6.9)."""

    def __init__(self, stats: RunStats, arm: Callable[[], None]) -> None:
        self._stats = stats
        self._arm = arm
        self._armed = False

    def settle(self, receiver: Any, message: Any, body_bytes: int) -> None:
        if not self._armed:
            self._arm()
            self._armed = True
        receiver.complete_message(message)
        self._stats.completed += 1


class DeferSettler:
    """C2: ``defer_message``, then -- only once the call returned -- the message joins the pending
    set that the next run commits (H2 / H5). Never arms ``write_always``: nothing is deleted."""

    def __init__(self, pending: PendingSetBuilder, entity: EntityRef, stats: RunStats) -> None:
        self._pending = pending
        self._entity = entity
        self._stats = stats

    def settle(self, receiver: Any, message: Any, body_bytes: int) -> None:
        receiver.defer_message(message)
        self._pending.add(self._entity, message.sequence_number, message.session_id, body_bytes)
        self._stats.deferred += 1


class NoopSettler:
    """C3 (already deleted on receive -- counted as such) and C4 (peeked -- nothing to settle).
    Never arms: C3 arms in the receive loop as soon as a receive first returns messages."""

    def __init__(self, stats: RunStats, count_as_deleted: bool) -> None:
        self._stats = stats
        self._count_as_deleted = count_as_deleted

    def settle(self, receiver: Any, message: Any, body_bytes: int) -> None:
        if self._count_as_deleted:
            self._stats.deleted_on_receive += 1


def make_settler(
    mode: SettlementMode,
    *,
    pending: PendingSetBuilder | None,
    entity: EntityRef,
    stats: RunStats,
    arm: Callable[[], None] = lambda: None,
) -> Settler:
    if mode is SettlementMode.COMPLETE:
        return CompleteSettler(stats, arm)
    if mode is SettlementMode.DEFER_COMMIT:
        if pending is None:
            raise ValueError("defer_commit settlement requires a PendingSetBuilder")
        return DeferSettler(pending, entity, stats)
    return NoopSettler(stats, count_as_deleted=mode is SettlementMode.RECEIVE_AND_DELETE)


def safe_settle(action: Callable[[], None], stats: RunStats) -> bool:
    """Run one settle / renew ``action``. A lost lock or an already-settled message is counted and
    reported, never fatal (J5); every other exception -- a connection failure above all --
    propagates so the caller's recycle logic sees it."""
    try:
        action()
    except (MessageLockLostError, MessageAlreadySettled) as error:
        stats.settlement_failures += 1
        stats.warn(
            "settlement_failures",
            f"A message could not be settled ({type(error).__name__}): its lock was lost or it was already "
            "settled. Such messages redeliver and the primary key deduplicates their rows; the run summary "
            "counts every occurrence (settlement_failures).",
        )
        return False
    return True


def renew_if_needed(receiver: Any, message: Any, now: datetime) -> None:
    """Renew a PEEK_LOCK message's lock from the main thread when it lapses within
    ``LOCK_RENEW_MARGIN`` (D10). Messages without a message lock (``locked_until_utc`` is ``None``:
    session receivers, RECEIVE_AND_DELETE, settled) are left alone."""
    locked_until = message.locked_until_utc
    if locked_until is not None and locked_until - now < LOCK_RENEW_MARGIN:
        receiver.renew_message_lock(message)


class UnreadableAction(StrEnum):
    RETRY = "retry"  # recycle the connection after the batch; the message comes back
    DISPOSED = "disposed"  # dead-lettered or left
    SKIPPED = "skipped"  # C3 / C4: no row, nothing to settle
    FAILED = "failed"  # policy ``fail``: recorded, raised after the batch


def _reason_of(error: Exception) -> str:
    if isinstance(error, NotJsonError):
        return "NotJson"
    if isinstance(error, BodyTooLargeError):
        return "BodyTooLarge"
    return "UnreadableBody"


def _who(message: Any) -> str:
    return f"Message {message.sequence_number} (message id {getattr(message, 'message_id', None)})"


class UnreadableHandler:
    """The ``body.unreadable_body`` policy (spec §6.6). ``handle`` never raises a policy failure:
    ``fail`` records the first ``UserException`` of the batch for ``raise_pending``, which the
    processor calls only after the batch's readable rows were written and settled.

    ``policy`` and ``retries_enabled`` are public: the peek pager turns retries off for cursor-mode
    and per-session peeks, where a first failure is final (they cannot resume at a sequence number).
    """

    def __init__(self, *, policy: UnreadablePolicy, mode: SettlementMode, is_sub_queue: bool, stats: RunStats) -> None:
        self.policy = policy
        self.retries_enabled = True
        self._mode = mode
        self._is_sub_queue = is_sub_queue
        self._stats = stats
        self._first_failure: dict[int, int] = {}  # sequence number -> generation of its first decode failure
        self._left: set[int] = set()
        self._pending: UserException | None = None
        self._retry_logged = False

    def handle(self, receiver: Any, message: Any, error: Exception, generation: int) -> UnreadableAction:
        seq = message.sequence_number
        if seq in self._left:
            return UnreadableAction.DISPOSED  # left once already: no new retry, counted once
        reason = _reason_of(error)
        if isinstance(error, BodyDecodeError) and self._should_retry(message, generation):
            return self._retry(receiver, message, generation, reason)
        return self._dispose(receiver, message, error, reason)

    def _should_retry(self, message: Any, generation: int) -> bool:
        first = self._first_failure.get(message.sequence_number)
        if first is not None and first != generation:
            return False  # it failed again on a fresh connection: final
        if self._mode is SettlementMode.RECEIVE_AND_DELETE or not self.retries_enabled:
            return False  # already deleted / a peek that cannot resume: the first failure is final
        if self._stats.unreadable_recycles >= MAX_UNREADABLE_RECYCLES:
            self._stats.warn(
                "unreadable_retry_budget",
                f"Unreadable-body retry budget exhausted ({MAX_UNREADABLE_RECYCLES} fresh-connection retries per "
                f"run): {_who(message)} and every later unreadable body go straight to the "
                f"'{self.policy.value}' policy.",
            )
            return False
        return True

    def _retry(self, receiver: Any, message: Any, generation: int, reason: str) -> UnreadableAction:
        self._first_failure.setdefault(message.sequence_number, generation)
        if self._mode in _LOCK_HOLDING_MODES:
            safe_settle(lambda: receiver.abandon_message(message), self._stats)
        self._stats.unreadable[f"{reason}:retry"] += 1
        if not self._retry_logged:
            self._retry_logged = True
            logger.info("%s: the body could not be read; retrying it on a fresh connection.", _who(message))
        return UnreadableAction.RETRY

    def _dispose(self, receiver: Any, message: Any, error: Exception, reason: str) -> UnreadableAction:
        who = _who(message)
        if self.policy is UnreadablePolicy.FAIL:
            self._stats.unreadable[f"{reason}:failed"] += 1
            if self._pending is None:
                self._pending = UserException(
                    f"{who} has an unreadable body ({reason}) and the unreadable_body policy is 'fail'."
                )
            return UnreadableAction.FAILED
        if self._mode not in _LOCK_HOLDING_MODES:
            detail = (
                "receive_and_delete already deleted it, so its row is lost"
                if self._mode is SettlementMode.RECEIVE_AND_DELETE
                else "no row is written; the message stays on the entity"
            )
            self._count(reason, "skipped", f"{who} has an unreadable body ({reason}) and was skipped: {detail}.")
            return UnreadableAction.SKIPPED
        if self.policy is UnreadablePolicy.DEAD_LETTER and not self._is_sub_queue:
            description = type(error.__cause__ or error).__name__
            safe_settle(
                lambda: receiver.dead_letter_message(message, reason=reason, error_description=description),
                self._stats,
            )
            self._count(reason, "dead_lettered", f"{who} has an unreadable body ({reason}) and was dead-lettered.")
            return UnreadableAction.DISPOSED
        if self.policy is UnreadablePolicy.DEAD_LETTER:
            self._stats.warn(
                "dead_letter_on_sub_queue",
                f"{who} has an unreadable body ({reason}); a message on a dead-letter sub-queue cannot be "
                "dead-lettered, so the 'dead_letter' policy is applied as 'leave' on this entity.",
            )
        self._left.add(message.sequence_number)
        self._count(
            reason,
            "left",
            f"{who} has an unreadable body ({reason}) and was left unsettled; its lock lapses and it redelivers.",
        )
        return UnreadableAction.DISPOSED

    def _count(self, reason: str, disposition: str, message: str) -> None:
        self._stats.unreadable[f"{reason}:{disposition}"] += 1
        self._stats.warn(
            f"unreadable_{reason}_{disposition}",
            f"{message} Further such messages are counted in the run summary.",
        )

    def raise_pending(self) -> None:
        """Raise (and clear) the first ``fail``-policy ``UserException`` recorded in the batch."""
        pending, self._pending = self._pending, None
        if pending is not None:
            raise pending

    def check_abort_share(self) -> None:
        total, received = self._stats.unreadable_total(), self._stats.received
        if total > UNREADABLE_ABORT_SHARE * received and total >= UNREADABLE_ABORT_MIN:
            raise UserException(
                f"{total} of the {received} messages received in this run had unreadable bodies (more than "
                f"{UNREADABLE_ABORT_SHARE:.0%} and at least {UNREADABLE_ABORT_MIN}), which points to a systemic "
                "problem such as a wrong body format or a faulty producer. The readable messages were written; "
                "check the body format setting of this row."
            )


@dataclass
class BatchResult:
    """``progressed``: rows were written or at least one unreadable body was finally disposed of
    (the receive loop's no-progress guard counts both, spec §6.5). ``retried`` lists the sequence
    numbers that asked for a fresh-connection retry (the peek pager re-peeks from the first one)."""

    written: int
    needs_recycle: bool
    progressed: bool
    retried: list[int] = field(default_factory=list)


class BatchProcessor:
    """Write-then-settle for one batch (spec §6.5, §6.6). ``unreadable`` is public: the loops and
    the pager adjust its ``policy`` / ``retries_enabled``."""

    def __init__(
        self,
        *,
        entity: EntityRef,
        mode: SettlementMode,
        body_format: BodyFormat,
        sink: RowSink,
        registry: FlattenRegistry | None,
        settler: Settler,
        unreadable: UnreadableHandler,
        stats: RunStats,
        clock: Callable[[], datetime],
    ) -> None:
        if body_format is BodyFormat.JSON_FLATTEN and registry is None:
            raise ValueError("the json_flatten body format requires a flatten registry")
        self._entity = entity
        self._mode = mode
        self._body_format = body_format
        self._sink = sink
        self._registry = registry
        self._settler = settler
        self.unreadable = unreadable
        self._stats = stats
        self._clock = clock

    def process(self, receiver: Any, messages: Sequence[Any], generation: int) -> BatchResult:
        extracted_at = self._clock()
        rows: list[OutputRow] = []
        readable: list[tuple[Any, int]] = []
        retried: list[int] = []
        disposed = False
        for message in messages:
            self._stats.received += 1
            self._stats.note_delivery_count(message.delivery_count or 0)
            try:
                row, body_bytes = self._to_row(message, extracted_at)
            except UNREADABLE_ERRORS as error:
                action = self.unreadable.handle(receiver, message, error, generation)
                if action is UnreadableAction.RETRY:
                    retried.append(message.sequence_number)
                else:
                    disposed = True
                continue
            rows.append(row)
            readable.append((message, body_bytes))

        if rows:
            self._sink.write_rows(rows)  # one write per batch, before any settle
            self._stats.written += len(rows)
        for message, body_bytes in readable:
            self._settle(receiver, message, body_bytes)

        # Write first, fail after: nothing below runs before the readable rows are written and settled.
        self.unreadable.raise_pending()
        if self._registry is not None and self._registry.overflowed:
            raise UserException(
                f"The message bodies produced more than {MAX_FLATTEN_COLUMNS} distinct JSON keys; this batch was "
                "written, but use the text body format for this entity."
            )
        self.unreadable.check_abort_share()
        return BatchResult(
            written=len(rows), needs_recycle=bool(retried), progressed=bool(rows) or disposed, retried=retried
        )

    def _to_row(self, message: Any, extracted_at: datetime) -> tuple[OutputRow, int]:
        """The message's output row and raw body size; raises one of ``UNREADABLE_ERRORS``."""
        encoded = encode_body(message, self._body_format)
        metadata = message_metadata(message, entity=self._entity, settlement_mode=self._mode, extracted_at=extracted_at)
        if encoded.fields is None:
            return OutputRow(metadata=metadata, body=encoded.cell), encoded.size_bytes
        registry = self._registry
        assert registry is not None, "json_flatten requires a registry (checked in __init__)"
        for path in encoded.fields:
            registry.register(path)  # over the cap: a provisional name and `overflowed`; the row is still written
        registry.split(encoded.fields)  # an over-limit body_unmapped cell raises BodyTooLargeError before any write
        return OutputRow(metadata=metadata, fields=encoded.fields), encoded.size_bytes

    def _settle(self, receiver: Any, message: Any, body_bytes: int) -> None:
        def action() -> None:
            if self._mode in _LOCK_HOLDING_MODES:
                renew_if_needed(receiver, message, self._clock())
            self._settler.settle(receiver, message, body_bytes)

        safe_settle(action, self._stats)
