"""Deferred-message commit (H5), orphan scan (H3) and foreign-deferral probe (Tasks 12-13, spec §6.3, §6.4).

``PendingCommitter`` deletes the messages the previous defer-commit run deferred -- this row's
stored pending set -- at the start of every destructive mode, before anything is received. Each
stored (entity, session, partition) group is read back by sequence number on a RECEIVE_AND_DELETE
receiver of the dedicated commit client (``retry_total=0``), in chunks of at most 250 sequence
numbers and 16 MiB of bodies, generated lazily from the stored ranges (a range can cover millions
of sequence numbers); the returned messages are discarded (the run that deferred them
already imported their rows). The commit client has no SDK retries, so the transient errors are
retried here (2 / 4 / 8 s); a sequence number the broker no longer has is bisected out and counted
as already gone. Failing here is safe: nothing has been received yet and the input state is
untouched, so the next run simply retries.

``OrphanScanner`` (C2 only, after H5) peeks the entity for ``DEFERRED`` messages no state owns --
the deferrals of a C2 run whose state was lost -- and recovers them through the batch processor,
which writes their rows and re-defers them into the new pending set. ``ForeignDeferralProbe``
(C1 / C3) only reports such messages. Message bodies are never logged.
"""

import logging
import time
from collections import defaultdict
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from itertools import batched
from typing import Any, NamedTuple

from azure.servicebus import ServiceBusClient, ServiceBusMessageState, ServiceBusReceiveMode
from azure.servicebus.exceptions import (
    MessageNotFoundError,
    MessagingEntityNotFoundError,
    OperationTimeoutError,
    ServiceBusAuthenticationError,
    ServiceBusAuthorizationError,
    ServiceBusCommunicationError,
    ServiceBusConnectionError,
    ServiceBusError,
    ServiceBusServerBusyError,
    SessionCannotBeLockedError,
)
from keboola.component.exceptions import UserException

from client import ServiceBusConnector, redact_secrets, to_user_exception
from entity import EntityInfo, EntityRef, partition_of
from settlement import BatchProcessor
from state import PendingEntity, PendingGroup
from stats import RunStats

logger = logging.getLogger(__name__)

COMMIT_MAX_COUNT = 250  # sequence numbers per deferred-receive call (broker limit [live])
COMMIT_MAX_BYTES = 16 * 1024 * 1024  # bodies per deferred-receive call
TRANSIENT_BACKOFF_SECONDS = (2, 4, 8)
TRANSIENT_ERRORS = (
    ServiceBusServerBusyError,
    ServiceBusConnectionError,
    ServiceBusCommunicationError,
    OperationTimeoutError,
)
DEFERRED_RECEIVE_TIMEOUT_SECONDS = 60
ORPHAN_PAGE_SIZE = 250
ORPHAN_PAGE_CAP = 20
ORPHAN_GUARD_LISTED = 20  # guarded sequence numbers named in the WARNING

_SESSION_LOCKED: tuple[type[Exception], ...] = (SessionCannotBeLockedError,)
# The stored entity is gone or no longer readable with these credentials (a missing queue via SAS
# reads as an authentication failure [live]).
_STALE_ERRORS = (MessagingEntityNotFoundError, ServiceBusAuthenticationError, ServiceBusAuthorizationError)


def commit_chunk_size(max_body_bytes: int) -> int:
    """Sequence numbers per deferred-receive call for a group whose largest body is
    ``max_body_bytes``: at most 250 and at most 16 MiB of bodies, but always at least one (a body
    above 16 MiB goes alone)."""
    return max(1, min(COMMIT_MAX_COUNT, COMMIT_MAX_BYTES // max(1, max_body_bytes)))


def with_transient_retry[T](
    fn: Callable[[], T], *, sleep: Callable[[float], None], extra: tuple[type[Exception], ...] = ()
) -> T:
    """Call ``fn``; on one of ``TRANSIENT_ERRORS`` (or ``extra``) sleep 2 / 4 / 8 s and call it again
    -- at most four calls -- then let the last error propagate."""
    retryable = TRANSIENT_ERRORS + extra
    for delay in TRANSIENT_BACKOFF_SECONDS:
        try:
            return fn()
        except retryable as error:
            logger.info("Service Bus reported %s; retrying in %s s.", type(error).__name__, delay)
            sleep(delay)
    return fn()


class DeferredReceive(NamedTuple):
    """The outcome of one deferred receive: the messages returned and the sequence numbers the
    broker no longer has."""

    received: list[Any]
    not_found: list[int]


def receive_deferred_bisect(
    receiver: Any,
    seqs: list[int],
    *,
    sleep: Callable[[float], None],
    extra: tuple[type[Exception], ...] = (),
) -> DeferredReceive:
    """One deferred receive of ``seqs`` (one partition, at most 250). A ``MessageNotFoundError``
    fails the whole call [live], so the call is split in halves until every missing sequence number
    fails alone; those are returned as not found. Every call goes through
    :func:`with_transient_retry` (``extra`` is passed on)."""
    if not seqs:
        return DeferredReceive([], [])
    call = partial(receiver.receive_deferred_messages, seqs, timeout=DEFERRED_RECEIVE_TIMEOUT_SECONDS)
    try:
        return DeferredReceive(list(with_transient_retry(call, sleep=sleep, extra=extra)), [])
    except MessageNotFoundError:
        if len(seqs) == 1:
            return DeferredReceive([], list(seqs))
    middle = len(seqs) // 2
    left = receive_deferred_bisect(receiver, seqs[:middle], sleep=sleep, extra=extra)
    right = receive_deferred_bisect(receiver, seqs[middle:], sleep=sleep, extra=extra)
    return DeferredReceive(left.received + right.received, left.not_found + right.not_found)


def _count(groups: list[PendingGroup]) -> int:
    return sum(group.count for group in groups)


@dataclass
class _GroupProgress:
    """How far the commit of one group got: every sequence number below ``next_seq`` is deleted
    (chunks go in ascending order), so ``group.tail(next_seq)`` is what is still pending."""

    next_seq: int = 0


def _receiver_profile(connector: ServiceBusConnector, receive_mode: ServiceBusReceiveMode) -> dict[str, Any]:
    """The §6.2 profile of every receiver outside the receive loop: one message of prefetch, no
    keep-alive thread, the row's client identifier."""
    return {
        "receive_mode": receive_mode,
        "prefetch_count": 1,
        "keep_alive": 0,
        "client_identifier": connector.client_identifier,
    }


class PendingCommitter:
    """H5: delete this row's stored pending set (spec §6.3). ``commit`` returns the groups carried
    forward into the new pending set -- those whose session another receiver holds."""

    def __init__(
        self,
        connector: ServiceBusConnector,
        *,
        configured: EntityRef,
        stats: RunStats,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        self._connector = connector
        self._configured = configured
        self._stats = stats
        self._sleep = sleep or time.sleep

    def commit(self, pending: list[PendingEntity]) -> list[PendingEntity]:
        work = [entity for entity in pending if any(group.ranges for group in entity.groups)]
        if not work:
            return []
        committed, already_gone = self._stats.committed, self._stats.already_gone
        carried: list[PendingEntity] = []
        client = self._connector.commit_client()  # created only when there is something to delete
        try:
            for pending_entity in work:
                groups = self._commit_entity(client, pending_entity)
                if groups:
                    carried.append(
                        PendingEntity(
                            entity=pending_entity.entity, groups=groups, deferred_at_utc=pending_entity.deferred_at_utc
                        )
                    )
        finally:
            client.close()
        logger.info(
            "Deleted %d message(s) deferred by the previous run; %d were already gone.",
            self._stats.committed - committed,
            self._stats.already_gone - already_gone,
        )
        return carried

    def _commit_entity(self, client: ServiceBusClient, pending_entity: PendingEntity) -> list[PendingGroup]:
        """Commit every group of one stored entity; returns its session-locked (carried) groups."""
        ref = pending_entity.entity_ref()
        groups = [group for group in pending_entity.groups if group.ranges]
        carried: list[PendingGroup] = []
        for index, group in enumerate(groups):
            progress = _GroupProgress()
            try:
                self._delete_group(client, ref, group, progress)
            except SessionCannotBeLockedError:
                left = group.tail(progress.next_seq)
                carried.append(left)
                self._stats.carried_forward += left.count
            except (*_STALE_ERRORS, UserException) as error:
                # UserException: `open_receiver`'s EntityPath mismatch -- a connection string scoped to
                # another entity can no longer read the stored one.
                if ref == self._configured:
                    if isinstance(error, UserException):
                        raise
                    raise to_user_exception(error, ref.path, self._connector.secrets) from error
                self._drop_stale(ref, group.tail(progress.next_seq).count + _count(groups[index + 1 :]))
                break  # the entity's remaining groups are just as unreadable
            except TRANSIENT_ERRORS as error:
                # Only what is still pending: earlier chunks / groups of this entity are already deleted.
                left = _count(carried) + group.tail(progress.next_seq).count + _count(groups[index + 1 :])
                raise UserException(
                    f"Could not delete the {left} message(s) deferred by the previous run on '{ref.path}': "
                    f"{redact_secrets(str(error), self._connector.secrets)}. Nothing was extracted in this run and "
                    "the state is unchanged; the next run retries."
                ) from error
            except ServiceBusError as error:
                raise to_user_exception(error, ref.path, self._connector.secrets) from error
        if carried:
            sessions = ", ".join(f"'{group.session_id}'" for group in carried)
            self._warn(
                "commit_session_locked",
                f"{_count(carried)} message(s) deferred by the previous run on '{ref.path}' could not be deleted "
                f"because another receiver holds their session ({sessions}); they stay DEFERRED and are carried "
                "forward to the next run.",
            )
        return carried

    def _delete_group(
        self, client: ServiceBusClient, ref: EntityRef, group: PendingGroup, progress: _GroupProgress
    ) -> None:
        """Delete ``group`` on one RECEIVE_AND_DELETE receiver in ascending chunks of
        ``commit_chunk_size``, generated from its ranges one chunk at a time. ``progress`` moves past a
        chunk only once its call returned, so when this raises ``group.tail(progress.next_seq)`` is
        exactly what is not yet deleted. Opening the receiver and its first call also retry a session
        held by another receiver (``SessionCannotBeLockedError``)."""
        receiver = with_transient_retry(
            partial(self._open_receiver, client, ref, group.session_id), sleep=self._sleep, extra=_SESSION_LOCKED
        )
        with closing(receiver):
            extra = _SESSION_LOCKED
            for chunk in batched(group.iter_sequence_numbers(), commit_chunk_size(group.max_body_bytes)):
                received, gone = receive_deferred_bisect(receiver, list(chunk), sleep=self._sleep, extra=extra)
                extra = ()
                self._stats.committed += len(received)
                self._stats.already_gone += len(gone)
                progress.next_seq = chunk[-1] + 1

    def _open_receiver(self, client: ServiceBusClient, ref: EntityRef, session_id: str | None) -> Any:
        """Open (attach) the group's receiver, so entity and session errors surface here. A sub-queue
        has no sessions: a dead-lettered session message keeps its ``session_id``, but the SDK refuses
        ``session_id`` together with ``sub_queue``."""
        kwargs = _receiver_profile(self._connector, ServiceBusReceiveMode.RECEIVE_AND_DELETE)
        if session_id is not None and not ref.is_sub_queue:
            kwargs["session_id"] = session_id
        receiver = ref.open_receiver(client, **kwargs)
        try:
            return receiver.__enter__()
        except Exception:
            receiver.close()
            raise

    def _drop_stale(self, ref: EntityRef, count: int) -> None:
        self._stats.dropped_stale += count
        self._warn(
            "commit_stale_entity",
            f"{count} message(s) deferred by an earlier run stay DEFERRED on '{ref.path}' because that entity is "
            "gone or no longer readable with these credentials; run a defer-commit row on that entity to recover "
            "them.",
        )

    def _warn(self, key: str, message: str) -> None:
        """``RunStats.warn`` logs a key once per run; a second entity under the same key is still
        named in the log, so no stranded deferral goes unmentioned."""
        if key in self._stats.warnings:
            logger.warning("%s", message)
        else:
            self._stats.warn(key, message)


# --- H3: orphan scan (C2) and foreign-deferral probe (C1 / C3) --------------------------------------


def k_stop(batch_size: int, prefetch_count: int) -> int:
    """The plain-entity orphan scan stops after this many consecutive qualifying messages (§6.4)."""
    # K derivation (spec §6.4 step 4): a currently-locked message peeks exactly like a never-delivered
    # one [live], so K must exceed the largest cluster of locked messages an earlier run can leave in
    # front of its later deferrals. One connection interruption leaves at most one batch plus the local
    # buffer locked: batch_size + prefetch_count + 1 (link credit included). The no-progress guard
    # (§6.5) fails a run interrupted twice without progress, and in C2 every written row is deferred,
    # which breaks a locked cluster -- so no cluster exceeds that bound and the factor 2 is margin.
    # 100 is the floor for small batches.
    return max(100, 2 * (batch_size + prefetch_count + 1))


def is_qualifying(message: Any, now: datetime) -> bool:
    """A message that counts toward the K stop: ``ACTIVE``, never delivered, not expired and never
    scheduled (§6.4 steps 3-4). An expired, not yet purged message still peeks ``ACTIVE`` with
    ``delivery_count`` 0 [live]."""
    expires_at = message.expires_at_utc
    return (
        message.state == ServiceBusMessageState.ACTIVE
        and (message.delivery_count or 0) == 0
        and (expires_at is None or expires_at > now)
        # An activated scheduled message keeps its scheduled_enqueue_time_utc [live, Phase 7] (and can
        # even report SCHEDULED on 7.14.3), so the property alone marks it as once-scheduled -- skipped.
        and message.scheduled_enqueue_time_utc is None
    )


def _peek_lock_receiver(connector: ServiceBusConnector, entity: EntityRef, client: ServiceBusClient) -> Any:
    """A PEEK_LOCK receiver for the entity: the peek receiver (a peek locks nothing) on the receive
    client, and the orphan-recovery receiver (a deferred receive locks the message) on the commit client."""
    return entity.open_receiver(client, **_receiver_profile(connector, ServiceBusReceiveMode.PEEK_LOCK))


class OrphanScanner:
    """H3 (C2 only, after H5): recover the deferrals no state owns (spec §6.4). Session entities
    cannot be scanned on 7.14.3 (a WARNING instead); partitioned entities and sub-queues get a
    best-effort capped full scan; plain entities stop after ``k_stop`` consecutive qualifying
    messages. Every scan is capped at ``ORPHAN_PAGE_CAP`` pages of ``ORPHAN_PAGE_SIZE``."""

    def __init__(
        self,
        connector: ServiceBusConnector,
        *,
        entity: EntityRef,
        info: EntityInfo,
        session_enabled: bool,
        batch_size: int,
        prefetch_count: int,
        processor: BatchProcessor,
        stats: RunStats,
        clock: Callable[[], datetime] | None = None,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        self._connector = connector
        self._entity = entity
        self._info = info
        self._session_enabled = session_enabled
        self._batch_size = batch_size
        self._prefetch_count = prefetch_count
        self._processor = processor
        self._stats = stats
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleep = sleep or time.sleep
        self._commit_client: ServiceBusClient | None = None  # created at the first orphan

    def scan(self) -> None:
        path = self._entity.path
        if self._session_enabled:
            self._stats.warn(
                "orphan_sessions",
                f"Orphan recovery is not available for session entities: deferrals of a failed defer-commit run on "
                f"'{path}' stay DEFERRED until recovered manually.",
            )
            return
        try:
            with (
                self._connector.receive_client() as client,
                _peek_lock_receiver(self._connector, self._entity, client) as peeker,
            ):
                finished = self._scan_pages(peeker)
        except ServiceBusError as error:
            raise to_user_exception(error, path, self._connector.secrets) from error
        finally:
            if self._commit_client is not None:
                self._commit_client.close()
                self._commit_client = None
        self._warn_guarded()
        if not finished:
            self._stats.warn(
                "orphan_scan_incomplete",
                f"Defer-commit orphan scan incomplete on '{path}': it stopped after {ORPHAN_PAGE_CAP} pages "
                f"({ORPHAN_PAGE_CAP * ORPHAN_PAGE_SIZE} messages) before the end of the entity, so deferrals of a "
                "failed run further back were not recovered in this run.",
            )

    def _scan_pages(self, peeker: Any) -> bool:
        """Peek page by page, recovering each page's orphans; True once the scan stopped (K rule or
        the end of the entity), False when it reached the page cap."""
        k = k_stop(self._batch_size, self._prefetch_count)
        consecutive = 0
        next_seq = 1
        for _ in range(ORPHAN_PAGE_CAP):
            # Cursor mode (sequence_number=0) on partitioned entities: explicit paging missed
            # partitions, cursor mode visited all of them [live].
            start = 0 if self._info.is_partitioned else next_seq
            self._best_effort()
            page = peeker.peek_messages(ORPHAN_PAGE_SIZE, sequence_number=start)
            if not page:
                return True
            now = self._clock()
            orphans: list[int] = []
            stopped = False
            for message in page:
                seq = message.sequence_number
                self._info.note_sequence_number(seq)  # a heuristic flip to partitioned: best-effort from here
                if message.state == ServiceBusMessageState.DEFERRED:
                    consecutive = 0
                    if (message.delivery_count or 0) >= self._info.orphan_guard_threshold:
                        self._stats.orphans_guarded.append(seq)
                    else:
                        orphans.append(seq)
                elif not self._best_effort() and is_qualifying(message, now):
                    consecutive += 1
                    if consecutive >= k:
                        stopped = True
                        break
            self._recover(orphans)
            if stopped:
                return True
            next_seq = page[-1].sequence_number + 1
        return False

    def _best_effort(self) -> bool:
        """Partitioned entities (known, or detected mid-scan) and sub-queues get a full capped scan
        without the K rule: it holds only per partition, and dead-lettered messages keep their original
        sequence numbers and any delivery count [live]. WARNING every run (once)."""
        if self._info.is_partitioned:
            kind = "partitioned entities"
        elif self._entity.is_sub_queue:
            kind = "dead-letter sub-queues"
        else:
            return False
        self._stats.warn(
            "orphan_best_effort",
            f"Orphan recovery is best-effort on {kind}: the scan of '{self._entity.path}' reads at most "
            f"{ORPHAN_PAGE_CAP * ORPHAN_PAGE_SIZE} messages without an early stop and can miss deferrals left by a "
            "failed defer-commit run.",
        )
        return True

    def _recover(self, orphans: list[int]) -> None:
        """Receive the page's orphans by sequence number (PEEK_LOCK on the commit client), one
        partition per call, and run them through the batch processor: their rows are written, then
        the ``DeferSettler`` re-defers them into the new pending set."""
        if not orphans:
            return
        if self._commit_client is None:
            self._commit_client = self._connector.commit_client()
        by_partition: dict[int, list[int]] = defaultdict(list)
        for seq in sorted(orphans):
            by_partition[partition_of(seq)].append(seq)
        size = min(COMMIT_MAX_COUNT, self._batch_size)
        # Recovery passes generation 0 and never recycles, so an unreadable-body retry would only
        # abandon the orphan (it stays DEFERRED, unrecovered): retries are off for its duration and an
        # unreadable orphan goes straight to its final disposition.
        unreadable = self._processor.unreadable
        retries_enabled, unreadable.retries_enabled = unreadable.retries_enabled, False
        try:
            with _peek_lock_receiver(self._connector, self._entity, self._commit_client) as receiver:
                for seqs in by_partition.values():
                    for begin in range(0, len(seqs), size):
                        received, gone = receive_deferred_bisect(
                            receiver, seqs[begin : begin + size], sleep=self._sleep
                        )
                        if gone:
                            logger.info(
                                "%d orphaned deferred message(s) on '%s' could no longer be received by sequence "
                                "number (settled, expired or locked meanwhile).",
                                len(gone),
                                self._entity.path,
                            )
                        if received:
                            self._processor.process(receiver, received, generation=0)
                            self._stats.orphans_recovered += len(received)
        finally:
            unreadable.retries_enabled = retries_enabled

    def _warn_guarded(self) -> None:
        guarded = self._stats.orphans_guarded
        if not guarded:
            return
        listed = ", ".join(str(seq) for seq in guarded[:ORPHAN_GUARD_LISTED])
        self._stats.warn(
            "orphan_guard",
            f"{len(guarded)} orphaned deferred message(s) on '{self._entity.path}' were not recovered because their "
            f"delivery count is at least {self._info.orphan_guard_threshold} (the entity's max delivery count minus "
            "1) and one more deferred receive could exhaust it; they stay DEFERRED. Sequence numbers (first "
            f"{ORPHAN_GUARD_LISTED}): {listed}.",
        )


class ForeignDeferralProbe:
    """C1 / C3, after H5: one peek page from the start of the entity; ``DEFERRED`` messages there
    belong to no state of this configuration -> WARNING. Detect-only: never receives or settles."""

    def __init__(
        self,
        connector: ServiceBusConnector,
        *,
        entity: EntityRef,
        info: EntityInfo,
        session_enabled: bool,
        stats: RunStats,
    ) -> None:
        self._connector = connector
        self._entity = entity
        self._info = info
        self._session_enabled = session_enabled
        self._stats = stats

    def probe(self) -> None:
        if self._session_enabled:
            return  # a peek on a session entity needs a session
        path = self._entity.path
        start = 0 if self._info.is_partitioned else 1  # cursor mode on partitioned entities
        try:
            with (
                self._connector.receive_client() as client,
                _peek_lock_receiver(self._connector, self._entity, client) as peeker,
            ):
                page = peeker.peek_messages(ORPHAN_PAGE_SIZE, sequence_number=start)
        except ServiceBusError as error:
            raise to_user_exception(error, path, self._connector.secrets) from error
        count = sum(1 for message in page if message.state == ServiceBusMessageState.DEFERRED)
        if count:
            self._stats.warn(
                "foreign_deferrals",
                f"{count} deferred message(s) on '{path}' are not owned by this configuration's state; if they came "
                "from a failed defer-commit run of this configuration, run it once in defer-commit mode to recover "
                "them.",
            )
