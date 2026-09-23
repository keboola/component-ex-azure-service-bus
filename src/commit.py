"""Deferred-message commit (H5, Task 12, spec §6.3).

``PendingCommitter`` deletes the messages the previous defer-commit run deferred -- this row's
stored pending set -- at the start of every destructive mode, before anything is received. Each
stored (entity, session, partition) group is read back by sequence number on a RECEIVE_AND_DELETE
receiver of the dedicated commit client (``retry_total=0``), in chunks of at most 250 sequence
numbers and 16 MiB of bodies; the returned messages are discarded (the run that deferred them
already imported their rows). The commit client has no SDK retries, so the transient errors are
retried here (2 / 4 / 8 s); a sequence number the broker no longer has is bisected out and counted
as already gone.

Failing here is safe: nothing has been received yet and the input state is untouched, so the next
run simply retries. Message bodies are never logged.
"""

import logging
import time
from collections import deque
from collections.abc import Callable
from contextlib import closing
from functools import partial
from typing import Any

from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode
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
from entity import EntityRef
from state import PendingEntity, PendingGroup, to_ranges
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


def receive_deferred_bisect(
    receiver: Any,
    seqs: list[int],
    *,
    sleep: Callable[[float], None],
    extra: tuple[type[Exception], ...] = (),
) -> tuple[list[Any], list[int]]:
    """``(received_messages, not_found_seqs)`` for one deferred receive of ``seqs`` (one partition,
    at most 250). A ``MessageNotFoundError`` fails the whole call [live], so the call is split in
    halves until every missing sequence number fails alone; those are returned as not found.
    Every call goes through :func:`with_transient_retry` (``extra`` is passed on)."""
    if not seqs:
        return [], []
    call = partial(receiver.receive_deferred_messages, seqs, timeout=DEFERRED_RECEIVE_TIMEOUT_SECONDS)
    try:
        return list(with_transient_retry(call, sleep=sleep, extra=extra)), []
    except MessageNotFoundError:
        if len(seqs) == 1:
            return [], list(seqs)
    middle = len(seqs) // 2
    left, left_gone = receive_deferred_bisect(receiver, seqs[:middle], sleep=sleep, extra=extra)
    right, right_gone = receive_deferred_bisect(receiver, seqs[middle:], sleep=sleep, extra=extra)
    return left + right, left_gone + right_gone


def _count(groups: list[PendingGroup]) -> int:
    return sum(len(group.sequence_numbers()) for group in groups)


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
            seqs = sorted(set(group.sequence_numbers()))
            size = commit_chunk_size(group.max_body_bytes)
            chunks = deque(seqs[start : start + size] for start in range(0, len(seqs), size))
            try:
                self._delete_group(client, ref, group.session_id, chunks)
            except SessionCannotBeLockedError:
                left = [seq for chunk in chunks for seq in chunk]
                carried.append(group.model_copy(update={"ranges": to_ranges(left)}))
                self._stats.carried_forward += len(left)
            except (*_STALE_ERRORS, UserException) as error:
                # UserException: `open_receiver`'s EntityPath mismatch -- a connection string scoped to
                # another entity can no longer read the stored one.
                if ref == self._configured:
                    if isinstance(error, UserException):
                        raise
                    raise to_user_exception(error, ref.path, self._connector.secrets) from error
                self._drop_stale(ref, sum(len(chunk) for chunk in chunks) + _count(groups[index + 1 :]))
                break  # the entity's remaining groups are just as unreadable
            except TRANSIENT_ERRORS as error:
                raise UserException(
                    f"Could not delete the {_count(groups)} message(s) deferred by the previous run on '{ref.path}': "
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
        self, client: ServiceBusClient, ref: EntityRef, session_id: str | None, chunks: deque[list[int]]
    ) -> None:
        """Delete ``chunks`` in order on one RECEIVE_AND_DELETE receiver. A chunk leaves the deque
        only once its call returned, so when this raises ``chunks`` still holds every chunk not yet
        deleted. Opening the receiver and its first call also retry a session held by another
        receiver (``SessionCannotBeLockedError``)."""
        receiver = with_transient_retry(
            partial(self._open_receiver, client, ref, session_id), sleep=self._sleep, extra=_SESSION_LOCKED
        )
        with closing(receiver):
            extra = _SESSION_LOCKED
            while chunks:
                received, gone = receive_deferred_bisect(receiver, chunks[0], sleep=self._sleep, extra=extra)
                extra = ()
                self._stats.committed += len(received)
                self._stats.already_gone += len(gone)
                chunks.popleft()

    def _open_receiver(self, client: ServiceBusClient, ref: EntityRef, session_id: str | None) -> Any:
        """Open (attach) the group's receiver, so entity and session errors surface here. A sub-queue
        has no sessions: a dead-lettered session message keeps its ``session_id``, but the SDK refuses
        ``session_id`` together with ``sub_queue``."""
        kwargs: dict[str, Any] = {
            "receive_mode": ServiceBusReceiveMode.RECEIVE_AND_DELETE,
            "prefetch_count": 1,
            "keep_alive": 0,
            "client_identifier": self._connector.client_identifier,
        }
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
