import pytest
from azure.servicebus import NEXT_AVAILABLE_SESSION, ServiceBusReceiveMode
from azure.servicebus.exceptions import (
    MessageNotFoundError,
    ServiceBusConnectionError,
    ServiceBusServerBusyError,
    SessionCannotBeLockedError,
)
from keboola.component.exceptions import UserException

from client import ServiceBusConnector
from commit import PendingCommitter, commit_chunk_size, receive_deferred_bisect, with_transient_retry
from configuration import AuthConfiguration, EntityType, SubQueue
from entity import EntityRef
from state import PendingSetBuilder
from stats import RunStats

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"
Q = EntityRef(EntityType.QUEUE, "q", None, None, SubQueue.NONE)


def connector() -> ServiceBusConnector:
    return ServiceBusConnector(AuthConfiguration(**{"#connection_string": SAS}), "kbc-test")


def deferred(entity_fake, n, **send):
    seqs = [entity_fake.send(b"x", **send) for _ in range(n)]
    for s in seqs:
        entity_fake.defer_existing(s)
    return seqs


def pending_for(ref, seqs, session_id=None, body_bytes=10):
    b = PendingSetBuilder()
    for s in seqs:
        b.add(ref, s, session_id, body_bytes)
    return b.build("2026-09-23 10:00:00.000000")


def committer(sleeps=None, configured=Q, stats=None):
    stats = stats or RunStats(mode="complete")
    return PendingCommitter(
        connector(), configured=configured, stats=stats, sleep=(sleeps.append if sleeps is not None else lambda s: None)
    ), stats


def test_chunk_size():
    assert commit_chunk_size(10) == 250
    assert commit_chunk_size(8 * 1024 * 1024) == 2
    assert commit_chunk_size(100 * 1024 * 1024) == 1


def test_commit_plain_in_chunks_of_250(broker):
    q = broker.add_queue("q")
    seqs = deferred(q, 600)
    c, stats = committer()
    assert c.commit(pending_for(Q, seqs)) == []
    assert q.sequence_numbers() == [] and stats.committed == 600
    calls = [op for op, _ in broker.calls if op == "receive_deferred_messages"]
    assert len(calls) == 3


def test_commit_uses_retry_total_zero_client(broker):
    q = broker.add_queue("q")
    c, _ = committer()
    c.commit(pending_for(Q, deferred(q, 1)))
    assert any(client.kwargs.get("retry_total") == 0 for client in broker.clients)


def test_byte_cap_chunks(broker):
    q = broker.add_queue("q")
    seqs = deferred(q, 5)
    c, _ = committer()
    c.commit(pending_for(Q, seqs, body_bytes=8 * 1024 * 1024))
    assert len([op for op, _ in broker.calls if op == "receive_deferred_messages"]) == 3


def test_partitions_committed_separately(broker):
    p = broker.add_queue("p", partitioned=True)
    ref = EntityRef(EntityType.QUEUE, "p", None, None, SubQueue.NONE)
    seqs = [p.send(b"x", partition=i % 3) for i in range(9)]
    for s in seqs:
        p.defer_existing(s)
    c, stats = committer(configured=ref)
    c.commit(pending_for(ref, seqs))
    assert p.sequence_numbers() == [] and stats.committed == 9


def test_not_found_is_already_gone(broker):
    q = broker.add_queue("q")
    seqs = deferred(q, 4)
    c, stats = committer()
    c.commit(pending_for(Q, seqs + [9999]))
    assert stats.committed == 4 and stats.already_gone == 1


def test_bisect_directly(broker):
    q = broker.add_queue("q")
    seqs = deferred(q, 3)
    from azure.servicebus import ServiceBusReceiveMode

    client = connector().commit_client()
    with client.get_queue_receiver(
        "q", receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE, prefetch_count=1, keep_alive=0
    ) as r:
        received, gone = receive_deferred_bisect(r, [seqs[0], 777, seqs[1], seqs[2]], sleep=lambda s: None)
    assert sorted(m.sequence_number for m in received) == seqs and gone == [777]


def test_transient_retry_then_success(broker):
    q = broker.add_queue("q")
    seqs = deferred(q, 2)
    broker.inject_commit_errors([ServiceBusServerBusyError(message="busy")])
    sleeps: list[float] = []
    c, stats = committer(sleeps)
    c.commit(pending_for(Q, seqs))
    assert sleeps == [2] and stats.committed == 2


def test_transient_exhausted_fails_run_state_intact(broker):
    q = broker.add_queue("q")
    seqs = deferred(q, 2)
    broker.inject_commit_errors([ServiceBusServerBusyError(message="busy")] * 4)
    sleeps: list[float] = []
    c, _ = committer(sleeps)
    with pytest.raises(UserException, match="next run retries"):
        c.commit(pending_for(Q, seqs))
    assert sleeps == [2, 4, 8] and all(q.state_of(s) == "DEFERRED" for s in seqs)


def test_session_group_committed(broker):
    s = broker.add_queue("s", sessions=True)
    ref = EntityRef(EntityType.QUEUE, "s", None, None, SubQueue.NONE)
    seqs = deferred(s, 2, session_id="A")
    c, stats = committer(configured=ref)
    c.commit(pending_for(ref, seqs, session_id="A"))
    assert s.sequence_numbers() == [] and stats.committed == 2


def test_session_locked_is_carried_forward(broker):
    s = broker.add_queue("s", sessions=True)
    ref = EntityRef(EntityType.QUEUE, "s", None, None, SubQueue.NONE)
    seqs = deferred(s, 1, session_id="A")
    s.send(b"active", session_id="A")
    holder = (
        connector()
        .receive_client()
        .get_queue_receiver("s", session_id=NEXT_AVAILABLE_SESSION, prefetch_count=1, keep_alive=0)
    )
    holder.receive_messages()  # holds session A
    c, stats = committer(configured=ref)
    carried = c.commit(pending_for(ref, seqs, session_id="A"))
    assert carried and carried[0].groups[0].session_id == "A" and stats.carried_forward == 1
    assert "commit_session_locked" in stats.warnings


def test_stale_entity_dropped_with_warning(broker):
    broker.add_queue("q")
    old = EntityRef(EntityType.QUEUE, "gone", None, None, SubQueue.NONE)
    c, stats = committer(configured=Q)
    assert c.commit(pending_for(old, [1, 2])) == []
    assert stats.dropped_stale == 2 and "commit_stale_entity" in stats.warnings


def test_configured_entity_missing_is_user_exception(broker):
    c, _ = committer(configured=Q)
    with pytest.raises(UserException):
        c.commit(pending_for(Q, [1]))


# --- beyond the brief ------------------------------------------------------------------------------


def _failing(errors: list[Exception], result: str = "ok"):
    """A callable raising each of ``errors`` once, then returning ``result``; ``calls`` counts attempts."""
    calls: list[int] = []

    def fn() -> str:
        calls.append(1)
        if errors:
            raise errors.pop(0)
        return result

    return fn, calls


def test_with_transient_retry_backs_off_then_reraises():
    fn, calls = _failing([ServiceBusConnectionError(message="down")] * 5)
    sleeps: list[float] = []
    with pytest.raises(ServiceBusConnectionError):
        with_transient_retry(fn, sleep=sleeps.append)
    assert len(calls) == 4 and sleeps == [2, 4, 8]


def test_with_transient_retry_extra_is_retried_and_other_errors_are_not():
    fn, calls = _failing([SessionCannotBeLockedError(message="held")])
    assert with_transient_retry(fn, sleep=lambda s: None, extra=(SessionCannotBeLockedError,)) == "ok"
    assert len(calls) == 2
    fn, calls = _failing([SessionCannotBeLockedError(message="held")])
    with pytest.raises(SessionCannotBeLockedError):
        with_transient_retry(fn, sleep=lambda s: None)
    fn, calls = _failing([MessageNotFoundError(message="gone")])
    with pytest.raises(MessageNotFoundError):
        with_transient_retry(fn, sleep=lambda s: None)
    assert len(calls) == 1


def test_nothing_to_commit_opens_no_client(broker):
    c, stats = committer()
    assert c.commit([]) == [] and broker.clients == []
    empty = pending_for(Q, [])
    assert c.commit(empty) == [] and broker.clients == [] and stats.committed == 0


def test_commit_receiver_profile_order_and_cleanup(broker):
    q = broker.add_queue("q")
    seqs = deferred(q, 5)
    c, _ = committer()
    c.commit(pending_for(Q, seqs, body_bytes=8 * 1024 * 1024))
    (receiver,) = broker.receivers
    assert receiver.kwargs == {
        "receive_mode": ServiceBusReceiveMode.RECEIVE_AND_DELETE,
        "prefetch_count": 1,
        "keep_alive": 0,
        "client_identifier": "kbc-test",
    }
    calls = [details for op, details in receiver.operations if op == "receive_deferred_messages"]
    assert [call["sequence_numbers"] for call in calls] == [seqs[0:2], seqs[2:4], seqs[4:]]
    assert all(call["timeout"] == 60 for call in calls)
    assert len(broker.clients) == 1 and broker.clients[0].closed and receiver.closed


def test_transient_exhausted_message_is_redacted_and_client_closed(broker):
    q = broker.add_queue("q")
    seqs = deferred(q, 2)
    broker.inject_commit_errors([ServiceBusServerBusyError(message="busy SharedAccessKey=c2VjcmV0")] * 4)
    c, _ = committer()
    with pytest.raises(UserException) as raised:
        c.commit(pending_for(Q, seqs))
    text = str(raised.value)
    assert "Could not delete the 2 message(s) deferred by the previous run on 'q'" in text
    assert "SharedAccessKey=***" in text and "c2VjcmV0" not in text
    assert "Nothing was extracted in this run and the state is unchanged" in text
    assert all(client.closed for client in broker.clients) and all(r.closed for r in broker.receivers)


def test_session_locked_carry_keeps_group_and_retries_open(broker):
    s = broker.add_queue("s", sessions=True)
    ref = EntityRef(EntityType.QUEUE, "s", None, None, SubQueue.NONE)
    seqs = deferred(s, 2, session_id="A")
    s.send(b"active", session_id="A")
    holder = (
        connector()
        .receive_client()
        .get_queue_receiver("s", session_id=NEXT_AVAILABLE_SESSION, prefetch_count=1, keep_alive=0)
    )
    holder.receive_messages()
    sleeps: list[float] = []
    c, stats = committer(sleeps, configured=ref)
    (carried,) = c.commit(pending_for(ref, seqs, session_id="A", body_bytes=77))
    (group,) = carried.groups
    assert group.sequence_numbers() == seqs and group.max_body_bytes == 77
    assert carried.deferred_at_utc == "2026-09-23 10:00:00.000000" and carried.entity_ref() == ref
    assert sleeps == [2, 4, 8] and stats.carried_forward == 2 and stats.committed == 0
    assert all(s.state_of(seq) == "DEFERRED" for seq in seqs)
    assert "'A'" in stats.warnings["commit_session_locked"]


def test_stale_entity_drops_every_group_with_one_warning(broker):
    broker.add_queue("q")
    old = EntityRef(EntityType.QUEUE, "gone", None, None, SubQueue.NONE)
    in_two_partitions = [(51 << 48) | 1, (52 << 48) | 1, (52 << 48) | 2]
    c, stats = committer(configured=Q)
    assert c.commit(pending_for(old, in_two_partitions)) == []
    assert stats.dropped_stale == 3
    assert stats.warnings["commit_stale_entity"].startswith(
        "3 message(s) deferred by an earlier run stay DEFERRED on 'gone'"
    )


def test_stale_entity_outside_an_entity_scoped_sas_is_dropped(broker):
    broker.add_queue("q")
    broker.add_queue("old")
    scoped = ServiceBusConnector(AuthConfiguration(**{"#connection_string": SAS + ";EntityPath=q"}), "kbc-test")
    stats = RunStats(mode="complete")
    old = EntityRef(EntityType.QUEUE, "old", None, None, SubQueue.NONE)
    committer_ = PendingCommitter(scoped, configured=Q, stats=stats, sleep=lambda s: None)
    assert committer_.commit(pending_for(old, [1])) == []
    assert stats.dropped_stale == 1 and "commit_stale_entity" in stats.warnings


def test_configured_entity_auth_error_names_the_entity(broker):
    c, _ = committer(configured=Q)
    with pytest.raises(UserException, match="Authentication to Azure Service Bus failed for 'q'"):
        c.commit(pending_for(Q, [1]))
    assert all(client.closed for client in broker.clients)


def test_sub_queue_group_commits_without_a_session(broker):
    s = broker.add_queue("s", sessions=True)
    seq = s.send(b"x", session_id="A")
    s.dead_letter_existing(seq, "r", "d")
    s.dead_letter.defer_existing(seq)
    ref = EntityRef(EntityType.QUEUE, "s", None, None, SubQueue.DEAD_LETTER)
    c, stats = committer(configured=ref)
    assert c.commit(pending_for(ref, [seq], session_id="A")) == []
    assert s.dead_letter.sequence_numbers() == [] and stats.committed == 1
    assert "session_id" not in broker.receivers[0].kwargs


def test_other_service_bus_error_is_mapped(broker):
    q = broker.add_queue("q")
    c, _ = committer(configured=Q)
    with pytest.raises(UserException, match="disable Sessions"):
        c.commit(pending_for(Q, deferred(q, 1), session_id="A"))
