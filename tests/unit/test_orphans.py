from datetime import UTC, datetime, timedelta

import pytest
from azure.servicebus import ServiceBusMessageState, ServiceBusReceiveMode
from keboola.component.exceptions import UserException

from client import ServiceBusConnector
from commit import ForeignDeferralProbe, OrphanScanner, is_qualifying, k_stop
from configuration import AuthConfiguration, BodyFormat, EntityType, SettlementMode, SubQueue, UnreadablePolicy
from entity import EntityInfo, EntityRef, partition_of
from settlement import BatchProcessor, UnreadableHandler, make_settler
from state import PendingSetBuilder
from stats import RunStats
from tests.fakes.broker import make_message
from tests.fakes.recording import RecordingSink

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"
Q = EntityRef(EntityType.QUEUE, "q", None, None, SubQueue.NONE)


def connector() -> ServiceBusConnector:
    return ServiceBusConnector(AuthConfiguration(**{"#connection_string": SAS}), "kbc-test")


def scanner(broker, *, entity=Q, info=None, session_enabled=False, batch_size=100):
    stats = RunStats(mode="defer_commit")
    pending = PendingSetBuilder()
    sink = RecordingSink()
    mode = SettlementMode.DEFER_COMMIT
    proc = BatchProcessor(
        entity=entity,
        mode=mode,
        body_format=BodyFormat.TEXT,
        sink=sink,
        registry=None,
        settler=make_settler(mode, pending=pending, entity=entity, stats=stats),
        unreadable=UnreadableHandler(
            policy=UnreadablePolicy.DEAD_LETTER, mode=mode, is_sub_queue=entity.is_sub_queue, stats=stats
        ),
        stats=stats,
        clock=broker.clock.now,
    )
    scan = OrphanScanner(
        connector(),
        entity=entity,
        info=info or EntityInfo(),
        session_enabled=session_enabled,
        batch_size=batch_size,
        prefetch_count=1,
        processor=proc,
        stats=stats,
        clock=broker.clock.now,
        sleep=lambda s: None,
    )
    return scan, pending, sink, stats


def peeks(broker) -> int:
    return len([op for op, _ in broker.calls if op == "peek_messages"])


def test_k_stop():
    assert k_stop(100, 1) == 204
    assert k_stop(10, 1) == 100


def test_is_qualifying():
    now = datetime(2026, 9, 23, 10, 0, tzinfo=UTC)
    active = ServiceBusMessageState.ACTIVE
    assert is_qualifying(make_message(b"x", state=active, delivery_count=0), now)
    assert not is_qualifying(make_message(b"x", state=active, delivery_count=1), now)
    assert not is_qualifying(make_message(b"x", state=active, expires_at_utc=now - timedelta(seconds=1)), now)
    assert not is_qualifying(make_message(b"x", state=active, scheduled_enqueue_time_utc=now), now)
    assert not is_qualifying(make_message(b"x", state=ServiceBusMessageState.SCHEDULED), now)


def test_plain_orphan_recovered_and_redeferred(broker):
    q = broker.add_queue("q")
    orphan = q.send(b"o")
    q.defer_existing(orphan)
    q.send(b"active")
    scan, pending, sink, stats = scanner(broker)
    scan.scan()
    assert [r["body"] for r in sink.rows] == ["o"] and q.state_of(orphan) == "DEFERRED"
    assert pending.count == 1 and stats.orphans_recovered == 1


def test_scan_stops_after_k(broker):
    q = broker.add_queue("q")
    for _ in range(150):
        q.send(b"a")
    late = q.send(b"late")
    q.defer_existing(late)
    scan, pending, _, _ = scanner(broker, batch_size=10)  # K = 100
    scan.scan()
    assert pending.count == 0 and peeks(broker) == 1


def test_locked_cluster_does_not_stop_scan_early(broker):
    q = broker.add_queue("q")
    for _ in range(60):
        q.lock_existing(q.send(b"l"), 60)
    orphan = q.send(b"o")
    q.defer_existing(orphan)
    scan, pending, _, _ = scanner(broker, batch_size=10)  # K = 100 > 60 locked
    scan.scan()
    assert pending.count == 1


def test_page_cap_warning(broker):
    q = broker.add_queue("q")
    for _ in range(20 * 250 + 1):
        q.send(b"r", delivery_count=1)  # redelivered stragglers never count toward K
    scan, _, _, stats = scanner(broker)
    scan.scan()
    assert "orphan_scan_incomplete" in stats.warnings and peeks(broker) == 20


def test_orphan_guard(broker):
    q = broker.add_queue("q", max_delivery_count=10)
    o = q.send(b"o", delivery_count=9)
    q.defer_existing(o)
    scan, pending, _, stats = scanner(broker, info=EntityInfo(max_delivery_count=10))
    scan.scan()
    assert pending.count == 0 and stats.orphans_guarded == [o] and "orphan_guard" in stats.warnings


def test_partitioned_best_effort(broker):
    p = broker.add_queue("p", partitioned=True)
    ref = EntityRef(EntityType.QUEUE, "p", None, None, SubQueue.NONE)
    for i in range(3):
        p.defer_existing(p.send(b"o", partition=i))
    scan, pending, _, stats = scanner(broker, entity=ref, info=EntityInfo(partitioned=True))
    scan.scan()
    assert pending.count == 3 and "orphan_best_effort" in stats.warnings


def test_sub_queue_best_effort(broker):
    q = broker.add_queue("q")
    seq = q.send(b"x")
    q.dead_letter_existing(seq, "r", "d")
    q.dead_letter.defer_existing(seq)
    ref = EntityRef(EntityType.QUEUE, "q", None, None, SubQueue.DEAD_LETTER)
    scan, pending, _, stats = scanner(broker, entity=ref)
    scan.scan()
    assert pending.count == 1 and "orphan_best_effort" in stats.warnings


def test_sessions_warn_without_peeking(broker):
    broker.add_queue("s", sessions=True)
    ref = EntityRef(EntityType.QUEUE, "s", None, None, SubQueue.NONE)
    scan, _, _, stats = scanner(broker, entity=ref, session_enabled=True)
    scan.scan()
    assert "orphan_sessions" in stats.warnings and peeks(broker) == 0


def test_foreign_deferral_probe_warns_and_touches_nothing(broker):
    q = broker.add_queue("q")
    f = q.send(b"f")
    q.defer_existing(f)
    stats = RunStats(mode="complete")
    ForeignDeferralProbe(connector(), entity=Q, info=EntityInfo(), session_enabled=False, stats=stats).probe()
    assert "foreign_deferrals" in stats.warnings and q.state_of(f) == "DEFERRED"


def test_probe_skips_sessions(broker):
    broker.add_queue("s", sessions=True)
    ref = EntityRef(EntityType.QUEUE, "s", None, None, SubQueue.NONE)
    stats = RunStats(mode="complete")
    ForeignDeferralProbe(connector(), entity=ref, info=EntityInfo(), session_enabled=True, stats=stats).probe()
    assert stats.warnings == {} and peeks(broker) == 0


# --- beyond the brief ------------------------------------------------------------------------------

P = EntityRef(EntityType.QUEUE, "p", None, None, SubQueue.NONE)
DLQ = EntityRef(EntityType.QUEUE, "q", None, None, SubQueue.DEAD_LETTER)
PROFILE = {"prefetch_count": 1, "keep_alive": 0, "client_identifier": "kbc-test"}


def build(broker, *, entity=Q, info=None, batch_size=100, policy=UnreadablePolicy.DEAD_LETTER):
    """Like ``scanner`` but with a configurable policy, also returning the processor."""
    stats = RunStats(mode="defer_commit")
    pending = PendingSetBuilder()
    sink = RecordingSink()
    mode = SettlementMode.DEFER_COMMIT
    proc = BatchProcessor(
        entity=entity,
        mode=mode,
        body_format=BodyFormat.TEXT,
        sink=sink,
        registry=None,
        settler=make_settler(mode, pending=pending, entity=entity, stats=stats),
        unreadable=UnreadableHandler(policy=policy, mode=mode, is_sub_queue=entity.is_sub_queue, stats=stats),
        stats=stats,
        clock=broker.clock.now,
    )
    scan = OrphanScanner(
        connector(),
        entity=entity,
        info=info or EntityInfo(),
        session_enabled=False,
        batch_size=batch_size,
        prefetch_count=1,
        processor=proc,
        stats=stats,
        clock=broker.clock.now,
        sleep=lambda s: None,
    )
    return scan, proc, pending, sink, stats


def peek_starts(broker) -> list[int]:
    """The ``sequence_number`` of every peek, in call order (0 = the SDK's cursor mode)."""
    return [
        details["sequence_number"]
        for receiver in broker.receivers
        for op, details in receiver.operations
        if op == "peek_messages"
    ]


def deferred_calls(broker) -> list[list[int]]:
    return [
        details["sequence_numbers"]
        for receiver in broker.receivers
        for op, details in receiver.operations
        if op == "receive_deferred_messages"
    ]


def commit_clients(broker) -> list:
    return [client for client in broker.clients if client.kwargs.get("retry_total") == 0]


def test_plain_scan_pages_explicitly_from_one_and_opens_no_commit_client(broker):
    q = broker.add_queue("q")
    for _ in range(300):
        q.send(b"r", delivery_count=1)
    scan, _, _, _, stats = build(broker)
    scan.scan()
    assert peek_starts(broker) == [1, 251, 301] and stats.warnings == {}
    assert commit_clients(broker) == [] and all(client.closed for client in broker.clients)
    (peeker,) = broker.receivers
    assert peeker.kwargs == {"receive_mode": ServiceBusReceiveMode.PEEK_LOCK, **PROFILE} and peeker.closed


def test_orphans_before_the_k_stop_are_recovered(broker):
    q = broker.add_queue("q")
    first = q.send(b"first")
    q.defer_existing(first)
    for _ in range(150):
        q.send(b"a")
    late = q.send(b"late")
    q.defer_existing(late)
    scan, _, pending, sink, stats = build(broker, batch_size=10)
    scan.scan()
    assert [row["body"] for row in sink.rows] == ["first"] and pending.count == 1 and peek_starts(broker) == [1]
    assert stats.orphans_recovered == 1 and q.state_of(late) == "DEFERRED"


def test_skipped_messages_never_count_toward_k(broker):
    q = broker.add_queue("q")
    start = broker.clock.now()
    for _ in range(40):
        q.send(b"expired", ttl_seconds=1)
    broker.clock.advance(5)
    for _ in range(40):
        q.send(b"scheduled", scheduled_at=start + timedelta(hours=1))
    for _ in range(40):
        q.send(b"activated", scheduled_at=start - timedelta(minutes=1))
    for _ in range(40):
        q.send(b"redelivered", delivery_count=2)
    orphan = q.send(b"o")
    q.defer_existing(orphan)
    scan, _, pending, _, _ = build(broker, batch_size=10)  # K = 100 < 160 skipped
    scan.scan()
    assert pending.count == 1


def test_partitioned_scan_peeks_in_cursor_mode_and_recovers_per_partition(broker):
    p = broker.add_queue("p", partitioned=True)
    orphans = [p.send(b"o", partition=i % 3) for i in range(6)]
    for seq in orphans:
        p.defer_existing(seq)
    scan, _, pending, _, stats = build(broker, entity=P, info=EntityInfo(partitioned=True))
    scan.scan()
    assert pending.count == 6 and stats.orphans_recovered == 6
    assert peek_starts(broker) and set(peek_starts(broker)) == {0}
    assert all(len({partition_of(seq) for seq in call}) == 1 for call in deferred_calls(broker))
    assert "best-effort" in stats.warnings["orphan_best_effort"]


def test_heuristic_flip_switches_to_best_effort_for_the_rest_of_the_scan(broker):
    p = broker.add_queue("p", partitioned=True)
    for _ in range(150):
        p.send(b"a")
    orphan = p.send(b"o", partition=1)
    p.defer_existing(orphan)
    info = EntityInfo()  # partitioning unknown (management denied)
    scan, _, pending, _, stats = build(broker, entity=P, info=info, batch_size=10)  # K = 100 < 150
    scan.scan()
    assert info.is_partitioned and "orphan_best_effort" in stats.warnings
    assert pending.count == 1  # the K rule is not applied after the flip
    assert peek_starts(broker) == [1, 0]


def test_sub_queue_scan_pages_explicitly_without_the_k_rule(broker):
    q = broker.add_queue("q")
    for _ in range(150):
        q.dead_letter_existing(q.send(b"d"), "r", "d")
    orphan = q.send(b"o")
    q.dead_letter_existing(orphan, "r", "d")
    q.dead_letter.defer_existing(orphan)
    scan, _, pending, _, stats = build(broker, entity=DLQ, batch_size=10)  # K would be 100 < 150
    scan.scan()
    assert pending.count == 1 and "best-effort" in stats.warnings["orphan_best_effort"]
    assert peek_starts(broker) == [1, orphan + 1]


def test_recovery_receiver_profile_and_batch_size_chunks(broker):
    q = broker.add_queue("q")
    orphans = [q.send(b"o") for _ in range(5)]
    for seq in orphans:
        q.defer_existing(seq)
    scan, _, pending, _, stats = build(broker, batch_size=2)
    scan.scan()
    assert deferred_calls(broker) == [orphans[0:2], orphans[2:4], orphans[4:]]
    assert pending.count == 5 and stats.orphans_recovered == 5 and stats.deferred == 5
    (client,) = commit_clients(broker)
    (recovery,) = [receiver for receiver in broker.receivers if receiver.client is client]
    assert recovery.kwargs == {"receive_mode": ServiceBusReceiveMode.PEEK_LOCK, **PROFILE}
    assert client.closed and recovery.closed


def test_unreadable_orphan_goes_straight_to_its_policy(broker):
    q = broker.add_queue("q")
    orphan = q.send(b"o")
    q.defer_existing(orphan)
    broker.inject_body_error(orphan, times=1)
    scan, proc, pending, _, stats = build(broker)
    scan.scan()
    assert stats.unreadable["UnreadableBody:dead_lettered"] == 1 and stats.unreadable["UnreadableBody:retry"] == 0
    assert q.dead_letter.sequence_numbers() == [orphan] and pending.count == 0
    assert proc.unreadable.retries_enabled is True


def test_retries_are_restored_when_recovery_raises(broker):
    q = broker.add_queue("q")
    orphan = q.send(b"o")
    q.defer_existing(orphan)
    broker.inject_body_error(orphan, times=1)
    scan, proc, _, _, _ = build(broker, policy=UnreadablePolicy.FAIL)
    with pytest.raises(UserException, match="policy is 'fail'"):
        scan.scan()
    assert proc.unreadable.retries_enabled is True and all(client.closed for client in broker.clients)


def test_guard_warning_lists_the_first_20(broker):
    q = broker.add_queue("q")
    guarded = [q.send(b"o", delivery_count=9) for _ in range(25)]
    for seq in guarded:
        q.defer_existing(seq)
    scan, _, pending, _, stats = build(broker)
    scan.scan()
    assert stats.orphans_guarded == guarded and pending.count == 0 and commit_clients(broker) == []
    message = stats.warnings["orphan_guard"]
    assert message.startswith("25 ") and message.endswith(": " + ", ".join(map(str, guarded[:20])) + ".")


def test_scan_cap_warning_text(broker):
    q = broker.add_queue("q")
    for _ in range(20 * 250 + 1):
        q.send(b"r", delivery_count=1)
    scan, _, _, _, stats = build(broker)
    scan.scan()
    assert "orphan scan incomplete" in stats.warnings["orphan_scan_incomplete"]


def test_scan_errors_are_user_exceptions(broker):
    broker.add_queue("q")
    broker.auth_failure = True
    scan, _, _, _, _ = build(broker)
    with pytest.raises(UserException, match="Authentication to Azure Service Bus failed for 'q'"):
        scan.scan()


def test_probe_reads_one_page_from_the_start_and_never_receives(broker):
    q = broker.add_queue("q")
    foreign = [q.send(b"f") for _ in range(2)]
    for seq in foreign:
        q.defer_existing(seq)
    for _ in range(300):
        q.send(b"a")
    stats = RunStats(mode="complete")
    ForeignDeferralProbe(connector(), entity=Q, info=EntityInfo(), session_enabled=False, stats=stats).probe()
    assert peek_starts(broker) == [1]
    (receiver,) = broker.receivers
    assert [op for op, _ in receiver.operations] == ["peek_messages"] and receiver.closed
    assert receiver.kwargs == {"receive_mode": ServiceBusReceiveMode.PEEK_LOCK, **PROFILE}
    assert commit_clients(broker) == [] and all(client.closed for client in broker.clients)
    assert stats.warnings["foreign_deferrals"] == (
        "2 deferred message(s) on 'q' are not owned by this configuration's state; if they came from a failed "
        "defer-commit run of this configuration, run it once in defer-commit mode to recover them."
    )
    assert all(q.state_of(seq) == "DEFERRED" for seq in foreign)


def test_probe_uses_cursor_mode_on_partitioned_entities(broker):
    p = broker.add_queue("p", partitioned=True)
    p.defer_existing(p.send(b"f", partition=2))
    stats = RunStats(mode="receive_and_delete")
    ForeignDeferralProbe(
        connector(), entity=P, info=EntityInfo(partitioned=True), session_enabled=False, stats=stats
    ).probe()
    assert peek_starts(broker) == [0] and "foreign_deferrals" in stats.warnings


def test_probe_without_deferrals_is_silent(broker):
    q = broker.add_queue("q")
    q.send(b"a")
    stats = RunStats(mode="complete")
    ForeignDeferralProbe(connector(), entity=Q, info=EntityInfo(), session_enabled=False, stats=stats).probe()
    assert stats.warnings == {} and peeks(broker) == 1


def test_probe_errors_are_user_exceptions(broker):
    broker.add_queue("q")
    broker.auth_failure = True
    stats = RunStats(mode="complete")
    probe = ForeignDeferralProbe(connector(), entity=Q, info=EntityInfo(), session_enabled=False, stats=stats)
    with pytest.raises(UserException, match="Authentication to Azure Service Bus failed for 'q'"):
        probe.probe()
