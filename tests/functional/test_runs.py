"""Functional run cases 20-45 (spec §8): every settlement mode, entity type and state transition.

Every case runs the real component against the FakeBroker (see ``conftest.py``). The broker's clock
drives T0, ``extracted_at_utc`` and every lock, so outputs are deterministic; case 20 compares its
table and manifest byte-for-byte with ``expected/20_run_c1_queue/``.
"""

from datetime import timedelta
from pathlib import Path

from azure.servicebus.exceptions import ServiceBusServerBusyError

from state import iter_ranges
from tests.functional.conftest import run_case, run_twice, summary

EXPECTED = Path(__file__).resolve().parent / "expected"


def queue_ref(name: str = "q", sub_queue: str = "none") -> dict:
    return {
        "entity_type": "queue",
        "queue_name": name,
        "topic_name": None,
        "subscription_name": None,
        "sub_queue": sub_queue,
    }


def pending_state(ranges: list[list[int]], *, max_body_bytes: int = 1) -> dict:
    """An input state holding one pending-commit group on queue ``q`` (a previous C2 run's deferrals)."""
    group = {"session_id": None, "partition": 0, "max_body_bytes": max_body_bytes, "ranges": ranges}
    entity = {"entity": queue_ref(), "groups": [group], "deferred_at_utc": "2026-09-23 09:00:00.000000"}
    return {"version": 1, "pending_commit": [entity]}


def pending_seqs(state: dict | None) -> list[int]:
    assert state is not None
    return sorted(
        seq for entity in state["pending_commit"] for g in entity["groups"] for seq in iter_ranges(g["ranges"])
    )


def test_20_run_c1_queue(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    q.send(
        b'{"order_id": 1, "status": "new"}',
        message_id="m-1",
        content_type="application/json",
        subject="order",
        application_properties={"source": "web", "priority": 1},
    )
    q.send(
        b'{"order_id": 2, "status": "paid"}',
        message_id="m-2",
        content_type="application/json",
        subject="order",
        correlation_id="c-2",
        ttl_seconds=3600,
    )
    result = run_case("20_run_c1_queue", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    tables = result.out_dir / "tables"
    golden = EXPECTED / "20_run_c1_queue"
    assert (tables / "q.csv").read_bytes() == (golden / "q.csv").read_bytes()
    assert (tables / "q.csv.manifest").read_bytes() == (golden / "q.csv.manifest").read_bytes()
    assert result.manifests["q.csv"]["write_always"] is True
    assert q.sequence_numbers() == []
    assert result.state is not None and result.state["pending_commit"] == []


def test_21_run_c1_subscription_sp(fake_broker, tmp_path, monkeypatch, capsys):
    s = fake_broker.add_subscription("t", "s")
    s.send(b"a")
    s.send(b"b")
    result = run_case("21_run_c1_subscription_sp", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    rows = result.tables["t_s.csv"]
    assert len(rows) == 2 and {row["source_entity"] for row in rows} == {"t/Subscriptions/s"}
    assert fake_broker.credentials and {client.auth_kind for client in fake_broker.clients} == {"credential"}
    assert s.sequence_numbers() == []


def test_21_run_c1_subscription_sp_credentials_rejected(fake_broker, tmp_path, monkeypatch, capsys):
    """Variant of case 21: Entra ID rejects the secret; the run fails with the credentials message."""
    s = fake_broker.add_subscription("t", "s")
    s.send(b"a")
    fake_broker.credential_failure = "AADSTS7000215: Invalid client secret provided."
    result = run_case("21_run_c1_subscription_sp", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "The service principal credentials were rejected" in result.stderr and "AADSTS7000215" in result.stderr
    assert result.tables["t_s.csv"] == [] and result.state is None
    assert s.sequence_numbers() == [1] and s.delivery_count(1) == 0


def test_22_run_c2_first_run_defers(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    seqs = [q.send(f"m{i}".encode()) for i in range(3)]
    result = run_case("22_run_c2_first_run_defers", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert len(result.tables["q.csv"]) == 3
    assert all(q.state_of(seq) == "DEFERRED" for seq in seqs)
    assert result.state is not None
    assert result.state["pending_commit"][0]["groups"][0]["ranges"] == [[1, 3]]
    assert result.manifests["q.csv"]["write_always"] is False


def test_23_run_c2_second_run_commits(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")

    def seed(index: int) -> None:
        for i in range(3 if index == 0 else 2):
            q.send(f"run{index}-{i}".encode())

    first, second = run_twice("23_run_c2_second_run_commits", tmp_path, monkeypatch, capsys, before_each=seed)
    assert first.exit_code == 0 and second.exit_code == 0
    assert q.sequence_numbers() == [4, 5] and all(q.state_of(seq) == "DEFERRED" for seq in (4, 5))
    assert second.state is not None
    assert second.state["pending_commit"][0]["groups"][0]["ranges"] == [[4, 5]]
    assert summary(second)["committed"] == "3"
    assert [row["sequence_number"] for row in second.tables["q.csv"]] == ["4", "5"]


def test_24_run_c2_commit_not_found_bisection(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    for i in range(4):
        q.send(f"m{i}".encode())
    for seq in (1, 3, 4):
        q.defer_existing(seq)
    q.dead_letter_existing(2, "GoneMeanwhile", None)  # no longer in the queue when the commit runs
    result = run_case(
        "24_run_c2_commit_not_found_bisection", tmp_path, monkeypatch, capsys, state=pending_state([[1, 4]])
    )
    assert result.exit_code == 0
    tokens = summary(result)
    assert tokens["already_gone"] == "1" and tokens["committed"] == "3"
    assert q.sequence_numbers() == [] and result.state is not None and result.state["pending_commit"] == []


def test_25_run_c2_commit_transient_retry_exhausted(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    q.defer_existing(q.send(b"deferred by the previous run"))
    active = q.send(b"active")
    fake_broker.inject_commit_errors([ServiceBusServerBusyError(message="busy") for _ in range(4)])
    sleeps: list[float] = []
    monkeypatch.setattr("time.sleep", sleeps.append)  # the backoffs happen without waiting
    result = run_case(
        "25_run_c2_commit_transient_retry_exhausted", tmp_path, monkeypatch, capsys, state=pending_state([[1, 1]])
    )
    assert result.exit_code == 1
    assert "next run retries" in result.stderr
    assert sleeps == [2, 4, 8]
    assert not any(operation == "receive_messages" for operation, _ in fake_broker.calls)  # nothing received
    assert q.state_of(active) == "ACTIVE" and q.delivery_count(active) == 0 and q.state_of(1) == "DEFERRED"
    assert result.state is None  # no out/state.json: the input state stays


def test_26_run_c2_orphan_recovery_plain(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    orphan = q.send(b"orphan")
    q.defer_existing(orphan)  # deferred by a C2 run whose state was lost
    q.send(b"a")
    q.send(b"b")
    result = run_case("26_run_c2_orphan_recovery_plain", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert sorted(row["body"] for row in result.tables["q.csv"]) == ["a", "b", "orphan"]
    assert orphan in pending_seqs(result.state)
    assert summary(result)["orphans_recovered"] == "1"


def test_27_run_c2_orphan_scan_locked_cluster(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    for _ in range(60):
        q.lock_existing(q.send(b"locked"), seconds=300)  # an interrupted receiver's locks: peek as never delivered
    orphan = q.send(b"orphan")
    q.defer_existing(orphan)
    result = run_case("27_run_c2_orphan_scan_locked_cluster", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert [row["body"] for row in result.tables["q.csv"]] == ["orphan"]
    assert pending_seqs(result.state) == [orphan]
    assert summary(result)["orphans_recovered"] == "1"


def test_28_run_c2_orphan_scan_cap_warning(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    for _ in range(5001):
        q.send(b"x", delivery_count=1)  # delivered before: never counts toward the K stop
    result = run_case("28_run_c2_orphan_scan_cap_warning", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert "orphan scan incomplete" in result.stderr
    assert len(result.tables["q.csv"]) == 1


def test_29_run_c2_orphan_guard_delivery_count(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    orphan = q.send(b"orphan", delivery_count=9)
    q.defer_existing(orphan)
    result = run_case("29_run_c2_orphan_guard_delivery_count", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert "were not recovered because their delivery count" in result.stderr
    assert f"Sequence numbers (first 20): {orphan}." in result.stderr
    assert result.tables["q.csv"] == []
    assert q.state_of(orphan) == "DEFERRED" and q.delivery_count(orphan) == 9
    assert pending_seqs(result.state) == []


def test_30_run_c2_partitioned(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q", partitioned=True)
    seqs = [q.send(f"p{partition}".encode(), partition=partition) for partition in range(3)]
    first, second = run_twice("30_run_c2_partitioned", tmp_path, monkeypatch, capsys)
    assert first.exit_code == 0 and second.exit_code == 0
    assert first.state is not None
    groups = first.state["pending_commit"][0]["groups"]
    assert [group["partition"] for group in groups] == [seq >> 48 for seq in seqs] == [51, 52, 53]
    assert summary(second)["committed"] == "3" and q.sequence_numbers() == []
    assert second.state is not None and second.state["pending_commit"] == []
    for result in (first, second):
        assert "Orphan recovery is best-effort on partitioned entities" in result.stderr


def test_31_run_c2_session_entity(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q", sessions=True)
    q.send(b"a1", session_id="A")
    q.send(b"b1", session_id="B")
    first, second = run_twice("31_run_c2_session_entity", tmp_path, monkeypatch, capsys)
    assert first.exit_code == 0 and second.exit_code == 0
    assert first.state is not None
    assert [group["session_id"] for group in first.state["pending_commit"][0]["groups"]] == ["A", "B"]
    assert summary(second)["committed"] == "2" and q.sequence_numbers() == []
    assert second.state is not None and second.state["pending_commit"] == []
    assert "recovery is not available for session entities" in first.stderr


def test_32_run_c2_dlq(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    for i in range(2):
        q.dead_letter_existing(q.send(f"dead{i}".encode()), "R", "D")
    first, second = run_twice("32_run_c2_dlq", tmp_path, monkeypatch, capsys)
    assert first.exit_code == 0 and second.exit_code == 0
    assert len(first.tables["q_dead_letter.csv"]) == 2
    assert summary(second)["committed"] == "2"
    assert q.dead_letter.sequence_numbers() == []
    assert "Orphan recovery is best-effort on dead-letter sub-queues" in first.stderr


def test_33_run_c2_state_budget(fake_broker, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("state.STATE_BUDGET_BYTES", 1)
    monkeypatch.setattr("receiver.STATE_BUDGET_BYTES", 1)
    q = fake_broker.add_queue("q")
    q.send(b"x")
    result = run_case("33_run_c2_state_budget", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert "state budget" in result.stderr
    assert summary(result)["stop"] == "state_budget"
    assert q.state_of(1) == "ACTIVE"


def test_34_run_c3_receive_and_delete(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    for i in range(3):
        q.send(f"m{i}".encode())
    result = run_case("34_run_c3_receive_and_delete", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert [row["body"] for row in result.tables["q.csv"]] == ["m0", "m1", "m2"]  # 1 + the drained buffer of 2
    assert "deletes messages on delivery" in result.stderr  # the at-most-once WARNING
    assert result.manifests["q.csv"]["write_always"] is True
    assert q.sequence_numbers() == []


def test_35_run_c3_prefetch_refused(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("q").send(b"x")
    result = run_case("35_run_c3_prefetch_refused", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "prefetch" in result.stderr
    assert fake_broker.receivers == []


def test_36_run_c4_incremental_two_runs(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")

    def seed(index: int) -> None:
        for _ in range(2 if index == 0 else 1):
            q.send(b"x")

    first, second = run_twice("36_run_c4_incremental_two_runs", tmp_path, monkeypatch, capsys, before_each=seed)
    assert first.exit_code == 0 and second.exit_code == 0
    assert [row["sequence_number"] for row in first.tables["q.csv"]] == ["1", "2"]
    assert [row["sequence_number"] for row in second.tables["q.csv"]] == ["3"]
    assert q.sequence_numbers() == [1, 2, 3] and all(q.delivery_count(seq) == 0 for seq in (1, 2, 3))
    assert second.state is not None
    assert second.state["peek_cursor"] == {"entity_path": "q", "last_sequence_number": 3}


def test_37_run_c4_full_fetch(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    now = fake_broker.clock.now()
    q.defer_existing(q.send(b"deferred"))
    q.send(b"scheduled", scheduled_at=now + timedelta(hours=1))
    q.send(b"expired", enqueued_at=now - timedelta(hours=2), ttl_seconds=3600)
    result = run_case("37_run_c4_full_fetch", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    # the scheduled message is not active yet: skipped and counted, exported once it activates
    assert [(row["body"], row["state"]) for row in result.tables["q.csv"]] == [("deferred", "DEFERRED")]
    tokens = summary(result)
    assert tokens["expired_skipped"] == "1" and tokens["skipped_scheduled"] == "1"
    assert "Skipped 1 scheduled message(s)" in result.log


def test_38_run_c4_incremental_partitioned_refused(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("q", partitioned=True).send(b"x", partition=1)
    result = run_case("38_run_c4_incremental_partitioned_refused", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "Full Fetch" in result.stderr


def test_39_run_c4_leftover_pending_carried(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    for i in range(2):
        q.defer_existing(q.send(f"m{i}".encode()))
    state = pending_state([[1, 2]])
    result = run_case("39_run_c4_leftover_pending_carried", tmp_path, monkeypatch, capsys, state=state)
    assert result.exit_code == 0
    assert result.state is not None and result.state["pending_commit"] == state["pending_commit"]
    assert "still pending" in result.stderr
    assert all(q.state_of(seq) == "DEFERRED" and q.delivery_count(seq) == 0 for seq in (1, 2))


def test_40_run_c1_leftover_pending_committed(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    for i in range(2):
        q.defer_existing(q.send(f"m{i}".encode()))
    result = run_case(
        "40_run_c1_leftover_pending_committed", tmp_path, monkeypatch, capsys, state=pending_state([[1, 2]])
    )
    assert result.exit_code == 0
    assert q.sequence_numbers() == []
    assert result.state is not None and result.state["pending_commit"] == []
    assert summary(result)["committed"] == "2" and result.tables["q.csv"] == []


def test_41_run_c1_foreign_deferral_probe(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    foreign = q.send(b"foreign")
    q.defer_existing(foreign)
    q.send(b"active")
    result = run_case("41_run_c1_foreign_deferral_probe", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert "not owned by this configuration" in result.stderr
    assert q.sequence_numbers() == [foreign] and q.state_of(foreign) == "DEFERRED" and q.delivery_count(foreign) == 0
    assert [row["body"] for row in result.tables["q.csv"]] == ["active"]


def test_42_run_sessions_loop(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q", sessions=True)
    q.send(b"a1", session_id="A")
    q.send(b"b1", session_id="B")
    q.send(b"a2", session_id="A")
    result = run_case("42_run_sessions_loop", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert sorted(row["body"] for row in result.tables["q.csv"]) == ["a1", "a2", "b1"]
    assert q.sequence_numbers() == []
    assert summary(result)["stop"] == "no_more_sessions"


def test_43_run_session_mismatch(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("q", sessions=True).send(b"x", session_id="A")
    result = run_case("43_run_session_mismatch", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "Sessions" in result.stderr


def test_44_run_dlq_c1(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    q.dead_letter_existing(q.send(b"dead"), "R", "D")
    result = run_case("44_run_dlq_c1", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    (row,) = result.tables["q_dead_letter.csv"]
    assert row["dead_letter_reason"] == "R" and row["dead_letter_error_description"] == "D"
    assert row["source_entity"] == "q/$DeadLetterQueue"
    assert q.dead_letter.sequence_numbers() == []


def test_45_run_tdlq_c1(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    q.transfer_dead_letter.send(b"transfer dead")
    result = run_case("45_run_tdlq_c1", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    (row,) = result.tables["q_transfer_dead_letter.csv"]
    assert row["source_entity"] == "q/$Transfer/$DeadLetterQueue"
    assert q.transfer_dead_letter.sequence_numbers() == []
