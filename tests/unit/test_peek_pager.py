from datetime import timedelta
from typing import Any

import pytest
from azure.servicebus.exceptions import ServiceBusError
from keboola.component.exceptions import UserException

import receiver as receiver_mod
from client import ServiceBusConnector
from configuration import AuthConfiguration, Configuration
from entity import EntityInfo, EntityRef
from receiver import PeekPager
from settlement import BatchProcessor, UnreadableHandler, make_settler
from state import PeekCursor
from stats import RunStats
from tests.fakes.recording import RecordingSink

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"


def pager(
    broker, *, fetch_mode="incremental_fetch", queue="q", cursor=None, info=None, limits=None, t0=None, session=False
):
    source = {
        "entity_type": "queue",
        "queue_name": queue,
        "settlement_mode": "peek",
        "fetch_mode": fetch_mode,
        "session_enabled": session,
    }
    params: dict[str, Any] = {"#connection_string": SAS, "source": source}
    if limits:
        params["limits"] = limits
    config = Configuration(**params)
    entity = EntityRef.from_source(config.source)
    stats = RunStats(mode="peek")
    sink = RecordingSink()
    proc = BatchProcessor(
        entity=entity,
        mode=config.source.settlement_mode,
        body_format=config.body.body_format,
        sink=sink,
        registry=None,
        settler=make_settler(config.source.settlement_mode, pending=None, entity=entity, stats=stats),
        unreadable=UnreadableHandler(
            policy=config.body.unreadable_body, mode=config.source.settlement_mode, is_sub_queue=False, stats=stats
        ),
        stats=stats,
        clock=broker.clock.now,
    )
    p = PeekPager(
        connector=ServiceBusConnector(AuthConfiguration(**{"#connection_string": SAS}), "kbc-test"),
        entity=entity,
        info=info or EntityInfo(),
        config=config,
        processor=proc,
        stats=stats,
        cursor=cursor,
        t0=t0 or broker.clock.now() + timedelta(hours=1),
        clock=broker.clock.now,
        monotonic=lambda: broker.clock.now().timestamp(),
    )
    return p, sink, stats


def test_incremental_two_runs_nothing_settled(broker):
    q = broker.add_queue("q")
    first = [q.send(b"a"), q.send(b"b")]
    p, sink, _ = pager(broker)
    cursor = p.run()
    assert cursor == PeekCursor(entity_path="q", last_sequence_number=first[-1]) and len(sink.rows) == 2
    assert q.sequence_numbers() == first  # nothing removed or locked
    third = q.send(b"c")
    p2, sink2, _ = pager(broker, cursor=cursor)
    assert p2.run().last_sequence_number == third and [r["body"] for r in sink2.rows] == ["c"]


def test_cursor_for_other_entity_resets(broker):
    q = broker.add_queue("q")
    q.send(b"a")
    p, sink, _ = pager(broker, cursor=PeekCursor(entity_path="other", last_sequence_number=999))
    p.run()
    assert len(sink.rows) == 1


def test_full_fetch_states_and_expired_skipped(broker):
    q = broker.add_queue("q")
    d = q.send(b"d")
    q.defer_existing(d)
    q.send(b"s", scheduled_at=broker.clock.now() + timedelta(hours=1))
    q.send(b"e", ttl_seconds=1)
    broker.clock.advance(5)
    p, sink, stats = pager(broker, fetch_mode="full_fetch")
    assert p.run() is None
    assert sorted(r["state"] for r in sink.rows) == ["DEFERRED", "SCHEDULED"] and stats.expired_skipped == 1


def test_incremental_refused_on_partitioned(broker):
    broker.add_queue("q", partitioned=True).send(b"a", partition=2)
    p, sink, _ = pager(broker, info=EntityInfo(partitioned=True))
    with pytest.raises(UserException, match="Full Fetch"):
        p.run()
    assert sink.rows == []


def test_incremental_refused_by_heuristic(broker):
    broker.add_queue("q", partitioned=True).send(b"a", partition=2)
    p, sink, _ = pager(broker)
    with pytest.raises(UserException, match="Full Fetch"):
        p.run()
    assert sink.rows == []


def test_watermark_and_max_messages(broker):
    q = broker.add_queue("q")
    for _ in range(3):
        q.send(b"old")
    broker.clock.advance(10)
    t0 = broker.clock.now()
    q.send(b"new")
    p, sink, _ = pager(broker, t0=t0)
    p.run()
    assert [r["body"] for r in sink.rows] == ["old", "old", "old"]
    p2, sink2, _ = pager(broker, limits={"max_messages": 2}, t0=t0)
    p2.run()
    assert len(sink2.rows) == 2


def test_full_fetch_sessions(broker):
    s = broker.add_queue("s", sessions=True)
    s.send(b"a", session_id="A")
    s.send(b"b", session_id="B")
    p, sink, stats = pager(broker, fetch_mode="full_fetch", queue="s", session=True)
    p.run()
    assert sorted(r["body"] for r in sink.rows) == ["a", "b"] and "peek_sessions" in stats.warnings
    assert all(r.kwargs.get("max_wait_time") == 5 for r in broker.receivers if r.kwargs.get("session_id"))


def test_session_wait_ignores_hidden_idle_timeout(broker):
    broker.add_queue("s", sessions=True).send(b"a", session_id="A")
    source: dict[str, Any] = {
        "entity_type": "queue",
        "queue_name": "s",
        "settlement_mode": "peek",
        "fetch_mode": "full_fetch",
        "session_enabled": True,
        "idle_timeout_seconds": 99,
    }
    params: dict[str, Any] = {"#connection_string": SAS, "source": source}
    config = Configuration(**params)
    assert config.source.idle_timeout_seconds == 99  # kept by the model, but must not drive peek
    p, _, _ = pager(broker, fetch_mode="full_fetch", queue="s", session=True)
    p.config = config
    p.run()
    assert {r.kwargs.get("max_wait_time") for r in broker.receivers if r.kwargs.get("session_id")} == {5}


def test_repeek_after_unreadable_retry_does_not_duplicate(broker):
    q = broker.add_queue("q")
    seqs = [q.send(b"a"), q.send(b"b"), q.send(b"c")]
    broker.inject_body_error(seqs[1], times=1)
    p, sink, stats = pager(broker)
    cursor = p.run()
    assert [r["body"] for r in sink.rows].count("a") == 1
    assert sorted(r["sequence_number"] for r in sink.rows) == [str(s) for s in seqs]
    assert stats.unreadable_recycles == 1 and cursor.last_sequence_number == seqs[-1]


def test_cursor_moves_past_a_skipped_last_message(broker):
    q = broker.add_queue("q")
    good = q.send(b"a")
    bad = q.send(b"b")
    broker.inject_body_error(bad, times=2)
    p, sink, stats = pager(broker)
    cursor = p.run()
    assert [r["body"] for r in sink.rows] == ["a"] and cursor.last_sequence_number == bad
    assert stats.unreadable["UnreadableBody:skipped"] == 1 and good < bad


def test_partitioned_full_fetch_skips_without_retry(broker):
    pq = broker.add_queue("q", partitioned=True)
    ok = pq.send(b"a", partition=1)
    bad = pq.send(b"b", partition=2)
    broker.inject_body_error(bad, times=1)
    p, sink, stats = pager(broker, fetch_mode="full_fetch", info=EntityInfo(partitioned=True))
    p.run()
    assert [r["sequence_number"] for r in sink.rows] == [str(ok)]
    assert stats.unreadable_recycles == 0 and stats.unreadable["UnreadableBody:skipped"] == 1
    # cursor mode: explicit paging misses partitions on the real broker (the fake cannot show it)
    peeks = [d for r in broker.receivers for op, d in r.operations if op == "peek_messages"]
    assert peeks and all(d["sequence_number"] == 0 for d in peeks)


def test_scheduled_message_does_not_stop_watermark(broker):
    q = broker.add_queue("q")
    q.send(b"sched", scheduled_at=broker.clock.now() + timedelta(hours=2))
    q.send(b"old")
    t0 = broker.clock.now() + timedelta(seconds=1)
    p, sink, _ = pager(broker, fetch_mode="full_fetch", t0=t0)
    p.run()
    assert sorted(r["body"] for r in sink.rows) == ["old", "sched"]


def test_fail_policy_in_peek_is_not_recycled(broker):
    # amendment 2: the processor's own UserException is never treated as a connection failure
    from configuration import UnreadablePolicy

    q = broker.add_queue("q")
    q.send(b"a")
    bad = q.send(b"b")
    broker.inject_body_error(bad, times=5)
    p, sink, stats = pager(broker)
    p.processor.unreadable.policy = UnreadablePolicy.FAIL
    stats.unreadable_recycles = 50  # retry budget spent: the first failure is final
    with pytest.raises(UserException, match="unreadable"):
        p.run()
    assert [r["body"] for r in sink.rows] == ["a"] and stats.recoveries == 0 and len(broker.clients) == 1


# --- beyond the brief -------------------------------------------------------------------------------


def peek_ops(receiver):
    return [d for op, d in receiver.operations if op == "peek_messages"]


def test_peek_only_peeks_and_pages_explicitly(broker, monkeypatch):
    monkeypatch.setattr(receiver_mod, "PEEK_PAGE_SIZE", 2)
    q = broker.add_queue("q")
    seqs = [q.send(b"m") for _ in range(5)]
    p, sink, stats = pager(broker)
    assert p.run().last_sequence_number == seqs[-1] and len(sink.rows) == 5
    (only,) = broker.receivers
    assert {op for op, _ in only.operations} == {"peek_messages"}
    assert [(d["max_message_count"], d["sequence_number"]) for d in peek_ops(only)] == [(2, 1), (2, 3), (2, 5), (2, 6)]
    assert only.kwargs["keep_alive"] == 0 and only.kwargs["client_identifier"] == "kbc-test" and only.closed
    assert stats.stop_reason == "end_of_entity" and q.sequence_numbers() == seqs


def test_cursor_resumes_after_the_stored_sequence_number(broker):
    q = broker.add_queue("q")
    seqs = [q.send(b"m") for _ in range(3)]
    p, sink, _ = pager(broker, cursor=PeekCursor(entity_path="q", last_sequence_number=seqs[0]))
    p.run()
    assert peek_ops(broker.receivers[0])[0]["sequence_number"] == seqs[1]
    assert [r["sequence_number"] for r in sink.rows] == [str(s) for s in seqs[1:]]


def test_nothing_new_keeps_the_cursor(broker):
    q = broker.add_queue("q")
    seq = q.send(b"m")
    cursor = PeekCursor(entity_path="q", last_sequence_number=seq)
    p, sink, _ = pager(broker, cursor=cursor)
    assert p.run() == cursor and sink.rows == []


def test_expired_last_message_moves_the_cursor(broker):
    q = broker.add_queue("q")
    q.send(b"a")
    expired = q.send(b"e", ttl_seconds=1)
    broker.clock.advance(5)
    p, sink, stats = pager(broker)
    assert p.run().last_sequence_number == expired
    assert [r["body"] for r in sink.rows] == ["a"] and stats.expired_skipped == 1


def test_watermark_stop_keeps_the_cursor_before_it(broker):
    q = broker.add_queue("q")
    old = q.send(b"old")
    broker.clock.advance(10)
    t0 = broker.clock.now()
    q.send(b"new")
    q.send(b"newer")
    p, _, stats = pager(broker, t0=t0)
    assert p.run().last_sequence_number == old
    assert stats.stop_reason == "watermark" and "watermark_approximate" not in stats.warnings


def test_max_messages_cuts_a_page_and_the_cursor(broker):
    q = broker.add_queue("q")
    seqs = [q.send(b"m") for _ in range(5)]
    p, sink, stats = pager(broker, limits={"max_messages": 3})
    assert p.run().last_sequence_number == seqs[2]
    assert len(sink.rows) == 3 and stats.stop_reason == "max_messages"


def test_max_duration_stops_between_pages(broker, monkeypatch):
    monkeypatch.setattr(receiver_mod, "PEEK_PAGE_SIZE", 1)
    q = broker.add_queue("q")
    seqs = [q.send(b"m") for _ in range(3)]
    p, sink, stats = pager(broker, limits={"max_duration_seconds": 60})
    original = p.processor.process

    def slow(*args, **kwargs):
        broker.clock.advance(61)
        return original(*args, **kwargs)

    p.processor.process = slow
    assert p.run().last_sequence_number == seqs[0]
    assert len(sink.rows) == 1 and stats.stop_reason == "max_duration"


def test_a_pending_retry_caps_the_cursor(broker):
    q = broker.add_queue("q")
    bad = q.send(b"x")
    q.send(b"good")
    broker.inject_body_error(bad, times=1)
    p, sink, stats = pager(broker, limits={"max_duration_seconds": 60})
    original = p.processor.process

    def slow(*args, **kwargs):
        broker.clock.advance(61)  # the deadline passes before the re-peek of the unreadable body
        return original(*args, **kwargs)

    p.processor.process = slow
    assert p.run() is None  # the cursor must not move past the body still awaiting its retry
    assert [r["body"] for r in sink.rows] == ["good"] and stats.unreadable_recycles == 1


def test_peek_error_recycles_without_duplicates(broker, monkeypatch):
    monkeypatch.setattr(receiver_mod, "PEEK_PAGE_SIZE", 1)
    q = broker.add_queue("q")
    seqs = [q.send(b"m") for _ in range(3)]
    broker.inject_peek_error(ServiceBusError(message="link detached"), on_call=2)
    p, sink, stats = pager(broker)
    assert p.run().last_sequence_number == seqs[-1]
    assert [r["sequence_number"] for r in sink.rows] == [str(s) for s in seqs]
    assert stats.recoveries == 1 and len(broker.clients) == 2


def test_peek_errors_without_progress_fail_the_run(broker):
    broker.add_queue("q").send(b"m")
    broker.inject_peek_error(ServiceBusError(message="link detached"), on_call=1)
    broker.inject_peek_error(ServiceBusError(message="link detached"), on_call=2)
    p, _, _ = pager(broker)
    with pytest.raises(UserException, match="Lost the connection to Service Bus 2 times"):
        p.run()


def test_auth_error_in_peek_is_user_exception(broker):
    broker.add_queue("q")
    broker.auth_failure = True
    p, _, stats = pager(broker)
    with pytest.raises(UserException, match="IP firewall"):
        p.run()
    assert stats.recoveries == 0


def test_partitioned_full_fetch_pages_in_cursor_mode(broker, monkeypatch):
    monkeypatch.setattr(receiver_mod, "PEEK_PAGE_SIZE", 1)
    pq = broker.add_queue("q", partitioned=True)
    seqs = [pq.send(b"m", partition=n) for n in (0, 3, 7)]
    p, sink, stats = pager(broker, fetch_mode="full_fetch", info=EntityInfo(partitioned=True))
    assert p.run() is None
    assert sorted(r["sequence_number"] for r in sink.rows) == sorted(str(s) for s in seqs)
    assert all(d["sequence_number"] == 0 for d in peek_ops(broker.receivers[0]))
    assert p.processor.unreadable.retries_enabled is True  # restored after the run
    assert stats.stop_reason == "end_of_entity"


def test_full_fetch_switches_to_cursor_mode_when_the_heuristic_flips(broker, monkeypatch):
    monkeypatch.setattr(receiver_mod, "PEEK_PAGE_SIZE", 2)
    pq = broker.add_queue("q", partitioned=True)
    seqs = [pq.send(b"m", partition=n) for n in (0, 1, 2)]
    info = EntityInfo()  # no management access: partitioning is unknown
    p, sink, _ = pager(broker, fetch_mode="full_fetch", info=info)
    p.run()
    assert sorted(r["sequence_number"] for r in sink.rows) == sorted(str(s) for s in seqs)  # each once
    explicit, cursor_mode = broker.receivers  # a fresh receiver for the cursor-mode pass
    assert [d["sequence_number"] for d in peek_ops(explicit)] == [1]
    assert peek_ops(cursor_mode) and all(d["sequence_number"] == 0 for d in peek_ops(cursor_mode))
    assert info.is_partitioned


def test_cursor_mode_recycle_drops_processed_messages(broker, monkeypatch):
    monkeypatch.setattr(receiver_mod, "PEEK_PAGE_SIZE", 1)
    pq = broker.add_queue("q", partitioned=True)
    seqs = [pq.send(b"m", partition=n) for n in (0, 1, 2)]
    broker.inject_peek_error(ServiceBusError(message="link detached"), on_call=3)
    p, sink, stats = pager(broker, fetch_mode="full_fetch", info=EntityInfo(partitioned=True))
    p.run()  # the fresh receiver's cursor starts over: the two processed messages are dropped
    assert sorted(r["sequence_number"] for r in sink.rows) == sorted(str(s) for s in seqs)
    assert stats.recoveries == 1


def test_session_peek_keeps_earlier_sessions_held(broker):
    s = broker.add_queue("s", sessions=True)
    for session_id in ("A", "B", "C"):
        s.send(session_id.lower().encode(), session_id=session_id)
    p, sink, stats = pager(broker, fetch_mode="full_fetch", queue="s", session=True)
    p.run()
    assert [r["body"] for r in sink.rows] == ["a", "b", "c"] and stats.stop_reason == "no_more_sessions"
    assert all(r.closed for r in broker.receivers)  # all closed once the pager finished
    assert p.processor.unreadable.retries_enabled is True
    assert s.sequence_numbers() == [1, 2, 3]  # nothing settled


def test_session_peek_skips_unreadable_without_retry(broker):
    s = broker.add_queue("s", sessions=True)
    bad = s.send(b"x", session_id="A")
    s.send(b"a", session_id="A")
    broker.inject_body_error(bad, times=1)
    p, sink, stats = pager(broker, fetch_mode="full_fetch", queue="s", session=True)
    p.run()
    assert [r["body"] for r in sink.rows] == ["a"] and stats.unreadable_recycles == 0
    assert stats.unreadable["UnreadableBody:skipped"] == 1


def test_session_watermark_moves_to_the_next_session(broker):
    s = broker.add_queue("s", sessions=True)
    s.send(b"b-old", session_id="B")
    broker.clock.advance(10)
    t0 = broker.clock.now()
    s.send(b"a-new", session_id="A")
    p, sink, stats = pager(broker, fetch_mode="full_fetch", queue="s", session=True, t0=t0)
    p.run()
    assert [r["body"] for r in sink.rows] == ["b-old"] and "watermark_approximate" in stats.warnings


def test_session_peek_recycle_repeeks_without_duplicates(broker):
    s = broker.add_queue("s", sessions=True)
    s.send(b"a", session_id="A")
    s.send(b"b", session_id="B")
    broker.inject_peek_error(ServiceBusError(message="link detached"), on_call=2)
    p, sink, stats = pager(broker, fetch_mode="full_fetch", queue="s", session=True)
    p.run()  # every held session was released: all are peeked again, processed ones dropped
    assert sorted(r["body"] for r in sink.rows) == ["a", "b"] and stats.recoveries == 1


def test_session_mismatch_in_peek_is_user_exception(broker):
    broker.add_queue("s", sessions=True).send(b"a", session_id="A")
    p, _, _ = pager(broker, fetch_mode="full_fetch", queue="s")
    with pytest.raises(UserException, match="Sessions"):
        p.run()
