import logging
from datetime import timedelta
from typing import Any

import pytest
from azure.servicebus.exceptions import ServiceBusError
from keboola.component.exceptions import UserException

from client import ServiceBusConnector
from configuration import AuthConfiguration, Configuration, UnreadablePolicy
from entity import EntityInfo, EntityRef
from receiver import MAX_RECOVERIES, ReceiveLoop, RecoveryTracker, StopReason
from settlement import BatchProcessor, UnreadableHandler, make_settler
from state import PendingSetBuilder
from stats import RunStats
from tests.fakes.recording import RecordingSink

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"


def build(broker, *, source=None, limits=None, advanced=None, t0=None, state_size=lambda: 0, info=None):
    params: dict[str, Any] = {
        "#connection_string": SAS,
        "source": source or {"entity_type": "queue", "queue_name": "q"},
    }
    if limits:
        params["limits"] = limits
    if advanced:
        params |= {"advanced_options": True, "advanced": advanced}
    config = Configuration(**params)
    entity = EntityRef.from_source(config.source)
    mode = config.source.settlement_mode
    stats = RunStats(mode=mode.value)
    pending = PendingSetBuilder()
    sink = RecordingSink()
    proc = BatchProcessor(
        entity=entity,
        mode=mode,
        body_format=config.body.body_format,
        sink=sink,
        registry=None,
        settler=make_settler(mode, pending=pending, entity=entity, stats=stats),
        unreadable=UnreadableHandler(
            policy=config.body.unreadable_body, mode=mode, is_sub_queue=entity.is_sub_queue, stats=stats
        ),
        stats=stats,
        clock=broker.clock.now,
    )
    loop = ReceiveLoop(
        connector=ServiceBusConnector(AuthConfiguration(**{"#connection_string": SAS}), "kbc-1-2"),
        entity=entity,
        info=info or EntityInfo(),
        config=config,
        processor=proc,
        stats=stats,
        t0=t0 or broker.clock.now() + timedelta(hours=1),
        state_size=state_size,
        arm_write_always=lambda: None,
        clock=broker.clock.now,
        monotonic=lambda: broker.clock.now().timestamp(),
        sleep=broker.clock.advance,
    )
    return loop, sink, stats, pending


def test_drains_until_idle_with_thread_free_profile(broker):
    q = broker.add_queue("q")
    for _ in range(5):
        q.send(b"a")
    loop, sink, stats, _ = build(broker, advanced={"batch_size": 2})
    assert loop.run() is StopReason.IDLE
    assert len(sink.rows) == 5 and q.sequence_numbers() == [] and stats.completed == 5
    kwargs = broker.receivers[0].kwargs
    assert kwargs["prefetch_count"] == 1 and kwargs["keep_alive"] == 0 and kwargs["client_identifier"] == "kbc-1-2"


def test_max_messages_and_c1_drain_abandons(broker):
    q = broker.add_queue("q")
    for _ in range(5):
        q.send(b"a")
    loop, sink, _, _ = build(broker, limits={"max_messages": 2})
    assert loop.run() is StopReason.MAX_MESSAGES
    assert len(sink.rows) == 2
    left = q.sequence_numbers()
    assert len(left) == 3 and sum(q.delivery_count(s) for s in left) == 2  # the 2 drained were abandoned


def test_c3_drain_writes_buffer(broker):
    q = broker.add_queue("q")
    for _ in range(5):
        q.send(b"a")
    loop, sink, _, _ = build(
        broker,
        source={"entity_type": "queue", "queue_name": "q", "settlement_mode": "receive_and_delete"},
        limits={"max_messages": 2},
    )
    loop.run()
    assert len(sink.rows) == 4 and len(q.sequence_numbers()) == 1


def test_watermark_stop(broker):
    q = broker.add_queue("q")
    q.send(b"old1")
    q.send(b"old2")
    broker.clock.advance(10)
    t0 = broker.clock.now()
    for _ in range(4):
        q.send(b"new")
    loop, sink, _, _ = build(broker, advanced={"batch_size": 2}, t0=t0)
    assert loop.run() is StopReason.WATERMARK
    assert len(sink.rows) == 4 and len(q.sequence_numbers()) == 2


def test_watermark_on_partitioned_warns(broker):
    p = broker.add_queue("q", partitioned=True)
    t0 = broker.clock.now() - timedelta(seconds=1)
    p.send(b"x", partition=1)
    loop, _, stats, _ = build(broker, t0=t0, info=EntityInfo(partitioned=True))
    loop.run()
    assert "watermark_approximate" in stats.warnings


def test_max_duration(broker):
    q = broker.add_queue("q")
    for _ in range(3):
        q.send(b"a")
    loop, sink, _, _ = build(broker, limits={"max_duration_seconds": 60}, advanced={"batch_size": 1})
    original = loop.processor.process

    def slow(*args, **kwargs):
        broker.clock.advance(61)
        return original(*args, **kwargs)

    loop.processor.process = slow
    assert loop.run() is StopReason.MAX_DURATION and len(sink.rows) == 1


def test_state_budget_stops_c2(broker):
    q = broker.add_queue("q")
    q.send(b"a")
    loop, _, stats, _ = build(
        broker,
        source={"entity_type": "queue", "queue_name": "q", "settlement_mode": "defer_commit"},
        state_size=lambda: 10**9,
    )
    assert loop.run() is StopReason.STATE_BUDGET and "state_budget" in stats.warnings
    assert "reached the 256 KiB state budget" in stats.warnings["state_budget"]


def test_recycle_on_receive_error(broker):
    q = broker.add_queue("q")
    for _ in range(3):
        q.send(b"a")
    broker.inject_receive_error(TypeError("'NoneType' object is not callable"), on_call=2)
    loop, sink, stats, _ = build(broker, advanced={"batch_size": 1})
    loop.run()
    assert len(sink.rows) == 3 and stats.recoveries == 1 and len(broker.clients) >= 2


def test_no_progress_guard(broker):
    broker.add_queue("q").send(b"a")
    broker.inject_receive_error(TypeError("boom"), on_call=1)
    broker.inject_receive_error(TypeError("boom"), on_call=2)
    loop, _, _, _ = build(broker)
    with pytest.raises(TypeError):
        loop.run()


def test_exhausted_service_bus_errors_become_user_exception(broker):
    q = broker.add_queue("q")
    for _ in range(12):
        q.send(b"a")
    for call in range(2, 14, 2):
        broker.inject_receive_error(ServiceBusError(message="link detached"), on_call=call)
    loop, _, _, _ = build(broker, advanced={"batch_size": 1})
    with pytest.raises(UserException, match="Lost the connection to Service Bus"):
        loop.run()


def test_auth_error_is_not_recycled(broker):
    broker.add_queue("q")
    broker.auth_failure = True
    loop, _, stats, _ = build(broker)
    with pytest.raises(UserException, match="IP firewall"):
        loop.run()
    assert stats.recoveries == 0


def test_sessions_loop(broker):
    s = broker.add_queue("s", sessions=True)
    s.send(b"a1", session_id="A")
    s.send(b"a2", session_id="A")
    s.send(b"b1", session_id="B")
    loop, sink, _, _ = build(broker, source={"entity_type": "queue", "queue_name": "s", "session_enabled": True})
    assert loop.run() is StopReason.NO_MORE_SESSIONS
    assert sorted(r["body"] for r in sink.rows) == ["a1", "a2", "b1"]


def test_session_mismatch_is_user_exception(broker):
    broker.add_queue("s", sessions=True).send(b"a", session_id="A")
    loop, _, _, _ = build(broker, source={"entity_type": "queue", "queue_name": "s"})
    with pytest.raises(UserException, match="Sessions"):
        loop.run()


def test_catch_up_collects_stragglers(broker):
    q = broker.add_queue("q", lock_seconds=30)
    straggler = q.send(b"s")
    q.lock_existing(straggler, 30)  # locked by an interrupted batch
    q.send(b"a")
    broker.inject_receive_error(TypeError("boom"), on_call=2)
    loop, sink, _, _ = build(broker, advanced={"recovery_wait_seconds": 60})
    loop.run()
    assert sorted(r["body"] for r in sink.rows) == ["a", "s"]


def test_unreadable_retries_do_not_consume_recovery_budget(broker):
    q = broker.add_queue("q")
    seqs = [q.send(b"x") for _ in range(8)]
    for seq in seqs:
        broker.inject_body_error(seq, times=2)
    good = q.send(b"ok")
    loop, sink, stats, _ = build(broker, advanced={"batch_size": 1})
    assert loop.run() is StopReason.IDLE
    assert stats.recoveries == 0 and stats.unreadable_recycles == 8
    assert all(q.dead_letter.state_of(s) == "ACTIVE" for s in seqs) and [r["body"] for r in sink.rows] == ["ok"]
    assert q.state_of(good) is None


def test_unreadable_share_abort_reached_before_any_cap(broker):
    q = broker.add_queue("q")
    for _ in range(12):
        broker.inject_body_error(q.send(b"x"), times=2)
    loop, _, stats, _ = build(broker, source={"entity_type": "queue", "queue_name": "q"}, advanced={"batch_size": 1})
    with pytest.raises(UserException, match="unreadable"):
        loop.run()
    assert stats.recoveries == 0 and stats.unreadable_recycles <= 12


def test_c3_arms_when_first_messages_arrive(broker):
    q = broker.add_queue("q")
    seq = q.send(b"a")
    events: list[str] = []
    loop, sink, _, _ = build(
        broker, source={"entity_type": "queue", "queue_name": "q", "settlement_mode": "receive_and_delete"}
    )
    loop.arm_write_always = lambda: events.append(f"arm:{q.state_of(seq)}:{len(sink.rows)}")
    loop.run()
    assert events == ["arm:None:0"]  # once, after the broker deleted it, before its row was written


def test_c3_empty_run_never_arms(broker):
    broker.add_queue("q")
    events: list[str] = []
    loop, _, _, _ = build(
        broker, source={"entity_type": "queue", "queue_name": "q", "settlement_mode": "receive_and_delete"}
    )
    loop.arm_write_always = lambda: events.append("arm")
    assert loop.run() is StopReason.IDLE and events == []


def test_c2_never_arms(broker):
    broker.add_queue("q").send(b"a")
    events: list[str] = []
    loop, _, _, _ = build(broker, source={"entity_type": "queue", "queue_name": "q", "settlement_mode": "defer_commit"})
    loop.arm_write_always = lambda: events.append("arm")
    loop.run()
    assert events == []


def test_session_lock_renewed_near_expiry(broker):
    s = broker.add_queue("s", sessions=True, lock_seconds=30)
    for _ in range(3):
        s.send(b"a", session_id="A")
    loop, sink, _, _ = build(
        broker, source={"entity_type": "queue", "queue_name": "s", "session_enabled": True}, advanced={"batch_size": 1}
    )
    original = loop.processor.process

    def slow(*args, **kwargs):
        broker.clock.advance(25)
        return original(*args, **kwargs)

    loop.processor.process = slow
    loop.run()
    assert len(sink.rows) == 3
    assert [op for op, _ in broker.calls].count("session_renew_lock") >= 1


def test_processor_user_exception_is_never_recycled(broker):
    # amendment 2: the fail policy raises after writing; it must not be retried as a connection failure
    q = broker.add_queue("q")
    seqs = [q.send(b"a") for _ in range(4)]
    broker.inject_body_error(seqs[1], times=5)
    loop, sink, stats, _ = build(
        broker,
        source={"entity_type": "queue", "queue_name": "q"},
        advanced={"batch_size": 1},
    )
    loop.processor.unreadable.policy = UnreadablePolicy.FAIL
    stats.unreadable_recycles = 50  # no retry: straight to the policy
    with pytest.raises(UserException, match="unreadable"):
        loop.run()
    assert stats.recoveries == 0 and len(broker.clients) == 1
    assert [r["body"] for r in sink.rows] == ["a"] and q.state_of(seqs[1]) == "ACTIVE"


def test_c3_drains_buffer_before_raising(broker):
    # amendment 1: in C3 the buffered messages are already deleted — write them before the run fails
    q = broker.add_queue("q")
    seqs = [q.send(b"m") for _ in range(5)]
    broker.inject_body_error(seqs[0], times=5)
    loop, sink, stats, _ = build(
        broker,
        source={"entity_type": "queue", "queue_name": "q", "settlement_mode": "receive_and_delete"},
        advanced={"batch_size": 2},
    )
    loop.processor.unreadable.policy = UnreadablePolicy.FAIL
    with pytest.raises(UserException, match="unreadable"):
        loop.run()
    # batch 1 = seqs 0-1 (seq 0 unreadable, seq 1 written), drain = prefetch_count + 1 = 2 more (seqs 2-3)
    assert len(sink.rows) == 3 and q.sequence_numbers() == [seqs[4]] and stats.recoveries == 0


# --- beyond the brief -------------------------------------------------------------------------------


def test_batches_shrink_to_the_remaining_message_budget(broker):
    q = broker.add_queue("q")
    for _ in range(5):
        q.send(b"a")
    loop, sink, _, _ = build(broker, limits={"max_messages": 3}, advanced={"batch_size": 2})
    assert loop.run() is StopReason.MAX_MESSAGES and len(sink.rows) == 3
    counts = [d["max_message_count"] for op, d in broker.receivers[0].operations if op == "receive_messages"]
    waits = [d["max_wait_time"] for op, d in broker.receivers[0].operations if op == "receive_messages"]
    assert counts == [2, 1, 2] and waits == [10, 10, 1]  # min(batch, remaining) twice, then the stop drain


def test_c3_arms_exactly_once_across_batches(broker):
    q = broker.add_queue("q")
    for _ in range(3):
        q.send(b"a")
    events: list[str] = []
    loop, sink, _, _ = build(
        broker,
        source={"entity_type": "queue", "queue_name": "q", "settlement_mode": "receive_and_delete"},
        advanced={"batch_size": 1},
    )
    loop.arm_write_always = lambda: events.append("arm")
    assert loop.run() is StopReason.IDLE
    assert events == ["arm"] and len(sink.rows) == 3


def test_retried_body_counts_once_toward_max_messages(broker):
    q = broker.add_queue("q")
    first = q.send(b"a")
    q.send(b"b")
    q.send(b"c")
    broker.inject_body_error(first, times=1)  # fails once, reads fine on the fresh connection
    loop, sink, stats, _ = build(broker, limits={"max_messages": 2}, advanced={"batch_size": 1})
    assert loop.run() is StopReason.MAX_MESSAGES
    assert [r["body"] for r in sink.rows] == ["a", "b"] and stats.unreadable_recycles == 1


def test_c1_drain_uses_abandon(broker):
    q = broker.add_queue("q")
    for _ in range(4):
        q.send(b"a")
    loop, _, _, _ = build(broker, limits={"max_messages": 1})
    loop.run()
    ops = [op for op, _ in broker.receivers[0].operations]
    assert ops == ["receive_messages", "complete_message", "receive_messages", "abandon_message", "abandon_message"]


def test_stop_drain_failure_is_logged_not_fatal(broker, caplog):
    q = broker.add_queue("q")
    for _ in range(3):
        q.send(b"a")
    broker.inject_receive_error(ServiceBusError(message="link detached"), on_call=2)  # the drain's receive
    loop, sink, stats, _ = build(broker, limits={"max_messages": 1})
    with caplog.at_level(logging.WARNING):
        assert loop.run() is StopReason.MAX_MESSAGES
    assert len(sink.rows) == 1 and stats.recoveries == 0
    assert any("stop drain failed" in r.getMessage() for r in caplog.records)


def test_processor_failure_is_raised_even_when_its_drain_fails(broker, caplog):
    q = broker.add_queue("q")
    seqs = [q.send(b"a") for _ in range(4)]
    broker.inject_body_error(seqs[1], times=5)
    broker.inject_receive_error(TypeError("boom"), on_call=3)  # call 3 = the drain after the failing batch
    loop, sink, stats, _ = build(broker, advanced={"batch_size": 1})
    loop.processor.unreadable.policy = UnreadablePolicy.FAIL
    stats.unreadable_recycles = 50
    with caplog.at_level(logging.WARNING), pytest.raises(UserException, match=f"Message {seqs[1]} "):
        loop.run()
    assert stats.recoveries == 0 and len(broker.clients) == 1 and [r["body"] for r in sink.rows] == ["a"]
    assert any("stop drain failed" in r.getMessage() for r in caplog.records)


def test_c3_second_failure_in_the_drain_is_swallowed(broker):
    q = broker.add_queue("q")
    seqs = [q.send(b"m") for _ in range(4)]
    broker.inject_body_error(seqs[0], times=5)
    broker.inject_body_error(seqs[1], times=5)
    loop, sink, _, _ = build(
        broker,
        source={"entity_type": "queue", "queue_name": "q", "settlement_mode": "receive_and_delete"},
        advanced={"batch_size": 1},
    )
    loop.processor.unreadable.policy = UnreadablePolicy.FAIL
    with pytest.raises(UserException, match=f"Message {seqs[0]} "):  # the original, not the drain's
        loop.run()
    assert [r["sequence_number"] for r in sink.rows] == [str(seqs[2])]  # the drained readable one is written
    assert q.sequence_numbers() == [seqs[3]]


def test_reverse_session_mismatch_is_user_exception(broker):
    broker.add_queue("q").send(b"a")
    loop, _, stats, _ = build(broker, source={"entity_type": "queue", "queue_name": "q", "session_enabled": True})
    with pytest.raises(UserException, match="disable Sessions"):
        loop.run()
    assert stats.recoveries == 0


def test_session_receivers_use_next_available_and_idle_wait(broker):
    from azure.servicebus import NEXT_AVAILABLE_SESSION

    broker.add_queue("s", sessions=True).send(b"a", session_id="A")
    loop, _, _, _ = build(
        broker,
        source={"entity_type": "queue", "queue_name": "s", "session_enabled": True, "idle_timeout_seconds": 7},
    )
    loop.run()
    kwargs = broker.receivers[0].kwargs
    assert kwargs["session_id"] is NEXT_AVAILABLE_SESSION and kwargs["max_wait_time"] == 7
    assert kwargs["keep_alive"] == 0 and kwargs["prefetch_count"] == 1


def test_session_watermark_continues_with_the_next_session(broker):
    s = broker.add_queue("s", sessions=True)
    s.send(b"b-old", session_id="B")
    broker.clock.advance(10)
    t0 = broker.clock.now()
    s.send(b"a-new", session_id="A")
    loop, sink, stats, _ = build(
        broker, source={"entity_type": "queue", "queue_name": "s", "session_enabled": True}, t0=t0
    )
    assert loop.run() is StopReason.NO_MORE_SESSIONS
    assert [r["body"] for r in sink.rows] == ["a-new", "b-old"]
    assert "watermark_approximate" not in stats.warnings


def test_session_handed_out_again_ends_the_loop(broker):
    s = broker.add_queue("s", sessions=True)
    t0 = broker.clock.now()
    seqs = [s.send(b"a", session_id="A") for _ in range(3)]
    s.send(b"b", session_id="B")
    loop, sink, _, _ = build(
        broker,
        source={"entity_type": "queue", "queue_name": "s", "session_enabled": True},
        advanced={"batch_size": 1},
        t0=t0,
    )
    assert loop.run() is StopReason.SESSION_REVISITED
    assert len(sink.rows) == 1 and s.sequence_numbers() == [*seqs[1:], seqs[-1] + 1]
    # drained (abandoned) once at A's watermark; the receiver that got A again never received: no drain
    assert [s.delivery_count(seq) for seq in seqs[1:]] == [1, 1]
    assert [op for op, _ in broker.receivers[1].operations] == []


def test_session_recycle_continues_the_interrupted_session(broker):
    s = broker.add_queue("s", sessions=True)
    for body in (b"a1", b"a2", b"a3"):
        s.send(body, session_id="A")
    broker.inject_receive_error(TypeError("boom"), on_call=2)
    loop, sink, stats, _ = build(
        broker, source={"entity_type": "queue", "queue_name": "s", "session_enabled": True}, advanced={"batch_size": 1}
    )
    assert loop.run() is StopReason.NO_MORE_SESSIONS  # re-accepting A after the recycle is not a revisit
    assert [r["body"] for r in sink.rows] == ["a1", "a2", "a3"] and stats.recoveries == 1


def test_session_catch_up_collects_a_straggler(broker):
    s = broker.add_queue("s", sessions=True, lock_seconds=30)
    straggler = s.send(b"s", session_id="A")
    s.lock_existing(straggler, 30)  # locked by an interrupted batch: A is not handed out until it lapses
    s.send(b"b", session_id="B")
    broker.inject_receive_error(TypeError("boom"), on_call=2)
    loop, sink, stats, _ = build(
        broker,
        source={"entity_type": "queue", "queue_name": "s", "session_enabled": True},
        advanced={"recovery_wait_seconds": 60},
    )
    assert loop.run() is StopReason.NO_MORE_SESSIONS
    assert [r["body"] for r in sink.rows] == ["b", "s"] and stats.recoveries == 1
    assert stats.stop_reason == "no_more_sessions"


def test_no_catch_up_without_a_recovery(broker):
    broker.add_queue("q").send(b"a")
    loop, _, stats, _ = build(broker, advanced={"recovery_wait_seconds": 60})
    start = broker.clock.now()
    assert loop.run() is StopReason.IDLE and broker.clock.now() == start
    assert stats.stop_reason == "idle"


def test_rows_written_before_a_settle_error_count_as_progress(broker):
    q = broker.add_queue("q")
    for _ in range(3):
        q.send(b"a")
    loop, sink, stats, _ = build(broker, advanced={"batch_size": 1})
    original = loop.processor.process
    calls = []

    def settle_fails(*args, **kwargs):
        result = original(*args, **kwargs)  # the row is written (and settled) ...
        calls.append(result.written)
        if len(calls) <= 2:
            raise ServiceBusError(message="link detached")  # ... then the connection drops before it returns
        return result

    loop.processor.process = settle_fails
    assert loop.run() is StopReason.IDLE  # two failures in a row, but each followed a written row
    assert stats.recoveries == 2 and len(sink.rows) == 3


def test_recovery_is_logged_with_redacted_error(broker, caplog):
    q = broker.add_queue("q")
    q.send(b"a")
    q.send(b"b")
    broker.inject_receive_error(ServiceBusError(message="detached SharedAccessKey=c2VjcmV0 sig=abc"), on_call=2)
    loop, sink, stats, _ = build(broker, advanced={"batch_size": 1})
    with caplog.at_level(logging.WARNING):
        loop.run()
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert stats.recoveries == 1 and len(sink.rows) == 2
    assert "recovery 1 of 5" in text and "c2VjcmV0" not in text and "sig=abc" not in text


# --- RecoveryTracker --------------------------------------------------------------------------------


def test_tracker_reraises_user_exception_unchanged():
    tracker = RecoveryTracker()
    error = UserException("fail policy")
    with pytest.raises(UserException) as caught:
        tracker.failure(error)
    assert caught.value is error and tracker.count == 0 and tracker.generation == 0


def test_tracker_counts_and_guards_progress():
    tracker = RecoveryTracker()
    tracker.failure(TypeError("first"))  # the first failure is always recycled
    assert (tracker.count, tracker.generation) == (1, 1)
    tracker.unreadable_retry()
    assert (tracker.count, tracker.generation) == (1, 2)  # a retry recycle is not a connection failure
    with pytest.raises(TypeError, match="second"):
        tracker.failure(TypeError("second"))  # no progress since the previous failure
    tracker.progress()
    tracker.failure(TypeError("third"))
    assert (tracker.count, tracker.generation) == (2, 3)


def test_tracker_unreadable_retry_does_not_count_as_progress():
    tracker = RecoveryTracker()
    tracker.failure(TypeError("first"))
    tracker.unreadable_retry()
    with pytest.raises(TypeError):
        tracker.failure(TypeError("second"))


def test_tracker_exhausted_service_bus_error_is_redacted_user_exception():
    tracker = RecoveryTracker(entity_path="q", secrets=("topsecret",))
    for _ in range(MAX_RECOVERIES):
        tracker.failure(ServiceBusError(message="detached"))
        tracker.progress()
    with pytest.raises(UserException) as caught:
        tracker.failure(ServiceBusError(message="gone topsecret SharedAccessKey=abc"))
    text = str(caught.value)
    assert text.startswith(f"Lost the connection to Service Bus {MAX_RECOVERIES + 1} times in this run (limit 5,")
    assert "topsecret" not in text and "SharedAccessKey=abc" not in text
    assert text.endswith("Messages that were not settled redeliver on the next run.")


def test_tracker_fatal_errors_are_mapped_not_counted():
    from azure.servicebus.exceptions import MessagingEntityNotFoundError, SessionLockLostError

    tracker = RecoveryTracker(entity_path="t/Subscriptions/s")
    with pytest.raises(UserException, match="was not found"):
        tracker.failure(MessagingEntityNotFoundError(message="missing"))
    assert tracker.count == 0
    tracker.failure(SessionLockLostError())  # a lapsed session lock is recycled, not fatal
    assert tracker.count == 1
