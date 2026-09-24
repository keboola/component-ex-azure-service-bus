import pytest
from azure.servicebus import ServiceBusReceiveMode
from keboola.component.exceptions import UserException

from client import ServiceBusConnector
from configuration import AuthConfiguration, BodyFormat, EntityType, SettlementMode, SubQueue, UnreadablePolicy
from entity import EntityRef
from settlement import BatchProcessor, UnreadableAction, UnreadableHandler, make_settler
from state import PendingSetBuilder
from stats import RunStats
from tests.fakes.recording import RecordingSink

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"
Q = EntityRef(EntityType.QUEUE, "q", None, None, SubQueue.NONE)


def receiver(broker, mode=ServiceBusReceiveMode.PEEK_LOCK, sub_queue=None):
    connector = ServiceBusConnector(AuthConfiguration(**{"#connection_string": SAS}), "kbc-test")
    client = connector.receive_client()
    return client.get_queue_receiver("q", receive_mode=mode, sub_queue=sub_queue, prefetch_count=1, keep_alive=0)


def processor(broker, mode, *, policy=UnreadablePolicy.DEAD_LETTER, pending=None, fmt=BodyFormat.TEXT, entity=Q):
    stats = RunStats(mode=mode.value)
    sink = RecordingSink()
    proc = BatchProcessor(
        entity=entity,
        mode=mode,
        body_format=fmt,
        sink=sink,
        registry=None,
        settler=make_settler(mode, pending=pending, entity=entity, stats=stats),
        unreadable=UnreadableHandler(policy=policy, mode=mode, is_sub_queue=entity.is_sub_queue, stats=stats),
        stats=stats,
        clock=broker.clock.now,
    )
    return proc, sink, stats


def test_complete_writes_before_settling(broker):
    q = broker.add_queue("q")
    seqs = [q.send(b"a"), q.send(b"b")]
    proc, sink, stats = processor(broker, SettlementMode.COMPLETE)
    with receiver(broker) as r:
        result = proc.process(r, r.receive_messages(max_message_count=10), generation=0)
    assert result.written == 2 and [row["body"] for row in sink.rows] == ["a", "b"]
    assert all(q.state_of(s) is None for s in seqs) and stats.completed == 2


def test_defer_adds_to_pending(broker):
    q = broker.add_queue("q")
    seq = q.send(b"a")
    pending = PendingSetBuilder()
    proc, _, stats = processor(broker, SettlementMode.DEFER_COMMIT, pending=pending)
    with receiver(broker) as r:
        proc.process(r, r.receive_messages(), generation=0)
    assert q.state_of(seq) == "DEFERRED" and pending.count == 1 and stats.deferred == 1


def test_lock_lost_is_counted_not_fatal(broker):
    q = broker.add_queue("q")
    q.send(b"a")
    proc, _, stats = processor(broker, SettlementMode.COMPLETE)
    with receiver(broker) as r:
        msgs = r.receive_messages()
        broker.clock.advance(120)
        proc.process(r, msgs, generation=0)
    assert stats.settlement_failures == 1


def test_lock_renewed_near_expiry(broker):
    q = broker.add_queue("q", lock_seconds=60)
    seq = q.send(b"a")
    proc, _, stats = processor(broker, SettlementMode.COMPLETE)
    with receiver(broker) as r:
        msgs = r.receive_messages()
        broker.clock.advance(55)
        proc.process(r, msgs, generation=0)
    assert q.state_of(seq) is None and stats.settlement_failures == 0


def test_unreadable_retry_then_dead_letter(broker):
    q = broker.add_queue("q")
    seq = q.send(b"a")
    broker.inject_body_error(seq, times=2)
    proc, sink, stats = processor(broker, SettlementMode.COMPLETE)
    with receiver(broker) as r:
        first = proc.process(r, r.receive_messages(), generation=0)
        assert first.needs_recycle and q.state_of(seq) == "ACTIVE" and sink.rows == []
        second = proc.process(r, r.receive_messages(), generation=1)
    assert not second.needs_recycle and q.dead_letter.state_of(seq) == "ACTIVE"
    assert stats.unreadable["UnreadableBody:dead_lettered"] == 1


def test_flatten_without_registry_rejected(broker):
    with pytest.raises(ValueError, match="registry"):
        processor(broker, SettlementMode.COMPLETE, fmt=BodyFormat.JSON_FLATTEN)


def test_not_json_dead_lettered_without_retry(broker):
    from body import FlattenRegistry
    from columns import metadata_column_names

    q = broker.add_queue("q")
    seq = q.send(b"not json")
    stats = RunStats(mode="complete")
    proc = BatchProcessor(
        entity=Q,
        mode=SettlementMode.COMPLETE,
        body_format=BodyFormat.JSON_FLATTEN,
        sink=RecordingSink(),
        registry=FlattenRegistry([], reserved=set(metadata_column_names())),
        settler=make_settler(SettlementMode.COMPLETE, pending=None, entity=Q, stats=stats),
        unreadable=UnreadableHandler(
            policy=UnreadablePolicy.DEAD_LETTER, mode=SettlementMode.COMPLETE, is_sub_queue=False, stats=stats
        ),
        stats=stats,
        clock=broker.clock.now,
    )
    with receiver(broker) as r:
        result = proc.process(r, r.receive_messages(), generation=0)
    assert not result.needs_recycle and q.dead_letter.state_of(seq) == "ACTIVE"
    assert stats.unreadable["NotJson:dead_lettered"] == 1


def test_fail_policy_writes_rest_of_batch_first_c3(broker):
    q = broker.add_queue("q")
    seqs = [q.send(b"a"), q.send(b"b"), q.send(b"c")]
    broker.inject_body_error(seqs[1], times=5)
    proc, sink, _ = processor(broker, SettlementMode.RECEIVE_AND_DELETE, policy=UnreadablePolicy.FAIL)
    with (
        receiver(broker, mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE) as r,
        pytest.raises(UserException, match="unreadable"),
    ):
        proc.process(r, r.receive_messages(max_message_count=10), generation=0)
    assert [row["body"] for row in sink.rows] == ["a", "c"]  # written before the raise


def test_fail_policy_c1_settles_good_rows_and_leaves_the_bad_one(broker):
    q = broker.add_queue("q")
    seqs = [q.send(b"a"), q.send(b"b")]
    broker.inject_body_error(seqs[1], times=5)
    proc, sink, stats = processor(broker, SettlementMode.COMPLETE, policy=UnreadablePolicy.FAIL)
    stats.unreadable_recycles = 50  # no retry: go straight to the policy
    with receiver(broker) as r, pytest.raises(UserException):
        proc.process(r, r.receive_messages(max_message_count=10), generation=0)
    assert q.state_of(seqs[0]) is None and q.state_of(seqs[1]) == "ACTIVE" and len(sink.rows) == 1


def test_flatten_cap_writes_batch_then_raises_c3(broker, monkeypatch):
    import body as body_mod
    from body import FlattenRegistry
    from columns import metadata_column_names

    monkeypatch.setattr(body_mod, "MAX_FLATTEN_COLUMNS", 1)
    q = broker.add_queue("q")
    q.send(b'{"a":1}')
    q.send(b'{"b":2}')
    stats = RunStats(mode="receive_and_delete")
    sink = RecordingSink()
    mode = SettlementMode.RECEIVE_AND_DELETE
    proc = BatchProcessor(
        entity=Q,
        mode=mode,
        body_format=BodyFormat.JSON_FLATTEN,
        sink=sink,
        registry=FlattenRegistry([], reserved=set(metadata_column_names())),
        settler=make_settler(mode, pending=None, entity=Q, stats=stats),
        unreadable=UnreadableHandler(policy=UnreadablePolicy.DEAD_LETTER, mode=mode, is_sub_queue=False, stats=stats),
        stats=stats,
        clock=broker.clock.now,
    )
    with (
        receiver(broker, mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE) as r,
        pytest.raises(UserException, match="distinct JSON keys"),
    ):
        proc.process(r, r.receive_messages(max_message_count=10), generation=0)
    assert len(sink.rows) == 2  # both deleted messages reached the output


def test_dead_letter_degrades_to_leave_on_sub_queue(broker):
    stats = RunStats(mode="complete")
    handler = UnreadableHandler(
        policy=UnreadablePolicy.DEAD_LETTER, mode=SettlementMode.COMPLETE, is_sub_queue=True, stats=stats
    )
    from body import NotJsonError
    from tests.fakes.broker import make_message

    action = handler.handle(receiver=None, message=make_message(b"x"), error=NotJsonError("x"), generation=0)
    assert action is UnreadableAction.DISPOSED and stats.unreadable["NotJson:left"] == 1
    assert "dead_letter_on_sub_queue" in stats.warnings


def test_abort_share():
    stats = RunStats(mode="complete", received=50)
    stats.unreadable["UnreadableBody:dead_lettered"] = 10
    handler = UnreadableHandler(
        policy=UnreadablePolicy.DEAD_LETTER, mode=SettlementMode.COMPLETE, is_sub_queue=False, stats=stats
    )
    with pytest.raises(UserException):
        handler.check_abort_share()


def test_complete_settler_arms_before_first_complete(broker):
    q = broker.add_queue("q")
    seqs = [q.send(b"a"), q.send(b"b")]
    events: list[str] = []
    stats = RunStats(mode="complete")
    settler = make_settler(
        SettlementMode.COMPLETE,
        pending=None,
        entity=Q,
        stats=stats,
        arm=lambda: events.append(f"arm:{q.state_of(seqs[0])}"),
    )
    proc = BatchProcessor(
        entity=Q,
        mode=SettlementMode.COMPLETE,
        body_format=BodyFormat.TEXT,
        sink=RecordingSink(),
        registry=None,
        settler=settler,
        unreadable=UnreadableHandler(
            policy=UnreadablePolicy.DEAD_LETTER, mode=SettlementMode.COMPLETE, is_sub_queue=False, stats=stats
        ),
        stats=stats,
        clock=broker.clock.now,
    )
    with receiver(broker) as r:
        proc.process(r, r.receive_messages(max_message_count=10), generation=0)
    assert events == ["arm:ACTIVE"]  # armed once, before anything was deleted


@pytest.mark.parametrize("mode", [SettlementMode.DEFER_COMMIT, SettlementMode.RECEIVE_AND_DELETE, SettlementMode.PEEK])
def test_other_settlers_never_arm(mode):
    calls: list[int] = []
    make_settler(
        mode, pending=PendingSetBuilder(), entity=Q, stats=RunStats(mode=mode.value), arm=lambda: calls.append(1)
    )
    assert calls == []


def test_retry_budget_exhausted_goes_to_disposition(broker):
    q = broker.add_queue("q")
    seq = q.send(b"a")
    broker.inject_body_error(seq, times=1)
    proc, _, stats = processor(broker, SettlementMode.COMPLETE)
    stats.unreadable_recycles = 50
    with receiver(broker) as r:
        result = proc.process(r, r.receive_messages(), generation=0)
    assert not result.needs_recycle and result.progressed and q.dead_letter.state_of(seq) == "ACTIVE"
    assert "unreadable_retry_budget" in stats.warnings


def test_disposition_counts_as_progress(broker):
    q = broker.add_queue("q")
    seq = q.send(b"not json")
    from body import FlattenRegistry
    from columns import metadata_column_names

    stats = RunStats(mode="complete")
    proc = BatchProcessor(
        entity=Q,
        mode=SettlementMode.COMPLETE,
        body_format=BodyFormat.JSON_FLATTEN,
        sink=RecordingSink(),
        registry=FlattenRegistry([], reserved=set(metadata_column_names())),
        settler=make_settler(SettlementMode.COMPLETE, pending=None, entity=Q, stats=stats),
        unreadable=UnreadableHandler(
            policy=UnreadablePolicy.DEAD_LETTER, mode=SettlementMode.COMPLETE, is_sub_queue=False, stats=stats
        ),
        stats=stats,
        clock=broker.clock.now,
    )
    with receiver(broker) as r:
        result = proc.process(r, r.receive_messages(), generation=0)
    assert result.written == 0 and result.progressed and q.state_of(seq) is None


# --- edge rules beyond the brief's cases (spec §6.6) -------------------------------------------------


def _handler(mode, policy=UnreadablePolicy.DEAD_LETTER, *, is_sub_queue=False):
    stats = RunStats(mode=mode.value)
    return UnreadableHandler(policy=policy, mode=mode, is_sub_queue=is_sub_queue, stats=stats), stats


def test_left_message_redelivered_is_left_again_and_counted_once():
    from body import BodyDecodeError
    from tests.fakes.broker import make_message

    handler, stats = _handler(SettlementMode.COMPLETE, UnreadablePolicy.LEAVE)
    message = make_message(b"x", sequence_number=7)
    stats.unreadable_recycles = 50
    assert handler.handle(None, message, BodyDecodeError("x"), generation=0) is UnreadableAction.DISPOSED
    assert handler.handle(None, message, BodyDecodeError("x"), generation=3) is UnreadableAction.DISPOSED
    assert stats.unreadable == {"UnreadableBody:left": 1}


def test_peek_retry_settles_nothing_then_skips_on_a_new_generation():
    from body import BodyDecodeError
    from tests.fakes.broker import make_message

    handler, stats = _handler(SettlementMode.PEEK)
    message = make_message(b"x", sequence_number=3)
    # receiver=None: any settle attempt would raise
    assert handler.handle(None, message, BodyDecodeError("x"), generation=0) is UnreadableAction.RETRY
    assert handler.handle(None, message, BodyDecodeError("x"), generation=0) is UnreadableAction.RETRY
    assert handler.handle(None, message, BodyDecodeError("x"), generation=1) is UnreadableAction.SKIPPED
    assert stats.unreadable["UnreadableBody:retry"] == 2 and stats.unreadable["UnreadableBody:skipped"] == 1
    assert "unreadable_UnreadableBody_skipped" in stats.warnings


def test_retries_disabled_first_failure_is_final_without_budget_warning():
    from body import BodyDecodeError
    from tests.fakes.broker import make_message

    handler, stats = _handler(SettlementMode.PEEK)
    handler.retries_enabled = False
    action = handler.handle(None, make_message(b"x"), BodyDecodeError("x"), generation=0)
    assert action is UnreadableAction.SKIPPED and "unreadable_retry_budget" not in stats.warnings


def test_fail_policy_records_the_first_failure_only_and_clears_it():
    from body import NotJsonError
    from tests.fakes.broker import make_message

    handler, stats = _handler(SettlementMode.RECEIVE_AND_DELETE, UnreadablePolicy.FAIL)
    for seq in (4, 9):
        action = handler.handle(
            None, make_message(b"x", sequence_number=seq, message_id=f"m{seq}"), NotJsonError("x"), 0
        )
        assert action is UnreadableAction.FAILED
    with pytest.raises(UserException) as caught:
        handler.raise_pending()
    assert str(caught.value) == (
        "Message 4 (message id m4) has an unreadable body (NotJson) and the unreadable_body policy is 'fail'."
    )
    handler.raise_pending()  # cleared: nothing left to raise
    assert stats.unreadable["NotJson:failed"] == 2


def test_oversized_body_unmapped_cell_takes_the_unreadable_policy(broker, monkeypatch):
    import body as body_mod
    from body import FlattenRegistry
    from columns import metadata_column_names

    monkeypatch.setattr(body_mod, "CELL_LIMIT_BYTES", 30)  # each field fits, their body_unmapped cell does not
    q = broker.add_queue("q")
    seq = q.send(b'{"a":"xxxxxxxxxx","b":"yyyyyyyyyy"}')
    stats = RunStats(mode="complete")
    sink = RecordingSink()
    proc = BatchProcessor(
        entity=Q,
        mode=SettlementMode.COMPLETE,
        body_format=BodyFormat.JSON_FLATTEN,
        sink=sink,
        registry=FlattenRegistry([], reserved=set(metadata_column_names())),
        settler=make_settler(SettlementMode.COMPLETE, pending=None, entity=Q, stats=stats),
        unreadable=UnreadableHandler(
            policy=UnreadablePolicy.DEAD_LETTER, mode=SettlementMode.COMPLETE, is_sub_queue=False, stats=stats
        ),
        stats=stats,
        clock=broker.clock.now,
    )
    with receiver(broker) as r:
        result = proc.process(r, r.receive_messages(), generation=0)
    assert sink.rows == [] and result.written == 0 and q.dead_letter.state_of(seq) == "ACTIVE"
    assert stats.unreadable["BodyTooLarge:dead_lettered"] == 1


def test_dead_letter_description_is_the_underlying_exception_class(broker):
    from azure.servicebus import ServiceBusSubQueue

    q = broker.add_queue("q")
    seq = q.send(b"a")
    broker.inject_body_error(seq, times=1)
    proc, _, stats = processor(broker, SettlementMode.COMPLETE)
    stats.unreadable_recycles = 50
    with receiver(broker) as r:
        proc.process(r, r.receive_messages(), generation=0)
    with receiver(broker, sub_queue=ServiceBusSubQueue.DEAD_LETTER) as dlq:
        (dead,) = dlq.peek_messages(10)
    assert dead.dead_letter_reason == "UnreadableBody" and dead.dead_letter_error_description == "TypeError"


def test_c3_counts_deleted_and_never_renews(broker):
    q = broker.add_queue("q")
    q.send(b"a")
    q.send(b"b")
    proc, _, stats = processor(broker, SettlementMode.RECEIVE_AND_DELETE)
    with receiver(broker, mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE) as r:
        result = proc.process(r, r.receive_messages(max_message_count=10), generation=0)
        operations = [op for op, _ in r.operations]
    assert result.written == 2 and stats.deleted_on_receive == 2 and stats.written == 2
    assert "renew_message_lock" not in operations and stats.settlement_failures == 0


def test_abort_share_needs_both_share_and_minimum():
    handler, stats = _handler(SettlementMode.COMPLETE)
    stats.received = 20
    stats.unreadable["NotJson:dead_lettered"] = 9  # 45 % but fewer than 10
    stats.unreadable["UnreadableBody:retry"] = 30  # retries are not dispositions
    handler.check_abort_share()
    stats.received = 200
    stats.unreadable["NotJson:dead_lettered"] = 20  # 10 % exactly: not more than the share
    handler.check_abort_share()
