from datetime import UTC, datetime, timedelta

import pytest
from azure.core.exceptions import ClientAuthenticationError, HttpResponseError, ResourceNotFoundError
from azure.servicebus import NEXT_AVAILABLE_SESSION, ServiceBusMessageState, ServiceBusReceiveMode, ServiceBusSubQueue
from azure.servicebus.amqp import AmqpMessageBodyType
from azure.servicebus.exceptions import (
    MessageAlreadySettled,
    MessageLockLostError,
    MessageNotFoundError,
    MessagingEntityNotFoundError,
    OperationTimeoutError,
    ServiceBusAuthenticationError,
    ServiceBusError,
    ServiceBusServerBusyError,
    SessionCannotBeLockedError,
    SessionLockLostError,
)

import client as client_mod
from tests.fakes.broker import FakeCredential, make_message

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"
RAD = ServiceBusReceiveMode.RECEIVE_AND_DELETE
DLQ = ServiceBusSubQueue.DEAD_LETTER


def sas_client():
    return client_mod.ServiceBusClient.from_connection_string(
        conn_str="Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"
    )


def sp_client(**kwargs):
    credential = client_mod.ClientSecretCredential("t", "c", "x")
    return client_mod.ServiceBusClient(
        fully_qualified_namespace="ns.servicebus.windows.net", credential=credential, **kwargs
    )


# --- brief self-tests (Task 4, Step 1) ------------------------------------------------------------


def test_peek_lock_receive_complete(broker):
    q = broker.add_queue("q")
    s1, s2 = q.send(b"a"), q.send(b"b")
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        msgs = r.receive_messages(max_message_count=10)
        assert [m.sequence_number for m in msgs] == [s1, s2]
        assert b"".join(msgs[0].body) == b"a"
        r.complete_message(msgs[0])
    assert q.state_of(s1) is None and q.state_of(s2) == "ACTIVE"


def test_lock_expiry_redelivers_with_delivery_count(broker):
    q = broker.add_queue("q", lock_seconds=60)
    seq = q.send(b"a")
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        m = r.receive_messages()[0]
        broker.clock.advance(61)
        with pytest.raises(MessageLockLostError):
            r.complete_message(m)
        again = r.receive_messages()[0]
    assert again.sequence_number == seq and again.delivery_count == 1


def test_peek_shows_deferred_and_locked_without_locking(broker):
    q = broker.add_queue("q")
    a, b = q.send(b"a"), q.send(b"b")
    q.defer_existing(a)
    q.lock_existing(b, 60)
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        peeked = r.peek_messages(10, sequence_number=1)
    assert [(m.sequence_number, m.state.name, m.delivery_count) for m in peeked] == [
        (a, "DEFERRED", 0),
        (b, "ACTIVE", 0),
    ]


def test_deferred_receive_rules(broker):
    q = broker.add_queue("q")
    seqs = [q.send(b"x") for _ in range(3)]
    for s in seqs:
        q.defer_existing(s)
    rad = ServiceBusReceiveMode.RECEIVE_AND_DELETE
    with sas_client().get_queue_receiver("q", receive_mode=rad, prefetch_count=1, keep_alive=0) as r:
        with pytest.raises(MessageNotFoundError):
            r.receive_deferred_messages([seqs[0], 999])
        assert q.state_of(seqs[0]) == "DEFERRED"  # all-or-nothing
        assert len(r.receive_deferred_messages(seqs)) == 3
    assert q.sequence_numbers() == []


def test_deferred_receive_over_250_rejected(broker):
    q = broker.add_queue("q")
    seqs = [q.send(b"x") for _ in range(251)]
    for s in seqs:
        q.defer_existing(s)
    rad = ServiceBusReceiveMode.RECEIVE_AND_DELETE
    with (
        sas_client().get_queue_receiver("q", receive_mode=rad, prefetch_count=1, keep_alive=0) as r,
        pytest.raises(ServiceBusError, match="250"),
    ):
        r.receive_deferred_messages(seqs)
    assert len(q.sequence_numbers()) == 251


def test_partitioned_mixed_partitions_rejected(broker):
    q = broker.add_queue("p", partitioned=True)
    a, b = q.send(b"x", partition=0), q.send(b"y", partition=1)
    q.defer_existing(a)
    q.defer_existing(b)
    rad = ServiceBusReceiveMode.RECEIVE_AND_DELETE
    with (
        sas_client().get_queue_receiver("p", receive_mode=rad, prefetch_count=1, keep_alive=0) as r,
        pytest.raises(ServiceBusError, match="different partitions"),
    ):
        r.receive_deferred_messages([a, b])
    assert a >> 48 != b >> 48


def test_sessions_next_available_and_timeout(broker):
    q = broker.add_queue("s", sessions=True)
    q.send(b"a", session_id="A")
    with sas_client().get_queue_receiver("s", session_id=NEXT_AVAILABLE_SESSION, prefetch_count=1, keep_alive=0) as r:
        assert r.session.session_id == "A"
        r.complete_message(r.receive_messages()[0])
    with (
        pytest.raises(OperationTimeoutError),
        sas_client().get_queue_receiver("s", session_id=NEXT_AVAILABLE_SESSION, prefetch_count=1, keep_alive=0) as r,
    ):
        r.receive_messages()


def test_dead_letter_moves_and_is_ignored_on_dlq(broker):
    q = broker.add_queue("q")
    seq = q.send(b"a")
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        r.dead_letter_message(r.receive_messages()[0], reason="UnreadableBody", error_description="TypeError")
    assert q.dead_letter.state_of(seq) == "ACTIVE"
    from azure.servicebus import ServiceBusSubQueue

    with sas_client().get_queue_receiver(
        "q", sub_queue=ServiceBusSubQueue.DEAD_LETTER, prefetch_count=1, keep_alive=0
    ) as r:
        m = r.receive_messages()[0]
        assert m.dead_letter_reason == "UnreadableBody"
        r.dead_letter_message(m, reason="again")  # silently ignored, like 7.14.3
    assert q.dead_letter.state_of(seq) == "ACTIVE"


def test_injected_receive_error(broker):
    broker.add_queue("q").send(b"a")
    broker.inject_receive_error(TypeError("'NoneType' object is not callable"), on_call=1)
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        with pytest.raises(TypeError):
            r.receive_messages()
        assert len(r.receive_messages()) == 1


def test_real_parser_rejects_garbage(broker):
    with pytest.raises(ValueError):
        client_mod.ServiceBusClient.from_connection_string(conn_str="garbage")


def test_management_denied(broker):
    from azure.core.exceptions import ClientAuthenticationError

    broker.add_queue("q")
    broker.management_denied = True
    admin = client_mod.ServiceBusAdministrationClient.from_connection_string(
        "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"
    )
    with pytest.raises(ClientAuthenticationError):
        list(admin.list_queues())


# --- further self-tests: the remaining rules of the semantics table -------------------------------


def test_partitioned_sequence_layout_and_round_robin_receive(broker):
    p = broker.add_queue("p", partitioned=True)
    a1, a2, b1 = p.send(b"a1", partition=0), p.send(b"a2", partition=0), p.send(b"b1", partition=3)
    assert (a1, a2, b1) == ((51 << 48) | 1, (51 << 48) | 2, (54 << 48) | 1)  # low bits count per partition
    with sas_client().get_queue_receiver("p", prefetch_count=1, keep_alive=0) as r:
        received = [m.sequence_number for m in r.receive_messages(max_message_count=10)]
    assert received == [a1, b1, a2]  # partitions served round-robin, not in enqueue order
    with pytest.raises(ValueError):
        broker.add_queue("plain").send(b"x", partition=1)


def test_receive_and_delete_removes_immediately(broker):
    q = broker.add_queue("q")
    seq = q.send(b"a")
    with sas_client().get_queue_receiver("q", receive_mode=RAD, prefetch_count=1, keep_alive=0) as r:
        m = r.receive_messages()[0]
        assert q.state_of(seq) is None
        assert m.lock_token is None and m.locked_until_utc is None
        with pytest.raises(ValueError, match="RECEIVE_AND_DELETE"):
            r.complete_message(m)


def test_abandon_increments_delivery_count_and_releases_at_once(broker):
    q = broker.add_queue("q")
    seq = q.send(b"a")
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        first = r.receive_messages()[0]
        r.abandon_message(first)
        assert q.delivery_count(seq) == 1
        again = r.receive_messages()[0]
        assert again.sequence_number == seq and again.delivery_count == 1
        with pytest.raises(MessageAlreadySettled):
            r.complete_message(first)


def test_max_delivery_count_dead_letters(broker):
    q = broker.add_queue("q", max_delivery_count=2)
    seq = q.send(b"a")
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        r.abandon_message(r.receive_messages()[0])
        r.abandon_message(r.receive_messages()[0])
        assert r.receive_messages() == []
    assert q.state_of(seq) is None
    with sas_client().get_queue_receiver("q", sub_queue=DLQ, prefetch_count=1, keep_alive=0) as r:
        m = r.receive_messages()[0]
    assert m.dead_letter_reason == "MaxDeliveryCountExceeded" and m.delivery_count == 2


def test_peek_cursor_mode_explicit_start_and_page_cap(broker):
    q = broker.add_queue("q")
    seqs = [q.send(b"x") for _ in range(260)]
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        assert [m.sequence_number for m in r.peek_messages(2)] == seqs[:2]  # sequence_number=0: from 1
        assert [m.sequence_number for m in r.peek_messages(2)] == seqs[2:4]  # then last peeked + 1
        assert [m.sequence_number for m in r.peek_messages(3, sequence_number=seqs[100])] == seqs[100:103]
        assert len(r.peek_messages(1000, sequence_number=1)) == 250
        peeked = r.peek_messages(1, sequence_number=1)[0]
        with pytest.raises(ValueError, match="peeked"):
            r.complete_message(peeked)
    assert all(q.state_of(s) == "ACTIVE" and q.delivery_count(s) == 0 for s in seqs)
    assert broker.calls.count(("peek_messages", "q")) == 5


def test_scheduled_messages(broker):
    due = broker.clock.now() + timedelta(hours=1)
    sub = broker.add_subscription("t", "s")
    sub.send(b"s", scheduled_at=due)
    q = broker.add_queue("q")
    seq = q.send(b"s", scheduled_at=due)
    assert q.state_of(seq) == "SCHEDULED"
    with sas_client().get_subscription_receiver("t", "s", prefetch_count=1, keep_alive=0) as r:
        assert r.peek_messages(10) == []  # scheduled topic messages wait at the topic
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        [peeked] = r.peek_messages(10)
        assert peeked.state is ServiceBusMessageState.SCHEDULED
        assert peeked.enqueued_time_utc == due and peeked.scheduled_enqueue_time_utc == due
        assert r.receive_messages() == []
        broker.clock.advance(3600)
        assert r.receive_messages()[0].sequence_number == seq


def test_expired_messages_are_peeked_but_never_received(broker):
    q = broker.add_queue("q")
    q.send(b"e", ttl_seconds=1)
    broker.clock.advance(5)
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        assert r.receive_messages() == []
        [peeked] = r.peek_messages(10)
    assert peeked.state is ServiceBusMessageState.ACTIVE and peeked.expires_at_utc < broker.clock.now()
    assert peeked.time_to_live == timedelta(seconds=1)


def test_entity_path_mismatch_is_value_error(broker):
    broker.add_queue("orders")
    broker.add_subscription("t", "s")
    scoped = client_mod.ServiceBusClient.from_connection_string(conn_str=SAS + ";EntityPath=other")
    with pytest.raises(ValueError, match="EntityPath"):
        scoped.get_queue_receiver("orders", prefetch_count=1, keep_alive=0)
    with pytest.raises(ValueError, match="EntityPath"):
        scoped.get_subscription_receiver("t", "s", prefetch_count=1, keep_alive=0)
    assert broker.receivers == []


def test_unknown_entities(broker):
    broker.add_subscription("t", "s")
    missing_subscription = sas_client().get_subscription_receiver("t", "missing", prefetch_count=1, keep_alive=0)
    with pytest.raises(MessagingEntityNotFoundError):
        missing_subscription.__enter__()
    with pytest.raises(ServiceBusAuthenticationError):  # a missing queue via SAS reads as unauthorized
        sas_client().get_queue_receiver("nope", prefetch_count=1, keep_alive=0).receive_messages()
    with pytest.raises(MessagingEntityNotFoundError):
        sp_client().get_queue_receiver("nope", prefetch_count=1, keep_alive=0).peek_messages()


def test_auth_failure_raises_when_the_receiver_opens(broker):
    broker.add_queue("q")
    broker.auth_failure = True
    receiver = sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0)  # no connection yet
    with pytest.raises(ServiceBusAuthenticationError):
        receiver.receive_messages()
    with pytest.raises(ServiceBusAuthenticationError):
        receiver.__enter__()  # `with receiver:` opens the link, so the error surfaces there too


def test_non_session_receiver_on_session_entity(broker):
    broker.add_queue("s", sessions=True).send(b"a", session_id="A")
    receiver = sas_client().get_queue_receiver("s", prefetch_count=1, keep_alive=0)
    with pytest.raises(ServiceBusError, match="requires sessions"):
        receiver.receive_messages()


def test_session_rules(broker):
    s = broker.add_queue("s", sessions=True, lock_seconds=30)
    s.send(b"b", session_id="B")
    a = s.send(b"a", session_id="A")
    d = s.send(b"d", session_id="D")
    s.defer_existing(d)
    holder = sas_client().get_queue_receiver("s", session_id=NEXT_AVAILABLE_SESSION, prefetch_count=1, keep_alive=0)
    assert holder.session.session_id is NEXT_AVAILABLE_SESSION  # resolved when the receiver opens
    with holder:
        assert holder.session.session_id == "A"
        [m] = holder.receive_messages(max_message_count=10)
        assert m.sequence_number == a and m.session_id == "A" and m.locked_until_utc is None
        with pytest.raises(SessionCannotBeLockedError):
            sas_client().get_queue_receiver("s", session_id="A", prefetch_count=1, keep_alive=0).receive_messages()
        with sas_client().get_queue_receiver(
            "s", session_id=NEXT_AVAILABLE_SESSION, prefetch_count=1, keep_alive=0
        ) as other:
            assert other.session.session_id == "B"  # A is held; D holds only a deferred message
            assert [x.session_id for x in other.peek_messages(10)] == ["B"]  # a session receiver sees its session
        broker.clock.advance(25)
        before = holder.session.locked_until_utc
        assert holder.session.renew_lock() > before
        with pytest.raises(TypeError):
            holder.renew_message_lock(m)
    assert s.delivery_count(a) == 1  # closing the session receiver released its unsettled message
    assert ("session_renew_lock", "s") in broker.calls
    with sas_client().get_queue_receiver("s", session_id="D", prefetch_count=1, keep_alive=0) as r:
        assert [x.state for x in r.peek_messages(10)] == [ServiceBusMessageState.DEFERRED]
    with sas_client().get_queue_receiver("s", session_id="A", prefetch_count=1, keep_alive=0) as r:
        m = r.receive_messages()[0]
        broker.clock.advance(31)
        with pytest.raises(SessionLockLostError):
            r.complete_message(m)


def test_session_subscription_without_session_id_goes_to_dlq(broker):
    sub = broker.add_subscription("t", "s", sessions=True)
    seq = sub.send(b"x")
    assert sub.state_of(seq) is None and sub.dead_letter.state_of(seq) == "ACTIVE"


def test_message_locks_outlive_a_closed_receiver(broker):
    q = broker.add_queue("q", lock_seconds=60)
    q.send(b"a")
    client = sas_client()
    first = client.get_queue_receiver("q", prefetch_count=1, keep_alive=0)
    m = first.receive_messages()[0]
    first.close()
    with pytest.raises(ValueError, match="shutdown"):
        first.complete_message(m)
    with client.get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        assert r.receive_messages() == []  # 7.14.3 does not release locks on close
        broker.clock.advance(61)
        assert r.receive_messages()[0].delivery_count == 1


def test_renew_message_lock(broker):
    q = broker.add_queue("q", lock_seconds=60)
    seq = q.send(b"a")
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        m = r.receive_messages()[0]
        broker.clock.advance(50)
        assert r.renew_message_lock(m) == broker.clock.now() + timedelta(seconds=60) == m.locked_until_utc
        broker.clock.advance(50)
        r.complete_message(m)
        assert q.state_of(seq) is None
        late = q.send(b"b")
        m = r.receive_messages()[0]
        broker.clock.advance(61)
        with pytest.raises(MessageLockLostError):
            r.renew_message_lock(m)
    assert q.delivery_count(late) == 1


def test_peek_lock_deferred_receive_and_sub_queue_scope(broker):
    q = broker.add_queue("q")
    seq = q.send(b"o")
    q.defer_existing(seq)
    dl = q.send(b"d")
    q.dead_letter_existing(dl, "r", "d")
    q.dead_letter.defer_existing(dl)
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        [m] = r.receive_deferred_messages([seq])
        assert m.state is ServiceBusMessageState.DEFERRED and m.delivery_count == 0
        assert q.delivery_count(seq) == 1 and q.state_of(seq) == "DEFERRED"
        assert r.receive_messages() == []  # deferred messages are invisible to receive
        r.defer_message(m)
        with pytest.raises(MessageNotFoundError):  # a DLQ sequence number via the main-queue receiver
            r.receive_deferred_messages([dl])
    assert q.state_of(seq) == "DEFERRED"
    with sas_client().get_queue_receiver("q", sub_queue=DLQ, receive_mode=RAD, prefetch_count=1, keep_alive=0) as r:
        assert [x.sequence_number for x in r.receive_deferred_messages(dl)] == [dl]
    assert q.dead_letter.sequence_numbers() == []


def test_expired_deferred_message_is_gone_when_received_by_sequence_number(broker):
    q = broker.add_queue("q")
    seq = q.send(b"o", ttl_seconds=10)
    q.defer_existing(seq)
    broker.clock.advance(45)
    assert q.state_of(seq) == "DEFERRED"  # deferred messages expire only when received by number
    with (
        sas_client().get_queue_receiver("q", receive_mode=RAD, prefetch_count=1, keep_alive=0) as r,
        pytest.raises(MessageNotFoundError),
    ):
        r.receive_deferred_messages([seq])
    assert q.state_of(seq) is None


def test_dead_letter_keeps_number_and_enqueue_time(broker):
    q = broker.add_queue("q")
    enqueued = broker.clock.now()
    seq = q.send(b"a", application_properties={"k": "v"})
    broker.clock.advance(5)
    q.dead_letter_existing(seq, "R", "D")
    with sas_client().get_queue_receiver("q", sub_queue=DLQ, prefetch_count=1, keep_alive=0) as r:
        m = r.receive_messages()[0]
    assert m.sequence_number == seq and m.enqueued_time_utc == enqueued
    assert (m.dead_letter_reason, m.dead_letter_error_description) == ("R", "D")
    assert m.application_properties == {b"k": b"v", b"DeadLetterReason": b"R", b"DeadLetterErrorDescription": b"D"}
    tdlq = q.transfer_dead_letter.send(b"t")
    assert broker.entity("q/$Transfer/$DeadLetterQueue").state_of(tdlq) == "ACTIVE"


def test_injected_peek_error(broker):
    broker.add_queue("q").send(b"a")
    broker.inject_peek_error(ServiceBusError(message="link detached"), on_call=2)
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        assert len(r.peek_messages(10, sequence_number=1)) == 1
        with pytest.raises(ServiceBusError, match="link detached"):
            r.peek_messages(10, sequence_number=1)
        assert len(r.peek_messages(10, sequence_number=1)) == 1


def test_receive_injections_count_across_receivers(broker):
    q = broker.add_queue("q")
    for _ in range(3):
        q.send(b"a")
    broker.inject_receive_error(TypeError("boom"), on_call=2)
    broker.inject_receive_error(TypeError("boom"), on_call=4)
    client = sas_client()
    with client.get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r1:
        assert len(r1.receive_messages()) == 1
        with pytest.raises(TypeError):
            r1.receive_messages()
    with client.get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r2:
        assert len(r2.receive_messages()) == 1
        with pytest.raises(TypeError):
            r2.receive_messages()
        assert len(r2.receive_messages()) == 1


def test_injected_commit_errors_hit_receive_and_delete_deferred_calls_only(broker):
    q = broker.add_queue("q")
    a, b = q.send(b"x"), q.send(b"y")
    q.defer_existing(a)
    q.defer_existing(b)
    broker.inject_commit_errors([ServiceBusServerBusyError(message="busy")])
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        assert len(r.receive_deferred_messages([a])) == 1  # PEEK_LOCK: not a commit call
    with sas_client().get_queue_receiver("q", receive_mode=RAD, prefetch_count=1, keep_alive=0) as r:
        with pytest.raises(ServiceBusServerBusyError):
            r.receive_deferred_messages([b])
        assert len(r.receive_deferred_messages([b])) == 1
    assert q.state_of(b) is None


def test_recording_of_clients_receivers_and_calls(broker):
    q = broker.add_queue("q")
    q.send(b"a")
    client = client_mod.ServiceBusClient.from_connection_string(conn_str=SAS, user_agent="ua", retry_total=0)
    with pytest.raises(TypeError, match="retry_total"):  # the real SDK signature rejects it on the receiver
        client.get_queue_receiver("q", retry_total=0)
    with client.get_queue_receiver(
        "q", prefetch_count=1, keep_alive=0, client_identifier="kbc-1", max_wait_time=5
    ) as r:
        r.complete_message(r.receive_messages(max_message_count=5, max_wait_time=2)[0])
    [recorded_client], [receiver] = broker.clients, broker.receivers  # the typed fakes behind `client` / `r`
    assert recorded_client is client and recorded_client.kwargs == {"user_agent": "ua", "retry_total": 0}
    assert receiver is r and receiver.closed and receiver.client_identifier == "kbc-1"
    assert receiver.kwargs == {"prefetch_count": 1, "keep_alive": 0, "client_identifier": "kbc-1", "max_wait_time": 5}
    assert broker.calls == [("receive_messages", "q"), ("complete_message", "q")]
    assert receiver.operations[0] == ("receive_messages", {"max_message_count": 5, "max_wait_time": 2})


def test_client_close_closes_its_receivers(broker):
    broker.add_queue("q")
    client = sas_client()
    receiver = client.get_queue_receiver("q", prefetch_count=1, keep_alive=0)
    client.close()
    assert receiver.closed
    with pytest.raises(ValueError, match="shutdown"):
        receiver.receive_messages()


def test_service_principal_client_uses_fake_credential(broker):
    broker.add_queue("q").send(b"a")
    client = sp_client(user_agent="ua")
    assert isinstance(client.credential, FakeCredential) and broker.credentials == [client.credential]
    with client.get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        assert len(r.receive_messages()) == 1
    with pytest.raises(ValueError):  # real argument validation of ClientSecretCredential
        client_mod.ClientSecretCredential("", "c", "x")


def test_body_and_property_encodings(broker):
    q = broker.add_queue("q")
    stamp = datetime(2026, 9, 23, 10, 0, tzinfo=UTC)
    q.send(
        [b"p1-", b"p2"],
        message_id="m-1",
        content_type="text/plain",
        application_properties={"k": "v", "n": 1, "ts": stamp},
    )
    q.send({"k": "v", "n": [1, "a"]}, body_type="VALUE")
    q.send([[1, "a"]], body_type="SEQUENCE")
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        data, value, sequence = r.receive_messages(max_message_count=10)
    assert data.body_type is AmqpMessageBodyType.DATA and b"".join(data.body) == b"p1-p2"
    assert b"".join(data.body) == b"p1-p2"  # a fresh generator per access
    assert data.application_properties == {b"k": b"v", b"n": 1, b"ts": 1790157600000}
    assert data.message_id == "m-1" and data.content_type == "text/plain"
    assert data.raw_amqp_message.properties.message_id == b"m-1"
    assert data.raw_amqp_message.annotations[b"x-opt-sequence-number"] == data.sequence_number
    assert data.enqueued_time_utc == broker.clock.now() and data.locked_until_utc is not None
    assert value.body_type is AmqpMessageBodyType.VALUE and value.body == {b"k": b"v", b"n": [1, b"a"]}
    assert sequence.body_type is AmqpMessageBodyType.SEQUENCE and list(sequence.body) == [[1, b"a"]]


def test_body_errors_and_standalone_messages(broker):
    q = broker.add_queue("q")
    seq = q.send(b"a")
    broker.inject_body_error(seq, times=2)
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        m = r.receive_messages()[0]
        for _ in range(2):
            with pytest.raises(TypeError, match="injected decode failure"):
                _ = m.body
        assert b"".join(m.body) == b"a"
    standalone = make_message(b"x", body_error=True, delivery_count=3, user_id=b"u", subject="s")
    with pytest.raises(TypeError):
        _ = standalone.body
    header, properties = standalone.raw_amqp_message.header, standalone.raw_amqp_message.properties
    assert header is not None and properties is not None
    assert standalone.delivery_count == 3 and header.delivery_count == 3
    assert properties.user_id == b"u" and standalone.subject == "s"
    assert make_message("text").state is ServiceBusMessageState.ACTIVE
    with pytest.raises(TypeError):
        make_message(b"x", no_such_attribute=1)


def test_management_surface(broker):
    now = broker.clock.now()
    q = broker.add_queue("q", sessions=True, lock_seconds=30, max_delivery_count=5)
    q.send(b"a", session_id="A")
    q.defer_existing(q.send(b"b", session_id="A"))
    q.send(b"c", session_id="A", scheduled_at=now + timedelta(hours=1))
    q.dead_letter_existing(q.send(b"x", session_id="A"), "r", "d")
    q.transfer_dead_letter.send(b"t")
    broker.add_subscription("t1", "s", partitioned=True).add_rule("r1", "amount > 10")
    broker.add_subscription("t2", "s")
    admin = client_mod.ServiceBusAdministrationClient.from_connection_string(SAS)
    assert [x.name for x in admin.list_queues()] == ["q"]
    assert [x.name for x in admin.list_topics()] == ["t1", "t2"]
    assert [x.name for x in admin.list_subscriptions("t1")] == ["s"]
    props = admin.get_queue("q")
    assert (props.requires_session, props.enable_partitioning, props.max_delivery_count) == (True, False, 5)
    assert props.lock_duration == timedelta(seconds=30)
    runtime = admin.get_queue_runtime_properties("q")
    counts = (
        runtime.active_message_count,
        runtime.dead_letter_message_count,
        runtime.scheduled_message_count,
        runtime.transfer_dead_letter_message_count,
    )
    assert counts == (2, 1, 1, 1)  # active counts the deferred message too [inferred]
    assert admin.get_topic("t1").enable_partitioning is True
    subscription = admin.get_subscription("t1", "s")
    assert subscription.requires_session is False and not hasattr(subscription, "enable_partitioning")
    assert admin.get_subscription_runtime_properties("t1", "s").active_message_count == 0

    def rules(topic: str) -> list[tuple[str, str | None]]:
        return [(x.name, getattr(x.filter, "sql_expression", None)) for x in admin.list_rules(topic, "s")]

    assert rules("t1") == [("r1", "amount > 10")]
    assert rules("t2") == [("$Default", "1=1")]
    with pytest.raises(ResourceNotFoundError):
        list(admin.list_subscriptions("nope"))
    with pytest.raises(ResourceNotFoundError):
        admin.get_subscription("t1", "nope")
    broker.management_error = HttpResponseError(message="boom")
    with pytest.raises(HttpResponseError):
        admin.get_queue("q")
    broker.management_error = None
    broker.management_denied = True
    with pytest.raises(ClientAuthenticationError):
        admin.get_topic("t1")
    assert ("get_queue", "q") in broker.admin_calls


# --- fix round 1 ----------------------------------------------------------------------------------


def test_dlq_view_does_not_depend_on_call_order(broker):
    q = broker.add_queue("q", max_delivery_count=1)
    seq = q.send(b"a")
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        r.receive_messages()
    broker.clock.advance(61)  # the lapse dead-letters it; nothing has looked at the main queue since
    with sas_client().get_queue_receiver("q", sub_queue=DLQ, prefetch_count=1, keep_alive=0) as r:
        [m] = r.receive_messages()
    assert m.sequence_number == seq and m.dead_letter_reason == "MaxDeliveryCountExceeded"


def test_lock_lapses_at_exactly_lock_seconds(broker):
    q = broker.add_queue("q", lock_seconds=60)
    seq = q.send(b"a")
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        m = r.receive_messages()[0]
        broker.clock.advance(60)  # the SDK: locked_until_utc <= utc_now() means lapsed
        with pytest.raises(MessageLockLostError):
            r.complete_message(m)
        again = r.receive_messages()[0]
    assert again.sequence_number == seq and again.delivery_count == 1


def test_value_bodies_are_not_shared_between_deliveries(broker):
    q = broker.add_queue("q")
    q.send({"k": "v"}, body_type="VALUE")
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        first = r.peek_messages(1, sequence_number=1)[0]
        first.body[b"k"] = b"changed"
        assert r.peek_messages(1, sequence_number=1)[0].body == {b"k": b"v"}


def test_auth_failure_hits_an_open_receiver(broker):
    q = broker.add_queue("q")
    q.send(b"a")
    q.send(b"b")
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        m = r.receive_messages()[0]
        broker.auth_failure = True
        with pytest.raises(ServiceBusAuthenticationError):
            r.receive_messages()
        with pytest.raises(ServiceBusAuthenticationError):
            r.complete_message(m)


def test_injection_waits_for_a_call_that_reaches_the_broker(broker):
    broker.add_queue("q").send(b"a")
    broker.inject_receive_error(TypeError("boom"), on_call=1)
    client = sas_client()
    closed = client.get_queue_receiver("q", prefetch_count=1, keep_alive=0)
    closed.close()
    with pytest.raises(ValueError, match="shutdown"):  # rejected client-side: not counted
        closed.receive_messages()
    broker.auth_failure = True
    receiver = client.get_queue_receiver("q", prefetch_count=1, keep_alive=0)
    with pytest.raises(ServiceBusAuthenticationError):  # the open failed: not counted
        receiver.receive_messages()
    broker.auth_failure = False
    with pytest.raises(TypeError, match="boom"):
        receiver.receive_messages()
    assert len(receiver.receive_messages()) == 1
