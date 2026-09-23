import pytest
from keboola.component.exceptions import UserException

from client import ServiceBusConnector
from configuration import AuthConfiguration, SourceConfig
from entity import EntityInfo, EntityRef, describe_entity, list_entity_names, load_entity_info, partition_of

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"


def connector(auth_type: str = "connection_string") -> ServiceBusConnector:
    if auth_type == "connection_string":
        auth = AuthConfiguration(**{"auth_type": auth_type, "#connection_string": SAS})
    else:
        auth = AuthConfiguration(
            **{
                "auth_type": auth_type,
                "tenant_id": "t",
                "client_id": "c",
                "#client_secret": "x",
                "fully_qualified_namespace": "ns.servicebus.windows.net",
            }
        )
    return ServiceBusConnector(auth, "kbc-test")


@pytest.mark.parametrize(
    "source, path, table",
    [
        ({"entity_type": "queue", "queue_name": "orders"}, "orders", "orders"),
        (
            {"entity_type": "queue", "queue_name": "orders", "sub_queue": "dead_letter"},
            "orders/$DeadLetterQueue",
            "orders_dead_letter",
        ),
        (
            {"entity_type": "queue", "queue_name": "orders", "sub_queue": "transfer_dead_letter"},
            "orders/$Transfer/$DeadLetterQueue",
            "orders_transfer_dead_letter",
        ),
        (
            {"entity_type": "subscription", "topic_name": "ev", "subscription_name": "audit"},
            "ev/Subscriptions/audit",
            "ev_audit",
        ),
        ({"entity_type": "queue", "queue_name": "_we.ird-"}, "_we.ird-", "we_ird"),
    ],
)
def test_paths_and_table_names(source, path, table):
    ref = EntityRef.from_source(SourceConfig(**source))
    assert ref.path == path and ref.default_table_name() == table
    assert EntityRef.from_dict(ref.to_dict()) == ref


def test_partition_of():
    assert partition_of(5) == 0
    assert partition_of((52 << 48) | 7) == 52


def test_entity_info_heuristic():
    info = EntityInfo()
    info.note_sequence_number(3)
    assert not info.is_partitioned
    info.note_sequence_number((60 << 48) | 1)
    assert info.is_partitioned and info.orphan_guard_threshold == 9


def test_load_entity_info_from_management(broker):
    broker.add_queue("p", partitioned=True, sessions=False, lock_seconds=30, max_delivery_count=5).send(b"x")
    info = load_entity_info(connector(), EntityRef.from_source(SourceConfig(entity_type="queue", queue_name="p")))
    assert info.partitioned is True and info.lock_duration_seconds == 30 and info.max_delivery_count == 5
    assert info.counts["active"] == 1  # ty: ignore[not-subscriptable] -- non-None: the row above loaded successfully


def test_load_entity_info_from_management_subscription(broker):
    # regression: SubscriptionRuntimeProperties has no scheduled_message_count (7.14.3); reading it
    # unconditionally used to raise and get swallowed here, silently discarding every management value.
    broker.add_subscription("t", "s", sessions=True, partitioned=True, lock_seconds=45, max_delivery_count=7)
    ref = EntityRef.from_source(SourceConfig(entity_type="subscription", topic_name="t", subscription_name="s"))
    info = load_entity_info(connector(), ref)
    assert info.requires_session is True
    assert info.partitioned is True
    assert info.lock_duration_seconds == 45
    assert info.max_delivery_count == 7
    assert info.counts == {"active": 0, "dead_letter": 0, "scheduled": 0, "transfer_dead_letter": 0}


def test_load_entity_info_falls_back_when_denied(broker):
    broker.add_queue("q")
    broker.management_denied = True
    info = load_entity_info(connector(), EntityRef.from_source(SourceConfig(entity_type="queue", queue_name="q")))
    assert info.partitioned is None and info.lock_duration_seconds == 60


def test_list_names_sas_denied_returns_empty(broker):
    broker.add_queue("b")
    broker.management_denied = True
    assert list_entity_names(connector(), "queues") == []


def test_list_names_sp(broker):
    broker.add_queue("b")
    broker.add_queue("a")
    assert list_entity_names(connector("service_principal"), "queues") == ["a", "b"]


def test_list_names_sp_denied_is_user_exception(broker):
    broker.management_denied = True
    with pytest.raises(UserException):
        list_entity_names(connector("service_principal"), "topics")


def test_describe_entity_denied(broker):
    broker.add_queue("q")
    broker.management_denied = True
    with pytest.raises(UserException, match="Manage"):
        describe_entity(connector(), EntityRef.from_source(SourceConfig(entity_type="queue", queue_name="q")))


def test_describe_entity_subscription_lists_rules(broker):
    # regression: the same missing-attribute bug used to raise AttributeError (exit 2) for every
    # subscription, uncaught, before the markdown could be built.
    broker.add_subscription("t", "s").add_rule("big", "amount > 100")
    ref = EntityRef.from_source(SourceConfig(entity_type="subscription", topic_name="t", subscription_name="s"))
    markdown = describe_entity(connector(), ref)
    assert "big: amount > 100" in markdown


def test_open_receiver_entity_path_mismatch(broker):
    broker.add_queue("orders")
    auth = AuthConfiguration(**{"auth_type": "connection_string", "#connection_string": SAS + ";EntityPath=other"})
    ref = EntityRef.from_source(SourceConfig(entity_type="queue", queue_name="orders"))
    with pytest.raises(UserException, match="EntityPath"):
        ref.open_receiver(ServiceBusConnector(auth, "id").receive_client(), prefetch_count=1, keep_alive=0)


def test_log_entity_counts(caplog):
    import logging

    from entity import log_entity_counts

    ref = EntityRef.from_source(SourceConfig(entity_type="queue", queue_name="q"))
    counts = {"active": 3, "dead_letter": 1, "scheduled": 0, "transfer_dead_letter": 0}
    with caplog.at_level(logging.INFO, logger="entity"):
        log_entity_counts(EntityInfo(counts=counts), ref)
        log_entity_counts(EntityInfo(), ref)
    assert [r.getMessage() for r in caplog.records] == [
        "Entity 'q' holds 3 active, 1 dead-lettered, 0 scheduled and 0 transfer-dead-lettered message(s)."
    ]
