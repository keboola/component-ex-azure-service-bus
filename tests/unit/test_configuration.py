import pytest
from keboola.component.exceptions import UserException

from configuration import (
    AuthConfiguration,
    AuthType,
    BodyFormat,
    Configuration,
    FetchMode,
    LoadType,
    PrimaryKey,
    SettlementMode,
    SubQueue,
    SyncActionConfiguration,
    UnreadablePolicy,
    has_no_credentials,
)

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"
SOURCE = {"entity_type": "queue", "queue_name": "orders"}


def cfg(**overrides) -> Configuration:
    return Configuration(**{"auth_type": "connection_string", "#connection_string": SAS, "source": SOURCE, **overrides})  # ty: ignore[invalid-argument-type]


def test_defaults():
    c = cfg()
    assert c.source.settlement_mode is SettlementMode.COMPLETE
    assert c.source.settlement_mode.is_destructive
    assert c.source.sub_queue is SubQueue.NONE
    assert c.source.idle_timeout_seconds == 10
    assert (c.limits.max_messages, c.limits.stop_at_job_start) == (0, True)
    assert c.body.body_format is BodyFormat.TEXT
    assert c.body.unreadable_body is UnreadablePolicy.DEAD_LETTER
    assert c.destination.load_type is LoadType.INCREMENTAL_LOAD and c.destination.incremental
    assert c.destination.primary_key is PrimaryKey.SEQUENCE_NUMBER
    assert (c.advanced.batch_size, c.advanced.prefetch_count, c.advanced.recovery_wait_seconds) == (100, 1, 0)
    assert c.advanced.max_duration_seconds == 3000  # below the default one-hour job timeout


def test_queue_requires_queue_name():
    with pytest.raises(UserException, match="queue_name"):
        cfg(source={"entity_type": "queue"})


def test_subscription_requires_topic_and_subscription():
    with pytest.raises(UserException, match="topic_name"):
        cfg(source={"entity_type": "subscription", "subscription_name": "s"})


def test_other_entity_names_are_nulled():
    c = cfg(source={**SOURCE, "topic_name": "t", "subscription_name": "s"})
    assert c.source.topic_name is None and c.source.subscription_name is None


def test_fetch_mode_reset_outside_peek():
    c = cfg(source={**SOURCE, "fetch_mode": "full_fetch"})
    assert c.source.fetch_mode is FetchMode.INCREMENTAL_FETCH


def test_peek_keeps_fetch_mode():
    c = cfg(source={**SOURCE, "settlement_mode": "peek", "fetch_mode": "full_fetch"})
    assert c.source.fetch_mode is FetchMode.FULL_FETCH and not c.source.settlement_mode.is_destructive


def test_session_forced_off_on_sub_queue():
    c = cfg(source={**SOURCE, "sub_queue": "dead_letter", "session_enabled": True})
    assert c.source.session_enabled is False


def test_receive_and_delete_with_prefetch_refused():
    with pytest.raises(UserException, match="prefetch"):
        cfg(
            source={**SOURCE, "settlement_mode": "receive_and_delete"},
            advanced_options=True,
            advanced={"prefetch_count": 5},
        )


@pytest.mark.parametrize("extra", [{"session_enabled": True}, {"sub_queue": "dead_letter"}])
def test_peek_incremental_refused_on_sessions_and_sub_queues(extra):
    with pytest.raises(UserException, match="full_fetch"):
        cfg(source={**SOURCE, "settlement_mode": "peek", **extra})


def test_advanced_ignored_unless_enabled():
    assert cfg(advanced={"batch_size": 7}).advanced.batch_size == 100
    assert cfg(advanced_options=True, advanced={"batch_size": 7}).advanced.batch_size == 7


def test_max_duration_is_an_advanced_option():
    # Phase 8: hidden advanced values reset to the defaults, Max Duration included; the old
    # `limits.max_duration_seconds` key is ignored.
    assert cfg(advanced={"max_duration_seconds": 120}).advanced.max_duration_seconds == 3000
    assert cfg(advanced_options=True, advanced={"max_duration_seconds": 120}).advanced.max_duration_seconds == 120
    assert cfg(limits={"max_duration_seconds": 120}).advanced.max_duration_seconds == 3000
    with pytest.raises(UserException, match="max_duration_seconds"):
        cfg(advanced_options=True, advanced={"max_duration_seconds": 59})


@pytest.mark.parametrize("name", ["_bad", "bad-", "has space", "dots.no"])
def test_invalid_table_name(name):
    with pytest.raises(UserException, match="table_name"):
        cfg(destination={"table_name": name})


def test_valid_table_name():
    assert cfg(destination={"table_name": "orders-2026_raw"}).destination.table_name == "orders-2026_raw"


def test_service_principal_requires_fields():
    with pytest.raises(UserException, match="client_secret"):
        Configuration(auth_type="service_principal", tenant_id="t", client_id="c", source=SOURCE)


def test_removed_destructive_in_branch_key_is_ignored():
    # the removed dev-branch override: a config that still carries it keeps validating
    assert not hasattr(cfg(destructive_in_branch=True), "destructive_in_branch")


def test_primary_key_columns():
    assert PrimaryKey.SEQUENCE_NUMBER.columns == ["sequence_number"]
    assert PrimaryKey.MESSAGE_ID.columns == ["message_id"]
    assert PrimaryKey.SOURCE_ENTITY_SEQUENCE_NUMBER.columns == ["source_entity", "sequence_number"]


def test_auth_configuration_partial_ignores_row_fields():
    a = AuthConfiguration(**{"auth_type": "connection_string", "#connection_string": SAS, "source": {"x": 1}})
    assert a.auth_type is AuthType.CONNECTION_STRING and a.connection_string == SAS


def test_bad_enum_value_is_user_exception():
    with pytest.raises(UserException, match="settlement_mode"):
        cfg(source={**SOURCE, "settlement_mode": "delete_everything"})


def sync_cfg(**parameters) -> SyncActionConfiguration:
    return SyncActionConfiguration(**{"#connection_string": SAS, **parameters})  # ty: ignore[invalid-argument-type]


def test_sync_action_configuration_reads_a_partial_source():
    assert sync_cfg(source={"entity_type": "subscription", "topic_name": "t"}).topic_name == "t"
    assert sync_cfg(source={"entity_type": "subscription"}).topic_name is None
    assert sync_cfg().topic_name is None and sync_cfg(source=None).topic_name is None
    assert sync_cfg().topic_name is None
    with pytest.raises(UserException, match="source"):
        sync_cfg(source="not-an-object")


ROW_SAS = "Endpoint=sb://rowns.servicebus.windows.net/;SharedAccessKeyName=row;SharedAccessKey=cm93a2V5;EntityPath=t"


def test_row_connection_string_is_optional_and_empty_by_default():
    auth = AuthConfiguration(**{"#connection_string": SAS})
    assert auth.row_connection_string == "" and not auth.uses_row_connection_string
    assert auth.effective_auth_type is AuthType.CONNECTION_STRING and auth.effective_connection_string == SAS


def test_row_connection_string_replaces_the_root_connection_string():
    auth = AuthConfiguration(**{"#connection_string": SAS, "#row_connection_string": f"  {ROW_SAS}\n"})
    assert auth.uses_row_connection_string and auth.effective_connection_string == ROW_SAS


@pytest.mark.parametrize(
    "root",
    [{}, {"auth_type": "service_principal"}, {"auth_type": "service_principal", "tenant_id": "t"}],
    ids=["no_root_auth", "empty_service_principal", "partial_service_principal"],
)
def test_row_connection_string_makes_the_root_auth_optional(root):
    auth = AuthConfiguration(**root, **{"#row_connection_string": ROW_SAS})
    assert auth.effective_auth_type is AuthType.CONNECTION_STRING and auth.effective_connection_string == ROW_SAS


def test_row_connection_string_wins_over_a_complete_service_principal():
    auth = AuthConfiguration(
        auth_type="service_principal",
        tenant_id="t",
        client_id="c",
        fully_qualified_namespace="ns.servicebus.windows.net",
        **{"#client_secret": "x", "#row_connection_string": ROW_SAS},
    )
    assert auth.effective_auth_type is AuthType.CONNECTION_STRING and auth.auth_type is AuthType.SERVICE_PRINCIPAL


@pytest.mark.parametrize("blank", ["", "   ", "\n"])
def test_a_blank_row_connection_string_leaves_the_root_auth_required(blank):
    with pytest.raises(UserException, match="`#connection_string` is required for connection_string auth."):
        AuthConfiguration(**{"#row_connection_string": blank})


def test_row_connection_string_in_the_full_and_the_sync_models():
    row = {"#row_connection_string": ROW_SAS, "source": SOURCE}
    assert Configuration(**row).effective_connection_string == ROW_SAS  # ty: ignore[invalid-argument-type]
    assert SyncActionConfiguration(**row).effective_connection_string == ROW_SAS  # ty: ignore[invalid-argument-type]


@pytest.mark.parametrize("empty", [None, "", "  "])
def test_a_null_row_connection_string_is_empty(empty):
    auth = AuthConfiguration(**{"#connection_string": SAS, "#row_connection_string": empty})
    assert auth.row_connection_string == "" and auth.effective_connection_string == SAS


def test_a_non_string_row_connection_string_is_a_validation_error():
    with pytest.raises(UserException, match="Validation Error: #row_connection_string"):
        AuthConfiguration(**{"#connection_string": SAS, "#row_connection_string": 5})


@pytest.mark.parametrize(
    ("params", "missing"),
    [
        ({}, True),
        ({"auth_type": None, "#connection_string": ""}, True),
        ({"#connection_string": "  \n"}, True),
        ({"auth_type": "service_principal", "#connection_string": SAS}, True),
        ({"auth_type": "service_principal", "client_id": " "}, True),
        ({"#row_connection_string": "   "}, True),
        ({"#connection_string": SAS}, False),
        ({"auth_type": "service_principal", "tenant_id": "t"}, False),
        ({"auth_type": "connection_string", "#client_secret": "s"}, True),
        ({"#row_connection_string": ROW_SAS}, False),
        ({"auth_type": "bogus"}, False),
        ({"#row_connection_string": None}, True),
        ({"#row_connection_string": 5}, False),
        ({"#connection_string": ["x"]}, False),
    ],
)
def test_has_no_credentials(params, missing):
    assert has_no_credentials(params) is missing
