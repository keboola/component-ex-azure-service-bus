from unittest import mock

import msal
import pytest
from azure.core.exceptions import HttpResponseError
from keboola.component.exceptions import UserException

from client import ServiceBusConnector
from configuration import AuthConfiguration, SourceConfig
from entity import (
    EntityInfo,
    EntityRef,
    list_entity_names,
    load_entity_info,
    partition_of,
    probe_management,
)

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


# --- service-principal token failures vs. authorization denials (spec §6.11, J7) ----------------------

SP_SECRET = "sp-s3cr3t"
AADSTS_INVALID = "AADSTS7000215: Invalid client secret provided."
CREDENTIALS_REJECTED = (
    "The service principal credentials were rejected (check tenant ID, client ID and client secret). "
    f"(details: Authentication failed: {AADSTS_INVALID} sent ***)"
)
DENIED_DETAILS = "(details: Unauthorized access. 'Manage,EntityRead' claims required.)"
QUEUE = EntityRef.from_source(SourceConfig(entity_type="queue", queue_name="q"))


def sp_connector() -> ServiceBusConnector:
    auth = AuthConfiguration(
        **{
            "auth_type": "service_principal",
            "tenant_id": "t",
            "client_id": "c",
            "#client_secret": SP_SECRET,
            "fully_qualified_namespace": "ns.servicebus.windows.net",
        }
    )
    return ServiceBusConnector(auth, "kbc-test")


def reject_credentials(broker) -> None:
    broker.add_queue("q")
    broker.credential_failure = f"{AADSTS_INVALID} sent {SP_SECRET}"  # the secret proves the redaction


def user_error(call) -> str:
    with pytest.raises(UserException) as caught:
        call()
    return str(caught.value)


def test_probe_management_sp_credentials_rejected(broker):
    reject_credentials(broker)
    assert user_error(lambda: probe_management(sp_connector())) == CREDENTIALS_REJECTED


@pytest.mark.parametrize("kind", ["queues", "topics"])
def test_list_names_sp_credentials_rejected(broker, kind):
    reject_credentials(broker)
    assert user_error(lambda: list_entity_names(sp_connector(), kind)) == CREDENTIALS_REJECTED


def test_probe_management_sp_denied_names_the_role_with_details(broker):
    broker.management_denied = True
    assert user_error(lambda: probe_management(sp_connector())) == (
        "The service principal cannot read the namespace: grant it the 'Azure Service Bus Data Receiver' role. "
        + DENIED_DETAILS
    )


def test_probe_management_sp_forbidden_names_the_role_with_details(broker):
    forbidden = HttpResponseError(message="Forbidden")
    forbidden.status_code = 403
    broker.management_error = forbidden
    assert user_error(lambda: probe_management(sp_connector())).endswith(
        "grant it the 'Azure Service Bus Data Receiver' role. (details: Forbidden)"
    )


def test_probe_management_listen_sas_with_details(broker):
    broker.management_denied = True
    assert user_error(lambda: probe_management(connector())) == (
        "The connection string has no Manage rights, so it cannot read the namespace's management data and "
        "cannot be tested here. Listen rights are enough to extract: use Preview Messages in a row to check "
        "that it can read the entity. " + DENIED_DETAILS
    )


def test_list_names_sp_denied_names_the_role_with_details(broker):
    broker.management_denied = True
    text = user_error(lambda: list_entity_names(sp_connector(), "topics"))
    assert "'Azure Service Bus Data Receiver' role" in text and text.endswith(DENIED_DETAILS)


class _RejectingMsalApp:
    """``msal.ConfidentialClientApplication`` failing the token request as Entra ID does for a bad secret."""

    def __init__(self, *args, **kwargs) -> None:
        pass

    def acquire_token_silent_with_error(self, *args, **kwargs) -> None:
        return None

    def acquire_token_for_client(self, *args, **kwargs) -> dict[str, str]:
        return {"error": "invalid_client", "error_description": AADSTS_INVALID}


UNKNOWN_TENANT = "Unable to get authority configuration for https://login.microsoftonline.com/t."


@pytest.mark.parametrize(
    "msal_app, detail",
    [
        (_RejectingMsalApp, AADSTS_INVALID),
        # MSAL's authority discovery fails for an unknown tenant; the text has no AADSTS code
        (mock.Mock(side_effect=ValueError(UNKNOWN_TENANT)), UNKNOWN_TENANT),
    ],
    ids=["invalid_secret", "unknown_tenant"],
)
def test_probe_management_real_sdk_credentials_rejected(msal_app, detail):
    """The real ``ServiceBusAdministrationClient`` + ``ClientSecretCredential`` (no FakeBroker): the
    management client surfaces the credential's error so the classifier sees it. MSAL is faked and any
    HTTP send fails the test, so nothing leaves the process."""
    with (
        mock.patch.object(msal, "ConfidentialClientApplication", msal_app),
        mock.patch(
            "azure.core.pipeline.transport.RequestsTransport.send", side_effect=AssertionError("network access")
        ),
    ):
        text = user_error(lambda: probe_management(sp_connector()))
    assert text == (
        "The service principal credentials were rejected (check tenant ID, client ID and client secret). "
        f"(details: Authentication failed: {detail})"
    )


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
