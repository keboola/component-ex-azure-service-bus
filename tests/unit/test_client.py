from unittest import mock

import pytest
from azure.core.exceptions import AzureError, ClientAuthenticationError, HttpResponseError, ResourceNotFoundError
from azure.servicebus.exceptions import (
    MessagingEntityDisabledError,
    MessagingEntityNotFoundError,
    ServiceBusAuthenticationError,
    ServiceBusAuthorizationError,
    ServiceBusConnectionError,
    ServiceBusError,
)
from keboola.component.exceptions import UserException

import client as client_mod
from client import USER_AGENT, ServiceBusConnector, is_management_denied, redact_secrets, to_user_exception
from configuration import AuthConfiguration, AuthType

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"


def sas_auth() -> AuthConfiguration:
    return AuthConfiguration(**{"auth_type": "connection_string", "#connection_string": SAS})


def sp_auth() -> AuthConfiguration:
    return AuthConfiguration(
        **{
            "auth_type": "service_principal",
            "tenant_id": "t",
            "client_id": "c",
            "#client_secret": "s3cr3t",
            "fully_qualified_namespace": "ns.servicebus.windows.net",
        }
    )


def test_redacts_key_sig_and_literals():
    out = redact_secrets(f"boom {SAS} sr=x&sig=abc%2Bdef&se=1 s3cr3t", ["s3cr3t"])
    assert "c2VjcmV0" not in out and "abc%2Bdef" not in out and "s3cr3t" not in out
    assert "SharedAccessKey=***" in out and "sig=***" in out


def test_receive_client_sas_user_agent_no_uamqp_no_retry_override():
    with mock.patch.object(client_mod, "ServiceBusClient") as sb:
        ServiceBusConnector(sas_auth(), "kbc-1-2").receive_client()
    kwargs = sb.from_connection_string.call_args.kwargs
    assert kwargs["user_agent"] == USER_AGENT
    assert "uamqp_transport" not in kwargs and "retry_total" not in kwargs


def test_commit_client_retry_total_zero():
    with mock.patch.object(client_mod, "ServiceBusClient") as sb:
        ServiceBusConnector(sas_auth(), "id").commit_client()
    assert sb.from_connection_string.call_args.kwargs["retry_total"] == 0


def test_service_principal_credential():
    with (
        mock.patch.object(client_mod, "ServiceBusClient") as sb,
        mock.patch.object(client_mod, "ClientSecretCredential") as cred,
    ):
        ServiceBusConnector(sp_auth(), "id").receive_client()
    cred.assert_called_once_with("t", "c", "s3cr3t")
    assert sb.call_args.kwargs["fully_qualified_namespace"] == "ns.servicebus.windows.net"


def test_admin_client_sas_and_sp():
    with mock.patch.object(client_mod, "ServiceBusAdministrationClient") as adm:
        ServiceBusConnector(sas_auth(), "id").admin_client()
        adm.from_connection_string.assert_called_once()
    with (
        mock.patch.object(client_mod, "ServiceBusAdministrationClient") as adm,
        mock.patch.object(client_mod, "ClientSecretCredential"),
    ):
        ServiceBusConnector(sp_auth(), "id").admin_client()
        assert adm.call_args.kwargs["fully_qualified_namespace"] == "ns.servicebus.windows.net"


def test_malformed_connection_string():
    auth = AuthConfiguration(**{"auth_type": "connection_string", "#connection_string": "garbage"})
    with pytest.raises(UserException, match="Invalid connection string"):
        ServiceBusConnector(auth, "id").receive_client()


def test_malformed_connection_string_admin_client():
    # Root testConnection / listQueues / listTopics call admin_client() without receive_client()
    # first, so the connection-string parse failure must be caught there too (real SDK parser).
    auth = AuthConfiguration(**{"auth_type": "connection_string", "#connection_string": "garbage"})
    with pytest.raises(UserException, match="Invalid connection string"):
        ServiceBusConnector(auth, "id").admin_client()


def test_secrets_tuple():
    assert ServiceBusConnector(sp_auth(), "id").secrets == ("s3cr3t",)


def test_auth_type_property():
    assert ServiceBusConnector(sas_auth(), "id").auth_type is AuthType.CONNECTION_STRING
    assert ServiceBusConnector(sp_auth(), "id").auth_type is AuthType.SERVICE_PRINCIPAL


@pytest.mark.parametrize(
    "error, fragment",
    [
        (ServiceBusAuthenticationError(message="x"), "IP firewall"),
        (ServiceBusAuthorizationError(message="x"), "Listen"),
        (MessagingEntityNotFoundError(message="x"), "was not found"),
        (MessagingEntityDisabledError(message="x"), "disabled"),
        (ServiceBusConnectionError(message="x"), "Could not reach"),
        (
            ServiceBusError(
                message="It is not possible for an entity that requires sessions to create a non-sessionful message receiver"
            ),
            "Sessions",
        ),
        # Management-plane AzureError cases (pre-flight ruling R-3, spec §6.11).
        (ClientAuthenticationError("401"), "Data Receiver"),
        (ResourceNotFoundError("x"), "was not found"),
        (AzureError("x"), "management reported an error"),
        (
            ServiceBusError(
                message="It is not possible for an entity that does not require sessions "
                "to create a sessionful message receiver"
            ),
            "does not use sessions",
        ),
    ],
)
def test_error_mapping(error, fragment):
    exc = to_user_exception(error, "orders")
    assert isinstance(exc, UserException)
    assert fragment in str(exc) and "orders" in str(exc)


def test_error_mapping_redacts():
    exc = to_user_exception(ServiceBusAuthenticationError(message=f"bad {SAS}"), "orders")
    assert "c2VjcmV0" not in str(exc)


def test_management_denied():
    assert is_management_denied(ClientAuthenticationError("401"))
    denied = HttpResponseError("nope")
    denied.status_code = 401
    assert is_management_denied(denied)
    assert not is_management_denied(ValueError("x"))


def test_redacting_filter_masks_log_records():
    import io
    import logging

    from client import RedactingFilter

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(RedactingFilter(["s3cr3t"]))
    log = logging.getLogger("redaction-test")
    log.addHandler(handler)
    log.propagate = False
    try:
        log.warning("conn %s and %s", SAS, "s3cr3t")
    finally:
        log.removeHandler(handler)
    text = stream.getvalue()
    assert "c2VjcmV0" not in text and "s3cr3t" not in text and "SharedAccessKey=***" in text


def test_configure_logging_levels_and_filter():
    import logging

    from client import RedactingFilter, configure_logging

    root = logging.getLogger()
    handler = logging.StreamHandler()
    root.addHandler(handler)
    try:
        configure_logging(["x"], debug=False)
        assert logging.getLogger("azure").level == logging.CRITICAL
        assert any(isinstance(f, RedactingFilter) for f in handler.filters)
        configure_logging(["x"], debug=True)
        assert logging.getLogger("azure").level == logging.INFO
        assert sum(isinstance(f, RedactingFilter) for f in handler.filters) == 1  # no duplicates
    finally:
        root.removeHandler(handler)
