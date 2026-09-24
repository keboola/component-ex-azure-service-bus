from unittest import mock

import msal
import pytest
from azure.core.exceptions import AzureError, ClientAuthenticationError, HttpResponseError, ResourceNotFoundError
from azure.identity import ClientSecretCredential, CredentialUnavailableError
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
from client import (
    USER_AGENT,
    ServiceBusConnector,
    is_credential_failure,
    is_management_denied,
    redact_secrets,
    to_user_exception,
)
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


@pytest.mark.parametrize("build", ["receive_client", "commit_client", "admin_client"])
def test_malformed_tenant_id_is_user_exception(build):
    # the real ClientSecretCredential rejects a malformed tenant id with ValueError, before any network
    fields = {**sp_auth().model_dump(by_alias=True), "tenant_id": "not a tenant!"}
    connector = ServiceBusConnector(AuthConfiguration(**fields), "id")
    with pytest.raises(UserException, match="Invalid service principal settings") as excinfo:
        getattr(connector, build)()
    assert "s3cr3t" not in str(excinfo.value)


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
    forbidden = HttpResponseError("nope")
    forbidden.status_code = 403
    assert is_management_denied(forbidden)
    assert not is_management_denied(ValueError("x"))


# --- service-principal token failures vs. authorization denials (spec §6.11, J7) ----------------------

AADSTS_EXPIRED = "AADSTS7000222: The provided client secret keys for app 'c' are expired."
UNKNOWN_TENANT = (
    "Unable to get authority configuration for https://login.microsoftonline.com/t. Authority would typically be "
    "in a format of https://login.microsoftonline.com/your_tenant. Also please double check your tenant name or "
    "GUID is correct."
)
CREDENTIALS_REJECTED = (
    "The service principal credentials were rejected (check tenant ID, client ID and client secret). (details: "
)
SCOPE = "https://servicebus.azure.net/.default"


class _RejectingMsalApp:
    """Stand-in for ``msal.ConfidentialClientApplication``: no cached token, and the token request
    fails with the error response Entra ID sends for an expired secret."""

    def __init__(self, *args, **kwargs) -> None:
        pass

    def acquire_token_silent_with_error(self, *args, **kwargs) -> None:
        return None

    def acquire_token_for_client(self, *args, **kwargs) -> dict[str, str]:
        return {"error": "invalid_client", "error_description": AADSTS_EXPIRED}


def identity_error(app_class) -> ClientAuthenticationError:
    """The error the real ``ClientSecretCredential.get_token`` raises when MSAL (faked, offline)
    fails the way ``app_class`` does."""
    credential = ClientSecretCredential("t", "c", "s3cr3t")
    with (
        mock.patch.object(msal, "ConfidentialClientApplication", app_class),
        pytest.raises(ClientAuthenticationError) as caught,
    ):
        credential.get_token(SCOPE)
    return caught.value


def data_plane(error: ClientAuthenticationError) -> ServiceBusError:
    """How pyamqp surfaces a credential error on the data plane (``create_servicebus_exception``)."""
    return ServiceBusError(message=f"Handler failed: {error}.", error=error)


def test_real_identity_errors_are_credential_failures():
    rejected = identity_error(_RejectingMsalApp)
    assert str(rejected) == f"Authentication failed: {AADSTS_EXPIRED}"
    # an unknown tenant fails in MSAL's authority discovery: azure-identity wraps it, no AADSTS code
    unknown_tenant = identity_error(mock.Mock(side_effect=ValueError(UNKNOWN_TENANT)))
    assert str(unknown_tenant) == f"Authentication failed: {UNKNOWN_TENANT}"
    for error in (rejected, unknown_tenant):
        assert is_credential_failure(error) and is_credential_failure(data_plane(error))
        assert not is_management_denied(error)


@pytest.mark.parametrize(
    "error",
    [
        ClientAuthenticationError(message=f"Authentication failed: {AADSTS_EXPIRED}"),
        CredentialUnavailableError(message="ClientSecretCredential is unavailable."),
        data_plane(ClientAuthenticationError(message=f"Authentication failed: {AADSTS_EXPIRED}")),
    ],
    ids=["management_aadsts", "credential_unavailable", "data_plane_aadsts"],
)
def test_credential_failures_are_recognised(error):
    assert is_credential_failure(error)
    assert not is_management_denied(error)


def _status(error: HttpResponseError, status_code: int) -> HttpResponseError:
    error.status_code = status_code
    return error


@pytest.mark.parametrize(
    "error",
    [
        ClientAuthenticationError(message="Unauthorized access. 'Manage,EntityRead' claims required."),
        _status(HttpResponseError(message="Unauthorized"), 401),
        _status(HttpResponseError(message="Forbidden"), 403),
        ServiceBusAuthenticationError(message="CBS token authentication failed for 'q': unauthorized."),
        ServiceBusError(message="Handler failed: link detached."),
        ResourceNotFoundError(message="Queue 'q' does not exist."),
    ],
    ids=["management_401_fake", "http_401", "http_403", "cbs_unauthorized", "plain_service_bus", "not_found"],
)
def test_endpoint_errors_are_not_credential_failures(error):
    assert not is_credential_failure(error)


@pytest.mark.parametrize(
    "error",
    [
        ClientAuthenticationError(message=f"Authentication failed: {AADSTS_EXPIRED} sent s3cr3t"),
        CredentialUnavailableError(message="ClientSecretCredential is unavailable: s3cr3t"),
        data_plane(ClientAuthenticationError(message=f"Authentication failed: {AADSTS_EXPIRED} sent s3cr3t")),
    ],
    ids=["management_aadsts", "credential_unavailable", "data_plane_aadsts"],
)
def test_credential_failure_maps_to_the_credentials_message(error):
    text = str(to_user_exception(error, "orders", ("s3cr3t",)))
    assert text == f"{CREDENTIALS_REJECTED}{str(error).replace('s3cr3t', '***')})"


def test_real_identity_error_maps_to_the_credentials_message():
    text = str(to_user_exception(data_plane(identity_error(mock.Mock(side_effect=ValueError(UNKNOWN_TENANT))))))
    assert text == f"{CREDENTIALS_REJECTED}Handler failed: Authentication failed: {UNKNOWN_TENANT}.)"


@pytest.mark.parametrize(
    "error",
    [
        ClientAuthenticationError(message="Unauthorized access. 'Manage,EntityRead' claims required."),
        _status(HttpResponseError(message="Forbidden"), 403),
    ],
    ids=["client_authentication", "http_403"],
)
def test_management_denial_keeps_the_role_message_with_details(error):
    text = str(to_user_exception(error, "orders"))
    assert text == (
        "The credentials cannot read the Service Bus management data of 'orders': a service principal needs the "
        f"'Azure Service Bus Data Receiver' role; a connection string needs Manage rights. (details: {error})"
    )


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


def test_redacting_filter_keeps_numeric_placeholders_and_masks_objects():
    import io
    import logging

    from client import RedactingFilter

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(RedactingFilter(["s3cr3t"]))
    log = logging.getLogger("redaction-numeric-test")
    log.addHandler(handler)
    log.propagate = False
    try:
        log.warning("recovery %d of %d: %s", 1, 5, ValueError("token s3cr3t"))
    finally:
        log.removeHandler(handler)
    assert stream.getvalue() == "recovery 1 of 5: token ***\n"


def test_redacting_filter_masks_tracebacks_and_stack_info():
    import io
    import logging

    from client import RedactingFilter

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(RedactingFilter(["s3cr3t"]))
    log = logging.getLogger("redaction-traceback-test")
    log.addHandler(handler)
    log.propagate = False
    try:
        try:
            raise RuntimeError(f"cannot open {SAS} with s3cr3t")
        except RuntimeError:
            log.exception("Component failed with an unexpected error")
        log.warning("where am I", stack_info=True, extra={"marker": "s3cr3t"})
    finally:
        log.removeHandler(handler)
    text = stream.getvalue()
    assert "Traceback (most recent call last)" in text and "RuntimeError: cannot open" in text
    assert "c2VjcmV0" not in text and "s3cr3t" not in text and "SharedAccessKey=***" in text
    assert "Stack (most recent call last)" in text


def test_redacting_filter_masks_preformatted_exc_text():
    import logging

    from client import RedactingFilter

    record = logging.LogRecord("x", logging.ERROR, __file__, 1, "boom", None, None)
    record.exc_text = f"Traceback ...\nValueError: {SAS} s3cr3t"
    RedactingFilter(["s3cr3t"]).filter(record)
    assert record.exc_text is not None
    assert "c2VjcmV0" not in record.exc_text and "s3cr3t" not in record.exc_text


def test_configure_logging_levels_and_filter():
    import logging

    from client import RedactingFilter, configure_logging

    root = logging.getLogger()
    azure_level = logging.getLogger("azure").level
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
        for other in root.handlers:  # configure_logging touched every root handler (pytest's included)
            for installed in [f for f in other.filters if isinstance(f, RedactingFilter)]:
                other.removeFilter(installed)
        logging.getLogger("azure").setLevel(azure_level)
