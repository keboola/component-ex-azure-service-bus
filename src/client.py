"""Service Bus connector: credential factory, client builders, error mapping, log redaction.

This module is the pluggable auth seam (spec §3.1): ``ServiceBusConnector`` dispatches on
``AuthConfiguration.auth_type`` to a concrete builder function via a dict, so a new auth method
slots in by adding a builder and registering it. The transport is fixed to pyamqp -- no transport
field is exposed and ``uamqp_transport`` is never set.

The SDK names (``ServiceBusClient``, ``ServiceBusAdministrationClient``, ``ClientSecretCredential``)
are imported as plain module-level names and referenced by their bare identifiers inside the builder
functions below, never aliased or captured as default-argument values. That keeps every lookup a
live read of this module's globals at call time, so tests patch the SDK with
``mock.patch.object(client, "ServiceBusClient", ...)``.
"""

import logging
import re
from collections.abc import Callable, Iterable
from typing import Any

from azure.core.exceptions import AzureError, ClientAuthenticationError, HttpResponseError, ResourceNotFoundError
from azure.identity import ClientSecretCredential
from azure.servicebus import ServiceBusClient
from azure.servicebus.exceptions import (
    MessagingEntityDisabledError,
    MessagingEntityNotFoundError,
    ServiceBusAuthenticationError,
    ServiceBusAuthorizationError,
    ServiceBusCommunicationError,
    ServiceBusConnectionError,
    ServiceBusError,
)
from azure.servicebus.management import ServiceBusAdministrationClient
from keboola.component.exceptions import UserException

from configuration import AuthConfiguration, AuthType

USER_AGENT = "keboola.ex-azure-service-bus"

_SAS_KEY_RE = re.compile(r"(SharedAccessKey=)[^;\s]+", re.IGNORECASE)
_SIG_RE = re.compile(r"(sig=)[^&;\s]+", re.IGNORECASE)

# Session-mismatch texts the SDK raises as a plain ServiceBusError (spec §6.11); matched
# case-insensitively against str(error). The "enable Sessions" text ("requires sessions") is
# checked first and always wins, since it can itself contain "non-sessionful" as a substring.
_SESSION_REQUIRED_TEXT = "requires sessions"
_SESSION_NOT_USED_TEXTS = (
    "non-sessionful entity",
    "is not session",
    "session is not enabled",
    "not require sessions",  # [inferred] reverse-mismatch phrasing, e.g. "does not require sessions"
)


def redact_secrets(text: str, secrets: Iterable[str] = ()) -> str:
    """Mask SAS keys, ``sig=`` tokens and literal secret values (J7).

    ``SharedAccessKey=<value>`` and ``sig=<value>`` are replaced up to the next ``&``, ``;`` or
    whitespace; every non-empty literal in ``secrets`` is replaced wherever it occurs.
    """
    redacted = _SIG_RE.sub(r"\1***", _SAS_KEY_RE.sub(r"\1***", text))
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "***")
    return redacted


class RedactingFilter(logging.Filter):
    """Rewrites the record's message through :func:`redact_secrets` (J7).

    The message is formatted first (``msg % args``) and the result redacted, with ``args`` cleared:
    redacting each argument as a string instead would break numeric placeholders such as ``%d``,
    and formatting first also masks a secret carried by a non-string argument's ``str()``. Running
    the filter again on the same record (one instance sits on every root handler) is a no-op.
    """

    def __init__(self, secrets: Iterable[str]) -> None:
        super().__init__()
        self._secrets = tuple(s for s in secrets if s)

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact_secrets(record.getMessage(), self._secrets)
        record.args = None
        return True


def configure_logging(secrets: Iterable[str], debug: bool) -> None:
    """Install one :class:`RedactingFilter` on every root handler and gate the azure loggers (§6.12).

    Idempotent: a filter installed by an earlier call is replaced, never duplicated. ``azure.*``
    loggers are set to INFO when ``debug`` (platform debug mode) else CRITICAL.
    """
    root = logging.getLogger()
    redacting_filter = RedactingFilter(secrets)
    for handler in root.handlers:
        for existing in list(handler.filters):
            if isinstance(existing, RedactingFilter):
                handler.removeFilter(existing)
        handler.addFilter(redacting_filter)
    logging.getLogger("azure").setLevel(logging.INFO if debug else logging.CRITICAL)


def is_session_mismatch(error: BaseException) -> bool:
    """True for the plain ``ServiceBusError`` the SDK raises when the row's Sessions setting does not
    match the entity (§6.11) -- exactly the texts :func:`to_user_exception` maps to "enable / disable
    Sessions". The SDK's own session-lock errors (``SessionLockLostError``, ...) never match."""
    if not isinstance(error, ServiceBusError):
        return False
    text = str(error)
    return _SESSION_REQUIRED_TEXT in text or any(marker in text.lower() for marker in _SESSION_NOT_USED_TEXTS)


def is_management_denied(error: Exception) -> bool:
    """True when a management-plane call was rejected for lacking rights (§6.11)."""
    if isinstance(error, ClientAuthenticationError):
        return True
    return isinstance(error, HttpResponseError) and error.status_code in (401, 403)


def to_user_exception(
    error: ServiceBusError | AzureError, entity_path: str | None = None, secrets: Iterable[str] = ()
) -> UserException:
    """Translate a data-plane ``ServiceBusError`` or management-plane ``AzureError`` into an
    actionable, secret-free ``UserException`` (spec §6.11). Returns, does not raise, the exception.

    Handles ``azure.servicebus.exceptions.ServiceBusError`` subclasses and
    ``azure.core.exceptions.AzureError`` (management plane) only; never called with ``ValueError``
    (Global Constraints) -- that is mapped only at its two known sources.
    """
    detail = redact_secrets(str(error), secrets)
    target = f" '{entity_path}'" if entity_path else ""
    of_target = f" of '{entity_path}'" if entity_path else ""

    if isinstance(error, ServiceBusAuthenticationError):
        message = (
            f"Authentication to Azure Service Bus failed for{target}: the credentials are wrong, the entity "
            "does not exist, or the namespace's IP firewall rejected the Keboola stack (Service Bus reports "
            "all three as unauthorized). Check Listen rights, the entity name, and that the stack's egress "
            "IP addresses are allowed."
        )
    elif isinstance(error, ServiceBusAuthorizationError):
        message = (
            f"The credentials lack the rights to read{target}: a connection string needs Listen; "
            "a service principal needs the 'Azure Service Bus Data Receiver' role."
        )
    elif isinstance(error, MessagingEntityNotFoundError):
        message = f"The entity{target} was not found in the namespace."
    elif isinstance(error, MessagingEntityDisabledError):
        message = f"The entity{target} is disabled for receiving."
    elif isinstance(error, ServiceBusConnectionError | ServiceBusCommunicationError):
        message = (
            f"Could not reach the Service Bus namespace for{target}. "
            "Check the host name and network access (outbound AMQP port 5671)."
        )
    elif isinstance(error, ServiceBusError):
        if _SESSION_REQUIRED_TEXT in str(error):
            message = f"The entity{target} requires sessions: enable Sessions in the row."
        elif is_session_mismatch(error):
            message = f"The entity{target} does not use sessions: disable Sessions in the row."
        else:
            message = f"Azure Service Bus reported an error for{target}."
    elif is_management_denied(error):
        message = (
            f"The credentials cannot read the Service Bus management data{of_target}: a service principal "
            "needs the 'Azure Service Bus Data Receiver' role; a connection string needs Manage rights."
        )
    elif isinstance(error, ResourceNotFoundError):
        message = f"The entity{target} was not found in the namespace."
    else:
        message = f"Azure Service Bus management reported an error{of_target}."
    return UserException(f"{message} (details: {detail})")


def _invalid_connection_string(auth: AuthConfiguration, error: ValueError) -> UserException:
    """Shared ``ValueError`` -> ``UserException`` mapping for a malformed connection string (§6.11);
    used by both the data-plane and management-plane connection-string builders."""
    return UserException(f"Invalid connection string: {redact_secrets(str(error), (auth.connection_string,))}")


def _connection_string_data_client(auth: AuthConfiguration, extra: dict[str, Any]) -> ServiceBusClient:
    try:
        return ServiceBusClient.from_connection_string(conn_str=auth.connection_string, user_agent=USER_AGENT, **extra)
    except ValueError as e:
        raise _invalid_connection_string(auth, e) from e


def _service_principal_data_client(auth: AuthConfiguration, extra: dict[str, Any]) -> ServiceBusClient:
    credential = ClientSecretCredential(auth.tenant_id, auth.client_id, auth.client_secret)
    return ServiceBusClient(
        fully_qualified_namespace=auth.fully_qualified_namespace,
        credential=credential,
        user_agent=USER_AGENT,
        **extra,
    )


_DATA_BUILDERS: dict[AuthType, Callable[[AuthConfiguration, dict[str, Any]], ServiceBusClient]] = {
    AuthType.CONNECTION_STRING: _connection_string_data_client,
    AuthType.SERVICE_PRINCIPAL: _service_principal_data_client,
}


def _connection_string_admin_client(auth: AuthConfiguration) -> ServiceBusAdministrationClient:
    try:
        return ServiceBusAdministrationClient.from_connection_string(auth.connection_string)
    except ValueError as e:
        raise _invalid_connection_string(auth, e) from e


def _service_principal_admin_client(auth: AuthConfiguration) -> ServiceBusAdministrationClient:
    credential = ClientSecretCredential(auth.tenant_id, auth.client_id, auth.client_secret)
    return ServiceBusAdministrationClient(
        fully_qualified_namespace=auth.fully_qualified_namespace, credential=credential
    )


_ADMIN_BUILDERS: dict[AuthType, Callable[[AuthConfiguration], ServiceBusAdministrationClient]] = {
    AuthType.CONNECTION_STRING: _connection_string_admin_client,
    AuthType.SERVICE_PRINCIPAL: _service_principal_admin_client,
}


class ServiceBusConnector:
    """Builds the data-plane and management-plane clients for one row's auth configuration.

    No network I/O happens in ``__init__``; clients are opened lazily by the accessor methods.
    """

    def __init__(self, auth: AuthConfiguration, client_identifier: str) -> None:
        self._auth = auth
        self.client_identifier = client_identifier
        self.secrets: tuple[str, ...] = tuple(s for s in (auth.connection_string, auth.client_secret) if s)

    @property
    def auth_type(self) -> AuthType:
        """The auth method this connector was built with (SAS vs. service-principal wording)."""
        return self._auth.auth_type

    def receive_client(self) -> ServiceBusClient:
        """The data-plane client used for every receive / peek / settle (SDK default retries)."""
        return self._data_client({})

    def commit_client(self) -> ServiceBusClient:
        """A dedicated data-plane client for ``receive_deferred_messages`` with ``retry_total=0``
        (§6.2 -- never passed as a ``get_*_receiver`` kwarg, which raises ``TypeError`` on 7.14.3)."""
        return self._data_client({"retry_total": 0})

    def admin_client(self) -> ServiceBusAdministrationClient:
        """The management-plane client used by sync actions and ``entityInfo``."""
        return _ADMIN_BUILDERS[self._auth.auth_type](self._auth)

    def _data_client(self, extra: dict[str, Any]) -> ServiceBusClient:
        return _DATA_BUILDERS[self._auth.auth_type](self._auth, extra)
