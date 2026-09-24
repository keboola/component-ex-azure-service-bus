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
from collections.abc import Callable, Iterable, Iterator
from typing import Any

from azure.core.exceptions import AzureError, ClientAuthenticationError, HttpResponseError, ResourceNotFoundError
from azure.identity import ClientSecretCredential, CredentialUnavailableError
from azure.servicebus import ServiceBusClient
from azure.servicebus.exceptions import (
    MessagingEntityDisabledError,
    MessagingEntityNotFoundError,
    ServiceBusAuthenticationError,
    ServiceBusAuthorizationError,
    ServiceBusCommunicationError,
    ServiceBusConnectionError,
    ServiceBusError,
    ServiceBusServerBusyError,
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

# Every Entra ID token-endpoint error carries an "AADSTS<n>" code (AADSTS7000215 invalid secret,
# AADSTS7000222 expired secret, AADSTS700016 unknown application, ...); a Service Bus 401 / 403 never does.
_ENTRA_ERROR_CODE = "AADSTS"
_IDENTITY_PACKAGE = "azure.identity"
CREDENTIALS_REJECTED = "The service principal credentials were rejected (check tenant ID, client ID and client secret)."


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
    """Rewrites the record's message, traceback and stack through :func:`redact_secrets` (J7).

    The message is formatted first (``msg % args``) and the result redacted, with ``args`` cleared:
    redacting each argument as a string instead would break numeric placeholders such as ``%d``,
    and formatting first also masks a secret carried by a non-string argument's ``str()``. An
    attached exception (``logger.exception``) is formatted into ``exc_text`` and redacted, and
    ``exc_info`` is cleared so no formatter renders the raw traceback again; ``stack_info`` is
    redacted too. Running the filter again on the same record (one instance sits on every root
    handler) is a no-op.
    """

    _TRACEBACK_FORMATTER = logging.Formatter()

    def __init__(self, secrets: Iterable[str]) -> None:
        super().__init__()
        self._secrets = tuple(s for s in secrets if s)

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact_secrets(record.getMessage(), self._secrets)
        record.args = None
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self._TRACEBACK_FORMATTER.formatException(record.exc_info)
            record.exc_info = None
        if record.exc_text:
            record.exc_text = redact_secrets(record.exc_text, self._secrets)
        if record.stack_info:
            record.stack_info = redact_secrets(record.stack_info, self._secrets)
        return True


# The SDK reports every retryable AMQP error at INFO before it retries (7.14.3, pyamqp transport:
# "AMQP error occurred: (...), condition: (b'com.microsoft:server-busy'), ..."); a throttled request
# is recognised by that condition. The retry line that follows names the same error, so only the
# condition line is counted.
_THROTTLE_CONDITION = "com.microsoft:server-busy"
_THROTTLE_LINE = "AMQP error occurred"


class ThrottleCounter(logging.Handler):
    """Counts the SDK's reports of throttled requests (ServerBusy) and prints nothing (§6.12): the
    count goes into the run summary, the SDK's own lines and tracebacks stay out of the job log."""

    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.count = 0

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if _THROTTLE_LINE in message and _THROTTLE_CONDITION in message:
            self.count += 1


_THROTTLE_COUNTER = ThrottleCounter()


def throttled_requests() -> int:
    """How many throttled requests (ServerBusy) the SDK reported since ``configure_logging``."""
    return _THROTTLE_COUNTER.count


def configure_logging(secrets: Iterable[str], debug: bool) -> None:
    """Install one :class:`RedactingFilter` on every root handler, gate the azure loggers and count
    throttling (§6.12).

    Idempotent: a filter installed by an earlier call is replaced, never duplicated. ``azure.*``
    loggers are set to INFO when ``debug`` (platform debug mode) else CRITICAL. ``azure.servicebus``
    always emits INFO to the :class:`ThrottleCounter` (reset here) and reaches the job log only in
    debug mode, so the count needs no SDK line in a normal job log.
    """
    root = logging.getLogger()
    redacting_filter = RedactingFilter(secrets)
    for handler in root.handlers:
        for existing in list(handler.filters):
            if isinstance(existing, RedactingFilter):
                handler.removeFilter(existing)
        handler.addFilter(redacting_filter)
    logging.getLogger("azure").setLevel(logging.INFO if debug else logging.CRITICAL)
    servicebus = logging.getLogger("azure.servicebus")
    servicebus.setLevel(logging.INFO)
    servicebus.propagate = debug
    for existing in [h for h in servicebus.handlers if isinstance(h, ThrottleCounter)]:
        servicebus.removeHandler(existing)
    _THROTTLE_COUNTER.count = 0
    servicebus.addHandler(_THROTTLE_COUNTER)


def is_session_mismatch(error: BaseException) -> bool:
    """True for the plain ``ServiceBusError`` the SDK raises when the row's Sessions setting does not
    match the entity (§6.11) -- exactly the texts :func:`to_user_exception` maps to "enable / disable
    Sessions". The SDK's own session-lock errors (``SessionLockLostError``, ...) never match."""
    if not isinstance(error, ServiceBusError):
        return False
    text = str(error)
    return _SESSION_REQUIRED_TEXT in text or any(marker in text.lower() for marker in _SESSION_NOT_USED_TEXTS)


def _linked_errors(error: BaseException) -> Iterator[BaseException]:
    """``error`` and the originals the SDKs attach to it -- ``inner_exception`` (the ``error=`` a
    ``ServiceBusError`` wraps) and ``__cause__`` -- each once."""
    pending, seen = [error], set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        yield current
        for linked in (getattr(current, "inner_exception", None), current.__cause__):
            if isinstance(linked, BaseException):
                pending.append(linked)


def _raised_by_azure_identity(error: BaseException) -> bool:
    """The innermost traceback frame -- where ``error`` was raised -- belongs to azure-identity."""
    frame_tb = error.__traceback__
    if frame_tb is None:
        return False
    while frame_tb.tb_next is not None:
        frame_tb = frame_tb.tb_next
    return str(frame_tb.tb_frame.f_globals.get("__name__", "")).startswith(_IDENTITY_PACKAGE)


def is_credential_failure(error: BaseException) -> bool:
    """True when the service principal's Entra ID token could not be acquired (§6.11): a wrong or
    expired secret, an unknown client ID or tenant -- never a missing right.

    ``ClientSecretCredential`` raises a ``ClientAuthenticationError`` ("Authentication failed: ...",
    ``CredentialUnavailableError`` when it cannot even try). The management client re-raises it
    unchanged; the data plane (pyamqp) wraps it in a plain ``ServiceBusError`` "Handler failed: ..."
    whose ``inner_exception`` is the original. It is recognised by its Entra ID error code
    (``AADSTS<n>``) or, for failures without one (an unknown tenant fails in MSAL's authority
    discovery), by having been raised inside azure-identity. A Service Bus 401 / 403 is neither.
    """
    for linked in _linked_errors(error):
        if isinstance(linked, CredentialUnavailableError):
            return True
        if isinstance(linked, AzureError) and (_ENTRA_ERROR_CODE in str(linked) or _raised_by_azure_identity(linked)):
            return True
    return False


def is_management_denied(error: Exception) -> bool:
    """True when the management endpoint itself rejected a call for lacking rights (§6.11): a SAS
    without Manage or a service principal without the Data Receiver role (HTTP 401 / 403). A failed
    token acquisition (:func:`is_credential_failure`) is never a denial."""
    if is_credential_failure(error):
        return False
    if isinstance(error, ClientAuthenticationError):
        return True
    return isinstance(error, HttpResponseError) and error.status_code in (401, 403)


def with_details(message: str, error: BaseException, secrets: Iterable[str] = ()) -> UserException:
    """``UserException("<message> (details: <redacted SDK message>)")`` -- the §6.11 / J7 shape."""
    return UserException(f"{message} (details: {redact_secrets(str(error), secrets)})")


def to_user_exception(
    error: ServiceBusError | AzureError, entity_path: str | None = None, secrets: Iterable[str] = ()
) -> UserException:
    """Translate a data-plane ``ServiceBusError`` or management-plane ``AzureError`` into an
    actionable, secret-free ``UserException`` (spec §6.11). Returns, does not raise, the exception.

    Handles ``azure.servicebus.exceptions.ServiceBusError`` subclasses and
    ``azure.core.exceptions.AzureError`` (management plane) only; never called with ``ValueError``
    (Global Constraints) -- that is mapped only at its two known sources.
    """
    target = f" '{entity_path}'" if entity_path else ""
    of_target = f" of '{entity_path}'" if entity_path else ""

    if is_credential_failure(error):  # before every data-plane type: pyamqp wraps it in a plain ServiceBusError
        message = CREDENTIALS_REJECTED
    elif isinstance(error, ServiceBusAuthenticationError):
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
    elif isinstance(error, ServiceBusServerBusyError):
        message = (
            f"Azure Service Bus is throttling the namespace{of_target} (ServerBusy): it reached its throughput "
            "limit (Standard tier: about 1,000 operations per second shared by every client of the namespace). "
            "Try again later, run fewer consumers at once, or use the Premium tier."
        )
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
    return with_details(message, error, secrets)


def _invalid_connection_string(auth: AuthConfiguration, error: ValueError) -> UserException:
    """Shared ``ValueError`` -> ``UserException`` mapping for a malformed connection string (§6.11);
    used by both the data-plane and management-plane connection-string builders."""
    return UserException(f"Invalid connection string: {redact_secrets(str(error), (auth.connection_string,))}")


def _connection_string_data_client(auth: AuthConfiguration, extra: dict[str, Any]) -> ServiceBusClient:
    try:
        return ServiceBusClient.from_connection_string(conn_str=auth.connection_string, user_agent=USER_AGENT, **extra)
    except ValueError as e:
        raise _invalid_connection_string(auth, e) from e


def _client_secret_credential(auth: AuthConfiguration) -> ClientSecretCredential:
    """The service-principal credential; azure-identity rejects a malformed tenant id with a
    ``ValueError`` before any network call -- a user-fixable setting, so exit 1 (§6.11)."""
    try:
        return ClientSecretCredential(auth.tenant_id, auth.client_id, auth.client_secret)
    except ValueError as e:
        detail = redact_secrets(str(e), (auth.client_secret,))
        raise UserException(f"Invalid service principal settings: {detail}") from e


def _service_principal_data_client(auth: AuthConfiguration, extra: dict[str, Any]) -> ServiceBusClient:
    credential = _client_secret_credential(auth)
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
    credential = _client_secret_credential(auth)
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

    def session_probe_client(self) -> ServiceBusClient:
        """A data-plane client with ``retry_total=0`` for a sync action's ``NEXT_AVAILABLE_SESSION``
        peek: the SDK retries a timed-out session accept (``OperationTimeoutError``) three times with
        backoff, which turns the 5-second accept wait on an entity without an available session into
        ~34 s [live] -- past the platform's 30-second sync-action limit."""
        return self._data_client({"retry_total": 0})

    def commit_client(self) -> ServiceBusClient:
        """A dedicated data-plane client for ``receive_deferred_messages`` with ``retry_total=0``
        (§6.2 -- never passed as a ``get_*_receiver`` kwarg, which raises ``TypeError`` on 7.14.3)."""
        return self._data_client({"retry_total": 0})

    def admin_client(self) -> ServiceBusAdministrationClient:
        """The management-plane client of the sync actions and the run's metadata pre-check."""
        return _ADMIN_BUILDERS[self._auth.auth_type](self._auth)

    def _data_client(self, extra: dict[str, Any]) -> ServiceBusClient:
        return _DATA_BUILDERS[self._auth.auth_type](self._auth, extra)
