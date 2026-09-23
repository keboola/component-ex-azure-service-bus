# Azure Service Bus Extractor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Each task names the **owner component skill** its subagent must load so it stays Keboola-aware (`component-develop` for code, `component-build-ui` for schemas, `component-test` for the functional suite).

**Goal:** Build the Keboola extractor `keboola.ex-azure-service-bus` — one config row per Service Bus queue / topic subscription / dead-letter sub-queue, four settlement modes (complete / defer-commit / receive-and-delete / peek), typed metadata columns plus text / base64 / flattened-JSON bodies, row-scoped state for the defer-commit pending set, the peek cursor and the flatten column registry.

**Architecture:** Config-rows component. Root = auth (mirrors the writer); row = source + reading mode, limits, body, destination, gated advanced options. `component.py` holds a thin `run()` and the sync actions; logic lives in `client.py` (connector, errors, redaction), `entity.py`, `state.py`, `body.py`, `columns.py`, `output.py`, `stats.py`, `settlement.py` (per-batch write-then-settle pipeline + unreadable handling), `commit.py` (H5 commit, H3 orphan scan, foreign-deferral probe) and `receiver.py` (receive loop, peek pager). The data plane is AMQP, so tests run against an in-repo `FakeBroker` SDK double; real AMQP is proven in the Phase-7 cf-dev smoke.

**Tech Stack:** Python 3.14, `azure-servicebus>=7.14.3,<7.15` (pyamqp), `azure-identity>=1.19,<2`, `keboola-component`, `pydantic` v2, `pytest`, `ruff`, `ty`.

**Spec:** `docs/superpowers/specs/2026-09-23-ex-azure-service-bus-design.md` — read it alongside this plan; section numbers (§) below refer to it.

## Global Constraints

Every task's requirements implicitly include these (values from the spec):

- **Python 3.14** — `requires-python = "~=3.14.0"`; ruff target py314; PEP 758 `except A, B:` is valid.
- **SDK:** `azure-servicebus>=7.14.3,<7.15`, `azure-identity>=1.19,<2`. pyamqp only — **never** set `uamqp_transport`, no transport field, **no `websocket-client`** (K2 excluded, §11).
- **Receiver profile:** every receiver gets `prefetch_count >= 1` (default 1) **and receiver-level `keep_alive=0`**; no `AutoLockRenewer`; every SDK call on the main thread.
- **Commit client:** `retry_total=0` on a dedicated `ServiceBusClient` (never as a `get_*_receiver` kwarg — `TypeError` on 7.14.3).
- **Secrets:** `#connection_string`, `#client_secret` (`Field(alias="#…")`). Redact `SharedAccessKey=…`, `sig=…` and the literal secret values in every surfaced message and log line. Never log message bodies (sequence number + message id only).
- **Config rows:** single merged `config.json`; rows sequential; state row-scoped; hidden `destructive_in_branch` (model only).
- **Output:** `/data/out/tables/<table>.csv` **with a header row**; manifest `has_header=True`, native-type `schema`, `primary_key`, `incremental` from `load_type`, no `destination`; manifest written **before the first settle** with **`write_always=False`**, switched to `True` by `OutputTable.arm_write_always()` **immediately before the first destructive settle — C1 before its first `complete_message`, C3 before its first receive; never in C2 or C4** (spec §6.9 table); flatten staging only under `/tmp`.
- **Flatten (spec §6.10):** reserved JSON column `body_unmapped` always present; a run materialises the **input-state** registry columns + `body_unmapped`; a new key becomes a column in the same run only if the run succeeds **and** the table was never armed for `write_always`; otherwise its values go to `body_unmapped` and the key (saved in the output registry) is a column from the next run.
- **State:** `ExtractorState` version 1, written once at the end, merge rule (§6.8), budget 256 KiB.
- **Constants (§6):** K = `max(100, 2 * (batch_size + prefetch_count + 1))`; orphan page 250, page cap 20; commit ≤ 250 seqs and ≤ 16 MiB per call; transient commit retries 3 with 2/4/8 s backoff; recoveries 5 per run + no-progress guard; lock renew margin 10 s; unreadable abort share > 10 % and ≥ 10; flatten cap 1,000 columns, column name ≤ 64 chars; body cell limit 16 MiB; orphan guard `delivery_count >= max_delivery_count - 1` (9 when unknown); unreadable-body retry budget `MAX_UNREADABLE_RECYCLES = 50` per run (separate from the 5 connection recoveries); `SESSION_ACCEPT_WAIT_SECONDS = 5` for every session receiver outside the destructive receive loop.
- **Errors:** user-fixable → `UserException` (exit 1); unexpected → exit 2. Keep the scaffold `__main__` guard. `ValueError` is mapped to a `UserException` only at its two known sources — the connection-string parse (`ServiceBusConnector`) and the `EntityPath` mismatch (`EntityRef.open_receiver`); anywhere else it is a bug (exit 2).
- **Injectable time:** every `sleep` / `monotonic` / `clock` constructor parameter defaults to `None` and is resolved at construction (`self._sleep = sleep or time.sleep`, `self._monotonic = monotonic or time.monotonic`, `clock or (lambda: datetime.now(UTC))`), so functional tests can monkeypatch `time.sleep` / `time.monotonic`.
- **UI:** every enum stores the value; gated fields don't serialize while hidden; `destructive_in_branch` in no schema; row configs need `rows >= 1`.
- **Privacy:** no customer / company / person names, no ticket ids, no real secrets, no concrete test namespace / tenant / SP identifiers in any committed file. Grep before every commit.
- **Commits** end with the session's attribution trailer. Work on branch `initial-implementation`.

## File Structure

| Path | Responsibility |
|---|---|
| `src/configuration.py` | enums + Pydantic models (§5, §7) |
| `src/client.py` | `ServiceBusConnector`, credential factory, `redact_secrets`, `to_user_exception`, log redaction / azure logger levels |
| `src/entity.py` | `EntityRef`, `EntityInfo`, `partition_of`, management helpers (list / describe) |
| `src/state.py` | `ExtractorState` + range encoding + `PendingSetBuilder` |
| `src/body.py` | body decoding, JSON flattening, `FlattenRegistry` |
| `src/columns.py` | fixed column catalogue, message → metadata row, value formats, preview rendering |
| `src/output.py` | `OutputTable` (streaming CSV or `/tmp` staging), manifest |
| `src/stats.py` | `RunStats` |
| `src/settlement.py` | settlers, `UnreadableHandler`, `BatchProcessor` |
| `src/commit.py` | `PendingCommitter`, `OrphanScanner`, `ForeignDeferralProbe` |
| `src/receiver.py` | `ReceiveLoop`, `PeekPager`, `RecoveryTracker` |
| `src/component.py` | orchestrator + sync actions |
| `component_config/*` | schemas, uiOptions, descriptions, sample config |
| `tests/fakes/broker.py` | `FakeBroker` SDK double (data plane + management) |
| `tests/conftest.py` | `broker` fixture installing the fakes |
| `tests/unit/test_*.py` | unit tests per module |
| `tests/functional/` | datadir suite: `conftest.py` (harness), `test_sync_actions.py` (cases 01–16), `test_runs.py` (20–45), `test_bodies_and_robustness.py` (46–67), `test_sanitisation.py`, `expected/20_run_c1_queue/` |
| `tests/setup/configs.json` | wrapped-format functional configs (dummy credentials) |

---

## Phase 4 — Implementation

### Task 1: Dependencies & scaffold cleanup

**Owner skill:** `component-develop` (consult `component-defaults` for pyproject / Dockerfile alignment).

**Files:**
- Modify: `pyproject.toml`, `uv.lock`
- Modify: `src/configuration.py`, `src/component.py` (strip the cookiecutter example)
- Delete: `tests/test_component.py` (scaffold example; replaced by `tests/unit/`)
- Create: `tests/unit/__init__.py`, `tests/fakes/__init__.py`, `tests/functional/__init__.py`

**Interfaces:** Produces: `azure.servicebus` 7.14.x and `azure.identity` importable.

- [ ] **Step 1:** In `pyproject.toml` `[project].dependencies` set exactly:
  ```toml
  dependencies = [
      # capped <7.15: 7.15.0 removes uamqp_transport; this component is pyamqp-only (spec §3.1).
      "azure-servicebus>=7.14.3,<7.15",
      "azure-identity>=1.19,<2",
      # >=1.11: manifest write_always + has_header support as read in 1.11.0 (spec §6.9).
      "keboola-component>=1.11",
      "pydantic>=2.11.7",
  ]
  ```
  Remove `keboola-http-client` and `keboola-utils` (not used; §7). Remove `freezegun` and `mock` from the dev group (tests use `unittest.mock` and `FakeClock`). Add `extend-exclude = ["docs"]` under `[tool.ruff]` (keep ruff off authored docs, writer precedent).
- [ ] **Step 2:** `uv lock && uv sync --all-groups`.
- [ ] **Step 3:** Verify: `uv run python -c "import azure.servicebus, azure.identity; print(azure.servicebus.__version__)"` → `7.14.3`.
- [ ] **Step 4:** Replace `src/configuration.py` with a module docstring only (Task 2 fills it); reduce `src/component.py` to the `Component(ComponentBase)` class with `run()` raising `NotImplementedError` and the scaffold `__main__` guard (`UserException` → `logger.error(...)`, `sys.exit(1)`; `Exception` → `logger.exception(...)`, `sys.exit(2)`). Delete `tests/test_component.py`; create the empty `__init__.py` files. `uv run ruff check src tests` clean.
- [ ] **Step 5:** Commit: `chore: add azure-servicebus + azure-identity deps, strip scaffold example`.

---

### Task 2: Configuration model

**Owner skill:** `component-develop`.

**Files:**
- Create: `src/configuration.py`
- Test: `tests/unit/test_configuration.py`

**Interfaces — Produces:**
- `StrEnum`s: `AuthType` (`CONNECTION_STRING="connection_string"`, `SERVICE_PRINCIPAL="service_principal"`); `EntityType` (`QUEUE`, `SUBSCRIPTION`); `SubQueue` (`NONE="none"`, `DEAD_LETTER="dead_letter"`, `TRANSFER_DEAD_LETTER="transfer_dead_letter"`); `SettlementMode` (`COMPLETE="complete"`, `DEFER_COMMIT="defer_commit"`, `RECEIVE_AND_DELETE="receive_and_delete"`, `PEEK="peek"`) with property `is_destructive -> bool` (all but `PEEK`); `FetchMode` (`INCREMENTAL_FETCH`, `FULL_FETCH`); `BodyFormat` (`TEXT="text"`, `BASE64="base64"`, `JSON_FLATTEN="json_flatten"`); `UnreadablePolicy` (`DEAD_LETTER`, `LEAVE`, `FAIL`); `LoadType` (`INCREMENTAL_LOAD`, `FULL_LOAD`); `PrimaryKey` (`SEQUENCE_NUMBER`, `MESSAGE_ID`, `SOURCE_ENTITY_SEQUENCE_NUMBER="source_entity_sequence_number"`) with property `columns -> list[str]`.
- `AuthConfiguration(BaseModel)` — `model_config = ConfigDict(populate_by_name=True, extra="ignore")`; `auth_type: AuthType = CONNECTION_STRING`; `connection_string: str = Field("", alias="#connection_string")`; `tenant_id: str = ""`; `client_id: str = ""`; `client_secret: str = Field("", alias="#client_secret")`; `fully_qualified_namespace: str = ""`; validator `_validate_auth` (writer's rules). `__init__` catches `ValidationError` → `UserException("Validation Error: <loc>: <msg>")` — one `<loc>: <msg>` pair per error, joined with `, `; e.g. `UserException("Validation Error: source.queue_name: Value error, `source.queue_name` is required when `entity_type` is `queue`.")` with dotted locations.
- `SourceConfig`: `entity_type: EntityType`; `queue_name`, `topic_name`, `subscription_name: str | None = None`; `sub_queue: SubQueue = NONE`; `session_enabled: bool = False`; `settlement_mode: SettlementMode = COMPLETE`; `fetch_mode: FetchMode = INCREMENTAL_FETCH`; `idle_timeout_seconds: int = Field(10, ge=1, le=300)`.
- `LimitsConfig`: `max_messages: int = Field(0, ge=0)`; `max_duration_seconds: int = Field(3600, ge=60, le=43200)`; `stop_at_job_start: bool = True`.
- `BodyConfig`: `body_format: BodyFormat = TEXT`; `unreadable_body: UnreadablePolicy = DEAD_LETTER`.
- `DestinationConfig`: `table_name: str = ""`; `load_type: LoadType = INCREMENTAL_LOAD`; `primary_key: PrimaryKey = SEQUENCE_NUMBER`; property `incremental -> bool`.
- `AdvancedConfig`: `batch_size: int = Field(100, ge=1, le=5000)`; `prefetch_count: int = Field(1, ge=1, le=1000)`; `recovery_wait_seconds: int = Field(0, ge=0, le=330)`.
- `Configuration(AuthConfiguration)`: `source: SourceConfig`; `limits`, `body`, `destination`, `advanced` with `default_factory`; `advanced_options: bool = False`; `destructive_in_branch: bool = False`.

- [ ] **Step 1: Write the failing tests** — `tests/unit/test_configuration.py`:

```python
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
    UnreadablePolicy,
)

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"
SOURCE = {"entity_type": "queue", "queue_name": "orders"}


def cfg(**overrides) -> Configuration:
    return Configuration(**{"auth_type": "connection_string", "#connection_string": SAS, "source": SOURCE, **overrides})


def test_defaults():
    c = cfg()
    assert c.source.settlement_mode is SettlementMode.COMPLETE
    assert c.source.settlement_mode.is_destructive
    assert c.source.sub_queue is SubQueue.NONE
    assert c.source.idle_timeout_seconds == 10
    assert (c.limits.max_messages, c.limits.max_duration_seconds, c.limits.stop_at_job_start) == (0, 3600, True)
    assert c.body.body_format is BodyFormat.TEXT
    assert c.body.unreadable_body is UnreadablePolicy.DEAD_LETTER
    assert c.destination.load_type is LoadType.INCREMENTAL_LOAD and c.destination.incremental
    assert c.destination.primary_key is PrimaryKey.SEQUENCE_NUMBER
    assert (c.advanced.batch_size, c.advanced.prefetch_count, c.advanced.recovery_wait_seconds) == (100, 1, 0)
    assert c.destructive_in_branch is False


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


@pytest.mark.parametrize("name", ["_bad", "bad-", "has space", "dots.no"])
def test_invalid_table_name(name):
    with pytest.raises(UserException, match="table_name"):
        cfg(destination={"table_name": name})


def test_valid_table_name():
    assert cfg(destination={"table_name": "orders-2026_raw"}).destination.table_name == "orders-2026_raw"


def test_service_principal_requires_fields():
    with pytest.raises(UserException, match="client_secret"):
        Configuration(auth_type="service_principal", tenant_id="t", client_id="c", source=SOURCE)


def test_hidden_destructive_in_branch_accepted():
    assert cfg(destructive_in_branch=True).destructive_in_branch is True


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
```

- [ ] **Step 2:** `uv run pytest tests/unit/test_configuration.py -v` → FAIL (import error).
- [ ] **Step 3: Implement** `src/configuration.py` per the interface. Validators on `Configuration`, all `@model_validator(mode="after")`, in this order:
  1. `_normalise_source` — `queue`: require `queue_name` (`ValueError("`source.queue_name` is required when `entity_type` is `queue`.")`), null `topic_name` / `subscription_name`; `subscription`: require both names (message names the missing one), null `queue_name`; if `settlement_mode is not PEEK` reset `fetch_mode` to `INCREMENTAL_FETCH`; if `sub_queue is not NONE` set `session_enabled = False`.
  2. `_apply_advanced_gate` — if not `advanced_options`: `self.advanced = AdvancedConfig()`.
  3. `_refuse_unsafe` — `RECEIVE_AND_DELETE` and `advanced.prefetch_count > 1` → `ValueError("receive_and_delete requires prefetch_count 1: buffered messages are already deleted on the broker.")`; `PEEK` + `INCREMENTAL_FETCH` + (`session_enabled` or `sub_queue is not NONE`) → `ValueError("peek with incremental_fetch is not supported on session entities or sub-queues; use full_fetch.")`.
  4. `_validate_table_name` — non-empty `destination.table_name` must match `^[A-Za-z0-9](?:[A-Za-z0-9_-]*[A-Za-z0-9])?$`, else `ValueError("`destination.table_name` may contain only letters, digits, '-' and '_' and must not start or end with '-' or '_'.")`.
  The `ValidationError` → `UserException` wrapper lives in `AuthConfiguration.__init__` (inherited): `loc = ".".join(str(p) for p in err["loc"]) or "configuration"`, message `f"Validation Error: {', '.join(f'{loc}: {msg}')}"`, `raise UserException(...) from e`.
- [ ] **Step 4:** Tests PASS; `uv run ruff check src tests` clean.
- [ ] **Step 5:** Commit: `feat: pydantic configuration model (auth root, source/limits/body/destination rows, hidden branch override)`.

---

### Task 3: Connector, error mapping, redaction

**Owner skill:** `component-develop`.

**Files:**
- Create: `src/client.py`
- Test: `tests/unit/test_client.py`

**Interfaces:**
- Consumes: `AuthConfiguration`, `AuthType` (Task 2).
- Produces:
  - `USER_AGENT = "keboola.ex-azure-service-bus"`.
  - `redact_secrets(text: str, secrets: Iterable[str] = ()) -> str` — masks `SharedAccessKey=<v>` → `SharedAccessKey=***`, `sig=<v>` → `sig=***` (up to `&`, `;` or whitespace) and every non-empty literal in `secrets` → `***`.
  - `class RedactingFilter(logging.Filter)` (`__init__(self, secrets: Iterable[str])`; rewrites `record.msg` / `record.args` through `redact_secrets`).
  - `configure_logging(secrets: Iterable[str], debug: bool) -> None` — installs one `RedactingFilter` on every root handler (replacing an earlier one, never duplicating); sets `logging.getLogger("azure")` to `INFO` if `debug` else `CRITICAL`.
  - `is_management_denied(error: Exception) -> bool` — `ClientAuthenticationError`, or `HttpResponseError` with `status_code in (401, 403)`.
  - `to_user_exception(error: ServiceBusError | AzureError, entity_path: str | None = None, secrets: Iterable[str] = ()) -> UserException` — mapping per spec §6.11 (texts below). It handles `azure.servicebus.exceptions.ServiceBusError` subclasses and `azure.core.exceptions.AzureError` (management plane) only; it is never called with `ValueError` (Global Constraints).
  - `class ServiceBusConnector` — `__init__(self, auth: AuthConfiguration, client_identifier: str)`; `receive_client() -> ServiceBusClient`; `commit_client() -> ServiceBusClient` (adds `retry_total=0`); `admin_client() -> ServiceBusAdministrationClient`; `client_identifier: str`; `secrets: tuple[str, ...]` (non-empty `connection_string`, `client_secret`). No network in `__init__`. A `ValueError` from `from_connection_string` → `UserException("Invalid connection string: <redacted>")`.

- [ ] **Step 1: Write the failing tests** — `tests/unit/test_client.py`:

```python
from unittest import mock

import pytest
from azure.core.exceptions import ClientAuthenticationError, HttpResponseError
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
from configuration import AuthConfiguration

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


def test_secrets_tuple():
    assert ServiceBusConnector(sp_auth(), "id").secrets == ("s3cr3t",)


@pytest.mark.parametrize(
    "error, fragment",
    [
        (ServiceBusAuthenticationError(message="x"), "IP firewall"),
        (ServiceBusAuthorizationError(message="x"), "Listen"),
        (MessagingEntityNotFoundError(message="x"), "was not found"),
        (MessagingEntityDisabledError(message="x"), "disabled"),
        (ServiceBusConnectionError(message="x"), "Could not reach"),
        (
            ServiceBusError(message="It is not possible for an entity that requires sessions to create a non-sessionful message receiver"),
            "Sessions",
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
```

- [ ] **Step 2:** `uv run pytest tests/unit/test_client.py -v` → FAIL.
- [ ] **Step 3: Implement** `src/client.py`:
  - Builders dispatch on `auth_type` (dict `_DATA_BUILDERS`, like the writer). SAS: `ServiceBusClient.from_connection_string(conn_str=…, user_agent=USER_AGENT, **extra)`; SP: `ServiceBusClient(fully_qualified_namespace=…, credential=ClientSecretCredential(tenant_id, client_id, client_secret), user_agent=USER_AGENT, **extra)`; `extra = {"retry_total": 0}` for `commit_client()` only. Admin: `ServiceBusAdministrationClient.from_connection_string(conn_str)` / `ServiceBusAdministrationClient(fully_qualified_namespace=…, credential=…)`.
  - Regexes: `_SAS_KEY_RE = re.compile(r"(SharedAccessKey=)[^;\s]+", re.I)`, `_SIG_RE = re.compile(r"(sig=)[^&;\s]+", re.I)`.
  - `to_user_exception` messages (all end with ` (details: <redacted str(error)>)`; `target = f" '{entity_path}'"`):
    - `ServiceBusAuthenticationError` → `f"Authentication to Azure Service Bus failed for{target}: the credentials are wrong, the entity does not exist, or the namespace's IP firewall rejected the Keboola stack (Service Bus reports all three as unauthorized). Check Listen rights, the entity name, and that the stack's egress IP addresses are allowed."`
    - `ServiceBusAuthorizationError` → `f"The credentials lack the rights to read{target}: a connection string needs Listen; a service principal needs the 'Azure Service Bus Data Receiver' role."`
    - `MessagingEntityNotFoundError` → `f"The entity{target} was not found in the namespace."`
    - `MessagingEntityDisabledError` → `f"The entity{target} is disabled for receiving."`
    - `ServiceBusConnectionError` / `ServiceBusCommunicationError` → `f"Could not reach the Service Bus namespace for{target}. Check the host name and network access (outbound AMQP port 5671)."`
    - `ServiceBusError` whose text contains `"requires sessions"` → `f"The entity{target} requires sessions: enable Sessions in the row."`; containing `"non-sessionful entity"` or `"not session"`-style texts → `f"The entity{target} does not use sessions: disable Sessions in the row."`
    - any other `ServiceBusError` → `f"Azure Service Bus reported an error for{target}."`
  - `configure_logging` as in the interface.
- [ ] **Step 4:** Tests PASS; ruff clean.
- [ ] **Step 5:** Commit: `feat: service bus connector (receive/commit/admin clients), error mapping, secret redaction`.

---

### Task 4: `FakeBroker` SDK double

**Owner skill:** `component-test` (test infrastructure used by every later task).

**Files:**
- Create: `tests/fakes/broker.py`, `tests/conftest.py`
- Test: `tests/fakes/test_broker.py`

**Interfaces — Produces** (all later tests rely on these exact names):
- `FakeClock(start: datetime = datetime(2026, 9, 23, 10, 0, tzinfo=UTC))` with `now() -> datetime`, `advance(seconds: float) -> None`.
- `FakeBroker(clock: FakeClock | None = None)`:
  - `add_queue(name, *, sessions=False, partitioned=False, lock_seconds=60, max_delivery_count=10) -> FakeEntity`
  - `add_subscription(topic, name, *, sessions=False, partitioned=False, lock_seconds=60, max_delivery_count=10) -> FakeEntity`
  - `entity(path: str) -> FakeEntity` (`"q"`, `"q/$DeadLetterQueue"`, `"q/$Transfer/$DeadLetterQueue"`, `"t/Subscriptions/s"`, …)
  - flags: `auth_failure: bool` (data-plane ops raise `ServiceBusAuthenticationError`), `management_denied: bool` (admin ops raise `ClientAuthenticationError`), `management_error: Exception | None`
  - `inject_receive_error(error: Exception, *, on_call: int) -> None` (the N-th `receive_messages`, 1-based, across receivers, raises once)
  - `inject_body_error(sequence_number: int, *, times: int) -> None` (`.body` raises `TypeError("injected decode failure")` that many times)
  - `inject_commit_errors(errors: list[Exception]) -> None` (successive RAD `receive_deferred_messages` calls raise these first)
  - `calls: list[tuple[str, str]]` (operation, entity path); `clients: list[FakeServiceBusClient]`; `receivers: list[FakeReceiver]`
- `FakeEntity`: `path`, `requires_session`, `partitioned`, `lock_seconds`, `max_delivery_count`, `dead_letter: FakeEntity`, `transfer_dead_letter: FakeEntity`;
  `send(body=b"", *, body_type="DATA", session_id=None, message_id=None, enqueued_at=None, scheduled_at=None, ttl_seconds=None, partition=0, content_type=None, application_properties=None, subject=None, correlation_id=None, **extra) -> int` (returns the sequence number; `enqueued_at` defaults to `clock.now()`; `**extra` overrides any stored attribute, e.g. `delivery_count=1`, `to`, `reply_to`, `dead_letter_reason`);
  `defer_existing(seq)`, `lock_existing(seq, seconds)`, `dead_letter_existing(seq, reason, description)`;
  `state_of(seq) -> str | None` (`"ACTIVE"` / `"DEFERRED"` / `"SCHEDULED"` / `None` when gone); `sequence_numbers() -> list[int]`; `delivery_count(seq) -> int`.
- `FakeServiceBusClient` (class attribute `broker`), `FakeReceiver`, `FakeSession`, `FakeAdminClient`, `FakeCredential`, `FakeReceivedMessage`.
- `make_message(body: bytes | str | dict | list = b"", *, body_type: str = "DATA", sequence_number: int = 1, **attrs) -> FakeReceivedMessage` — a standalone message for unit tests (same encoding rules as `FakeEntity.send`: DATA `bytes` / `str` → one section, a list of `bytes` → several sections; VALUE → any value, bytes-encoded recursively; SEQUENCE → a list of lists; `attrs` override any attribute, e.g. `content_type`, `state`, `delivery_count`, `enqueued_time_utc`, `expires_at_utc`, `application_properties`, `body_error=True` makes `.body` raise `TypeError`).
- `install(monkeypatch, broker: FakeBroker) -> None` — patches `client.ServiceBusClient`, `client.ServiceBusAdministrationClient`, `client.ClientSecretCredential`.
- `tests/conftest.py`: fixture `broker(monkeypatch) -> FakeBroker` that builds a `FakeBroker`, calls `install`, and returns it.

**Semantics the fake must model** (from the spec's [live] evidence; each has a self-test):

| Behaviour | Fake rule |
|---|---|
| sequence numbers | per top-level entity counter from 1; partitioned: `((51 + partition) << 48) \| n`; sub-queues keep the original number |
| PEEK_LOCK receive | available = `ACTIVE`, unlocked (or lock expired → unlock + `delivery_count += 1` first), `scheduled_at <= now`, not expired; in sequence order (partitioned: round-robin partitions); locks for `lock_seconds`; returns ≤ `max_message_count`; empty → `[]` without sleeping |
| RECEIVE_AND_DELETE receive | as above but removes immediately |
| `complete` / `defer` / `abandon` / `dead_letter` | `MessageLockLostError` if `locked_until < now`; complete removes; defer → `DEFERRED`, unlocked; abandon → unlocked, `delivery_count += 1`; dead-letter → moved to `dead_letter` with reason/description; on a DLQ receiver dead-letter is **silently ignored** |
| peek | `min(max_message_count, 250)` records with `seq >= sequence_number` (`sequence_number=0` → receiver's last peeked + 1, else 1) in seq order, **including** `DEFERRED`, locked (peeks as stored: `ACTIVE`, `delivery_count` unchanged), expired-not-purged; `SCHEDULED` visible on queues only; nothing locked |
| `receive_deferred_messages` | all seqs must be `DEFERRED` in this entity/sub-queue (+ session), else `MessageNotFoundError` for the whole call; partitioned + mixed partitions → `ServiceBusError("ReceiveBatch of sequence numbers from different partitions is not supported for an entity with partitioning enabled.")`; RAD + `len > 250` → `ServiceBusError("ReceiveAndDelete only can process 250 deferred messages")`, nothing deleted; RAD removes; PEEK_LOCK locks and `delivery_count += 1` (state stays `DEFERRED`) |
| sessions | `session_id=NEXT_AVAILABLE_SESSION` → first session (by id) with an available `ACTIVE` message not locked by another open receiver; none → `OperationTimeoutError`; deferred-only sessions never handed out; a session receiver sees only its session; explicit `session_id` locked by another open receiver → `SessionCannotBeLockedError`; non-session receiver on a session entity → `ServiceBusError("It is not possible for an entity that requires sessions to create a non-sessionful message receiver")` on first op; `receiver.session.renew_lock()` extends `locked_until_utc` |
| auth / entities | a connection string carrying `EntityPath=<x>` → `get_queue_receiver` / `get_subscription_receiver` for another entity raise `ValueError` (real SDK behaviour); `auth_failure` → `ServiceBusAuthenticationError` on first op; unknown queue via SAS → `ServiceBusAuthenticationError`; unknown subscription → `MessagingEntityNotFoundError` |
| `from_connection_string` | validates through the **real** SDK parser offline (writer trick), then returns the fake |
| management | `list_queues()` / `list_topics()` / `list_subscriptions(topic)` return objects with `.name`; `get_queue` / `get_subscription` return `requires_session`, `enable_partitioning` (subscriptions: from the topic), `lock_duration: timedelta`, `max_delivery_count`; `get_*_runtime_properties` return `active_message_count`, `dead_letter_message_count`, `scheduled_message_count`, `transfer_dead_letter_message_count`; `list_rules(topic, sub)` → `.name`, `.filter.sql_expression`; unknown topic → `ResourceNotFoundError`; `management_denied` → `ClientAuthenticationError` |
| recording | `FakeServiceBusClient.kwargs`, `FakeReceiver.kwargs` (`receive_mode`, `prefetch_count`, `keep_alive`, `sub_queue`, `session_id`, `client_identifier`, `max_wait_time`), `FakeReceiver.closed`; `broker.calls` gets `(op, path)` for every data-plane operation (`receive_messages`, `peek_messages`, `receive_deferred_messages`, `complete_message`, `abandon_message`, `defer_message`, `dead_letter_message`, `renew_message_lock`, `session_renew_lock`) |
| peeked `SCHEDULED` messages | `enqueued_time_utc` = the scheduled time (models the [inferred] case the watermark must ignore) |

`FakeReceivedMessage` exposes the real attribute names with the real value types (§5.1 of the research, spec §6.9): `message_id`, `sequence_number`, `enqueued_sequence_number`, `enqueued_time_utc` (aware UTC), `content_type`, `correlation_id`, `subject`, `session_id`, `reply_to`, `reply_to_session_id`, `to`, `partition_key`, `application_properties` (**bytes keys and bytes string values**), `delivery_count`, `dead_letter_reason`, `dead_letter_error_description`, `dead_letter_source`, `time_to_live` (`timedelta | None`), `expires_at_utc`, `scheduled_enqueue_time_utc`, `state` (`ServiceBusMessageState`), `body_type` (`AmqpMessageBodyType`), `body` (DATA → a **generator of bytes sections**; VALUE → recursively bytes-encoded value; SEQUENCE → list of lists with bytes strings), `raw_amqp_message` (`.annotations` dict, `.header` with `durable`, `priority`, `first_acquirer`, `.properties` with `user_id: bytes`, `content_encoding`, `creation_time` / `absolute_expiry_time` as epoch-ms ints, `group_sequence`, `reply_to_group_id`), `locked_until_utc`, `lock_token`.

- [ ] **Step 1: Write the failing self-tests** — `tests/fakes/test_broker.py`:

```python
import pytest
from azure.servicebus import NEXT_AVAILABLE_SESSION, ServiceBusReceiveMode
from azure.servicebus.exceptions import (
    MessageLockLostError,
    MessageNotFoundError,
    OperationTimeoutError,
    ServiceBusError,
)

import client as client_mod


def sas_client():
    return client_mod.ServiceBusClient.from_connection_string(
        conn_str="Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"
    )


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
    assert [(m.sequence_number, m.state.name, m.delivery_count) for m in peeked] == [(a, "DEFERRED", 0), (b, "ACTIVE", 0)]


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
    with sas_client().get_queue_receiver("q", receive_mode=rad, prefetch_count=1, keep_alive=0) as r:
        with pytest.raises(ServiceBusError, match="250"):
            r.receive_deferred_messages(seqs)
    assert len(q.sequence_numbers()) == 251


def test_partitioned_mixed_partitions_rejected(broker):
    q = broker.add_queue("p", partitioned=True)
    a, b = q.send(b"x", partition=0), q.send(b"y", partition=1)
    q.defer_existing(a)
    q.defer_existing(b)
    rad = ServiceBusReceiveMode.RECEIVE_AND_DELETE
    with sas_client().get_queue_receiver("p", receive_mode=rad, prefetch_count=1, keep_alive=0) as r:
        with pytest.raises(ServiceBusError, match="different partitions"):
            r.receive_deferred_messages([a, b])
    assert a >> 48 != b >> 48


def test_sessions_next_available_and_timeout(broker):
    q = broker.add_queue("s", sessions=True)
    q.send(b"a", session_id="A")
    with sas_client().get_queue_receiver("s", session_id=NEXT_AVAILABLE_SESSION, prefetch_count=1, keep_alive=0) as r:
        assert r.session.session_id == "A"
        r.complete_message(r.receive_messages()[0])
    with pytest.raises(OperationTimeoutError):
        with sas_client().get_queue_receiver("s", session_id=NEXT_AVAILABLE_SESSION, prefetch_count=1, keep_alive=0) as r:
            r.receive_messages()


def test_dead_letter_moves_and_is_ignored_on_dlq(broker):
    q = broker.add_queue("q")
    seq = q.send(b"a")
    with sas_client().get_queue_receiver("q", prefetch_count=1, keep_alive=0) as r:
        r.dead_letter_message(r.receive_messages()[0], reason="UnreadableBody", error_description="TypeError")
    assert q.dead_letter.state_of(seq) == "ACTIVE"
    from azure.servicebus import ServiceBusSubQueue

    with sas_client().get_queue_receiver("q", sub_queue=ServiceBusSubQueue.DEAD_LETTER, prefetch_count=1, keep_alive=0) as r:
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
    admin = client_mod.ServiceBusAdministrationClient.from_connection_string("Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0")
    with pytest.raises(ClientAuthenticationError):
        list(admin.list_queues())
```

- [ ] **Step 2:** `uv run pytest tests/fakes -v` → FAIL (no fixture / module).
- [ ] **Step 3: Implement** `tests/fakes/broker.py` per the semantics table (plain classes, no threads; time only via `FakeClock`; `OperationTimeoutError` raised immediately instead of waiting) and `tests/conftest.py`:

```python
import pytest

from tests.fakes.broker import FakeBroker, install


@pytest.fixture
def broker(monkeypatch) -> FakeBroker:
    fake = FakeBroker()
    install(monkeypatch, fake)
    return fake
```

  Add `"tests"`'s parent to the import path by keeping `pythonpath = ["src", "."]` in `[tool.pytest.ini_options]` so `tests.fakes` imports.
- [ ] **Step 4:** `uv run pytest tests/fakes -v` → PASS; ruff clean.
- [ ] **Step 5:** Commit: `test: FakeBroker SDK double modelling Service Bus receive/peek/defer/session semantics`.

---

### Task 5: Entity reference, metadata, management helpers

**Owner skill:** `component-develop`.

**Files:**
- Create: `src/entity.py`
- Test: `tests/unit/test_entity.py`

**Interfaces:**
- Consumes: `SourceConfig`, `EntityType`, `SubQueue` (Task 2); `ServiceBusConnector`, `is_management_denied` (Task 3); `FakeBroker` (Task 4, tests).
- Produces:
  - `partition_of(sequence_number: int) -> int` (`>> 48`).
  - `@dataclass(frozen=True) class EntityRef`: `entity_type: EntityType`, `queue_name: str | None`, `topic_name: str | None`, `subscription_name: str | None`, `sub_queue: SubQueue`; `from_source(source: SourceConfig) -> EntityRef`; `from_dict(d: dict) -> EntityRef`; `to_dict() -> dict`; `path -> str` (`orders`, `orders/$DeadLetterQueue`, `orders/$Transfer/$DeadLetterQueue`, `t/Subscriptions/s[...]`); `main_path -> str` (without sub-queue); `is_sub_queue -> bool`; `open_receiver(client, **kwargs) -> ServiceBusReceiver` (calls `get_queue_receiver(queue_name=…)` or `get_subscription_receiver(topic_name=…, subscription_name=…)` adding `sub_queue=ServiceBusSubQueue.DEAD_LETTER / TRANSFER_DEAD_LETTER` when set; a `ValueError` from the SDK — the connection string's `EntityPath` names another entity — becomes `UserException(f"The connection string is scoped to another entity (EntityPath) than '{path}'. Use a namespace-level connection string or select the entity named in it. (details: <redacted>)")`, the only `ValueError` mapping outside the connector); `default_table_name() -> str`.
  - `@dataclass class EntityInfo`: `requires_session: bool | None = None`, `partitioned: bool | None = None`, `lock_duration_seconds: float = 60.0`, `max_delivery_count: int = 10`, `counts: dict[str, int] | None = None`, `seen_partitioned: bool = False`; `note_sequence_number(seq: int) -> None` (sets `seen_partitioned` when `partition_of(seq) != 0`); `is_partitioned -> bool` (`partitioned is True or seen_partitioned`); `orphan_guard_threshold -> int` (`max_delivery_count - 1`).
  - `load_entity_info(connector: ServiceBusConnector, entity: EntityRef) -> EntityInfo` — management `get_queue` / `get_subscription` (+ `get_topic` for subscription partitioning) and runtime properties (`counts` keys `active`, `dead_letter`, `scheduled`, `transfer_dead_letter`); **any** exception → DEBUG log + `EntityInfo()` defaults.
  - `log_entity_counts(info: EntityInfo, entity: EntityRef) -> None` — L1: one INFO line `"Entity '<path>' holds <a> active, <d> dead-lettered, <s> scheduled and <t> transfer-dead-lettered message(s)."`; nothing when `info.counts is None`.
  - `list_entity_names(connector, kind: Literal["queues", "topics", "subscriptions"], topic_name: str | None = None) -> list[str]` — sorted names; `is_management_denied` and `auth_type == connection_string` → `[]`; other errors → `UserException` via `to_user_exception`.
  - `probe_management(connector) -> None` — lists the first queue; `is_management_denied` → `UserException` (SAS: "A connection string with only Listen rights can be tested only from a row that has a source selected."; SP: "The service principal cannot read the namespace: grant it the 'Azure Service Bus Data Receiver' role."); other errors → `to_user_exception`.
  - `describe_entity(connector, entity: EntityRef) -> str` — markdown bullet list (requires session, partitioning, lock duration, max delivery count, the four counts, and for subscriptions each rule `name: filter`); `is_management_denied` → `UserException("Entity details need a connection string with Manage rights or a service principal with the 'Azure Service Bus Data Receiver' role.")`.

- [ ] **Step 1: Write the failing tests** — `tests/unit/test_entity.py`:

```python
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
            **{"auth_type": auth_type, "tenant_id": "t", "client_id": "c", "#client_secret": "x",
               "fully_qualified_namespace": "ns.servicebus.windows.net"}
        )
    return ServiceBusConnector(auth, "kbc-test")


@pytest.mark.parametrize(
    "source, path, table",
    [
        ({"entity_type": "queue", "queue_name": "orders"}, "orders", "orders"),
        ({"entity_type": "queue", "queue_name": "orders", "sub_queue": "dead_letter"}, "orders/$DeadLetterQueue", "orders_dead_letter"),
        ({"entity_type": "queue", "queue_name": "orders", "sub_queue": "transfer_dead_letter"}, "orders/$Transfer/$DeadLetterQueue", "orders_transfer_dead_letter"),
        ({"entity_type": "subscription", "topic_name": "ev", "subscription_name": "audit"}, "ev/Subscriptions/audit", "ev_audit"),
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
    assert info.counts["active"] == 1


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
```

- [ ] **Step 2:** `uv run pytest tests/unit/test_entity.py -v` → FAIL.
- [ ] **Step 3: Implement** `src/entity.py` per the interface. Table-name derivation: join parts with `_`, `re.sub(r"[^A-Za-z0-9_-]", "_", name).strip("_-")`; an empty result → `"messages"`.
- [ ] **Step 4:** PASS; ruff clean.
- [ ] **Step 5:** Commit: `feat: entity reference, management metadata with heuristic fallback, list/describe helpers`.

---

### Task 6: State model, range encoding, pending-set builder

**Owner skill:** `component-develop`.

**Files:**
- Create: `src/state.py`
- Test: `tests/unit/test_state.py`

**Interfaces:**
- Consumes: `EntityRef`, `partition_of` (Task 5).
- Produces:
  - `STATE_VERSION = 1`, `STATE_BUDGET_BYTES = 256 * 1024`.
  - `to_ranges(seqs: Iterable[int]) -> list[list[int]]` (sorted, deduplicated, inclusive `[start, end]`); `from_ranges(ranges: list[list[int]]) -> list[int]`.
  - Pydantic models: `PendingGroup(session_id: str | None, partition: int, max_body_bytes: int, ranges: list[list[int]])` with `sequence_numbers() -> list[int]`; `PendingEntity(entity: dict, groups: list[PendingGroup], deferred_at_utc: str)` with `entity_ref() -> EntityRef`; `PeekCursor(entity_path: str, last_sequence_number: int)`; `FlattenColumn(path: list[str], column: str)`; `ExtractorState(version: int = 1, pending_commit: list[PendingEntity] = [], peek_cursor: PeekCursor | None = None, flatten_columns: list[FlattenColumn] = [])` with `@classmethod load(raw: dict | None) -> ExtractorState` (empty → defaults; `version != 1` → `UserException(f"Unsupported state version {version}: this component understands state version 1. Reset the row state; the next run starts without a cursor, pending set or column registry.")`; unknown keys ignored), `to_dict() -> dict`, `size_bytes() -> int` (compact JSON length).
  - `class PendingSetBuilder`: `__init__(self)`; `add(entity: EntityRef, sequence_number: int, session_id: str | None, body_bytes: int) -> None`; `carry(entities: list[PendingEntity]) -> None`; `build(deferred_at_utc: str) -> list[PendingEntity]`; `count -> int`; `encoded_size(deferred_at_utc: str) -> int`.

- [ ] **Step 1: Write the failing tests** — `tests/unit/test_state.py`:

```python
import pytest
from keboola.component.exceptions import UserException

from configuration import EntityType, SubQueue
from entity import EntityRef
from state import ExtractorState, PendingSetBuilder, from_ranges, to_ranges

Q = EntityRef(EntityType.QUEUE, "orders", None, None, SubQueue.NONE)


def test_ranges_roundtrip():
    assert to_ranges([5, 1, 2, 3, 3, 9, 10]) == [[1, 3], [5, 5], [9, 10]]
    assert from_ranges([[1, 3], [5, 5]]) == [1, 2, 3, 5]
    assert to_ranges([]) == []


def test_load_defaults_and_roundtrip():
    s = ExtractorState.load(None)
    assert s.pending_commit == [] and s.peek_cursor is None and s.flatten_columns == []
    assert ExtractorState.load(s.to_dict()).to_dict() == s.to_dict()


def test_unknown_version_rejected():
    with pytest.raises(UserException, match="reset"):
        ExtractorState.load({"version": 99})


def test_unknown_keys_ignored():
    assert ExtractorState.load({"version": 1, "legacy": 1}).version == 1


def test_builder_groups_by_session_and_partition():
    b = PendingSetBuilder()
    b.add(Q, 1, None, 10)
    b.add(Q, 2, None, 30)
    b.add(Q, (52 << 48) | 1, None, 5)
    b.add(Q, 3, "s1", 7)
    built = b.build("2026-09-23 10:00:00.000000")
    assert len(built) == 1 and built[0].entity_ref() == Q
    groups = {(g.session_id, g.partition): g for g in built[0].groups}
    assert groups[(None, 0)].ranges == [[1, 2]] and groups[(None, 0)].max_body_bytes == 30
    assert groups[(None, 52)].sequence_numbers() == [(52 << 48) | 1]
    assert groups[("s1", 0)].sequence_numbers() == [3]
    assert b.count == 4


def test_builder_carry_forward_merges():
    b = PendingSetBuilder()
    b.add(Q, 10, None, 1)
    first = b.build("t")
    b2 = PendingSetBuilder()
    b2.carry(first)
    b2.add(Q, 11, None, 4)
    assert b2.build("t")[0].groups[0].ranges == [[10, 11]]


def test_size_is_small_for_contiguous_ranges():
    b = PendingSetBuilder()
    for seq in range(1, 100_001):
        b.add(Q, seq, None, 100)
    state = ExtractorState(pending_commit=b.build("t"))
    assert state.size_bytes() < 1_000
```

- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** `src/state.py`. `to_ranges`: sort unique, walk and extend while `seq == end + 1`. `PendingSetBuilder` keeps `dict[(entity_key, session_id, partition)] -> (set[int], max_bytes)` where `entity_key = json.dumps(entity.to_dict(), sort_keys=True)`; `build` emits one `PendingEntity` per entity key with groups sorted by `(session_id or "", partition)`. `encoded_size` = `len(json.dumps([p.model_dump() for p in self.build(ts)], separators=(",", ":")))`.
- [ ] **Step 4:** PASS; ruff clean.
- [ ] **Step 5:** Commit: `feat: versioned row state with range-encoded pending-commit set`.

---

### Task 7: Body decoding and JSON flattening

**Owner skill:** `component-develop`.

**Files:**
- Create: `src/body.py`
- Test: `tests/unit/test_body.py`

**Interfaces:**
- Consumes: `BodyFormat` (Task 2), `FlattenColumn` (Task 6), `make_message` (Task 4, tests).
- Produces:
  - `CELL_LIMIT_BYTES = 16 * 1024 * 1024`, `MAX_FLATTEN_COLUMNS = 1000`, `MAX_COLUMN_NAME = 64`, `FLATTEN_PREFIX = "body_"`, `ROOT_VALUE_COLUMN = "body_value"`, `UNMAPPED_COLUMN = "body_unmapped"` (reserved; spec §6.10).
  - Exceptions: `BodyDecodeError(Exception)` (body access / decoding raised — wraps the cause), `NotJsonError(Exception)`, `BodyTooLargeError(Exception)`, `FlattenColumnCapError(Exception)`.
  - `@dataclass class EncodedBody`: `body_type: str`, `size_bytes: int`, `cell: str | None`, `fields: dict[tuple[str, ...], str] | None`.
  - `charset_of(content_type: str | None) -> str` (the `charset=` parameter if Python knows it, else `"utf-8"`).
  - `to_jsonable(value: object) -> object` (recursive: `bytes` → UTF-8 str with replacement, `dict` keys too; `datetime` → ISO-8601; `Decimal` kept; `uuid.UUID` → str; other non-JSON scalars → `str()`).
  - `compact_json(value: object) -> str` (`json.dumps(to_jsonable(value), ensure_ascii=False, separators=(",", ":"), default=str)`).
  - `encode_body(message, body_format: BodyFormat) -> EncodedBody`.
  - `flatten_value(value: object) -> dict[tuple[str, ...], str]` (root non-object → `{(): compact_json(value)}`).
  - `column_name_for(path: tuple[str, ...]) -> str` (no collision handling).
  - `class FlattenRegistry`: `__init__(self, existing: list[FlattenColumn], reserved: Iterable[str])` (`UNMAPPED_COLUMN` is always reserved); `register(path: tuple[str, ...]) -> str` (the stable name — existing or newly assigned; cap check); `input_columns -> list[str]` (the entries loaded from state, in order); `new_columns -> list[str]` (registered in this run); `columns -> list[str]` (input + new); `to_state() -> list[FlattenColumn]` (all, for the output state); `split(fields: dict[tuple[str, ...], str], *, promote: bool) -> tuple[dict[str, str], str]` — values for the materialised columns (input columns, plus new ones when `promote`) and the `body_unmapped` cell: compact JSON `{"<dot.path>": "<value>"}` of every other field (root path `()` → key `"$"`), `""` when none.

- [ ] **Step 1: Write the failing tests** — `tests/unit/test_body.py`:

```python
import base64
import json

import pytest

import body as body_mod
from body import (
    BodyDecodeError,
    BodyTooLargeError,
    FlattenColumnCapError,
    FlattenRegistry,
    NotJsonError,
    charset_of,
    column_name_for,
    encode_body,
    flatten_value,
)
from configuration import BodyFormat
from state import FlattenColumn
from tests.fakes.broker import make_message


def test_text_multisection_and_charset():
    m = make_message([b"p\xe9", b"x"], content_type="text/plain; charset=latin-1")
    enc = encode_body(m, BodyFormat.TEXT)
    assert enc.cell == "péx" and enc.body_type == "DATA" and enc.size_bytes == 3


def test_unknown_charset_falls_back_and_replaces():
    m = make_message(b"ok\xff", content_type="text/plain; charset=klingon")
    assert encode_body(m, BodyFormat.TEXT).cell == "ok�"
    assert charset_of("application/json") == "utf-8"


def test_base64_data_and_value():
    assert encode_body(make_message(b"\x00\x01"), BodyFormat.BASE64).cell == base64.b64encode(b"\x00\x01").decode()
    value = make_message({"k": "v"}, body_type="VALUE")
    assert base64.b64decode(encode_body(value, BodyFormat.BASE64).cell) == b'{"k":"v"}'


def test_value_and_sequence_as_json_text():
    assert encode_body(make_message({"k": "v", "n": 1}, body_type="VALUE"), BodyFormat.TEXT).cell == '{"k":"v","n":1}'
    assert encode_body(make_message([[1, "a"]], body_type="SEQUENCE"), BodyFormat.TEXT).cell == '[[1,"a"]]'


def test_body_access_error_is_body_decode_error():
    with pytest.raises(BodyDecodeError):
        encode_body(make_message(b"x", body_error=True), BodyFormat.TEXT)


def test_flatten_nested_arrays_scalars():
    raw = b'{"order":{"id":1,"items":[1,2],"meta":{}},"ok":true,"n":null,"price":1.10,"s":"x"}'
    fields = encode_body(make_message(raw), BodyFormat.JSON_FLATTEN).fields
    assert fields == {
        ("order", "id"): "1",
        ("order", "items"): "[1,2]",
        ("order", "meta"): "{}",
        ("ok",): "true",
        ("n",): "",
        ("price",): "1.10",
        ("s",): "x",
    }


def test_flatten_value_body_without_parsing():
    assert encode_body(make_message({"a": {"b": "c"}}, body_type="VALUE"), BodyFormat.JSON_FLATTEN).fields == {("a", "b"): "c"}


def test_flatten_root_array():
    assert flatten_value([1, 2]) == {(): "[1,2]"}


@pytest.mark.parametrize("raw", [b"not json", b"\xff\xfe{", b""])
def test_flatten_not_json(raw):
    with pytest.raises(NotJsonError):
        encode_body(make_message(raw), BodyFormat.JSON_FLATTEN)


def test_too_large(monkeypatch):
    monkeypatch.setattr(body_mod, "CELL_LIMIT_BYTES", 4)
    with pytest.raises(BodyTooLargeError):
        encode_body(make_message(b"12345"), BodyFormat.TEXT)
    with pytest.raises(BodyTooLargeError):
        encode_body(make_message(b'{"a":"12345"}'), BodyFormat.JSON_FLATTEN)


@pytest.mark.parametrize(
    "path, name",
    [
        (("order", "customer", "id"), "body_order_customer_id"),
        (("Čena",), "body_Cena"),
        (("a.b",), "body_a_b"),
        (("x_",), "body_x"),
        (("a-b",), "body_a_b"),
        ((), "body_value"),
    ],
)
def test_column_names(path, name):
    assert column_name_for(path) == name


def test_long_column_name_truncated_with_hash():
    name = column_name_for(("k" * 100,))
    assert len(name) == 64 and name.startswith("body_kkk") and name[55] == "_"
    assert name == column_name_for(("k" * 100,))


def test_registry_collisions_and_stability():
    reg = FlattenRegistry([], reserved={"message_id"})
    assert reg.register(("a", "b")) == "body_a_b"
    assert reg.register(("a.b",)) == "body_a_b_2"
    assert reg.register(("a", "b")) == "body_a_b"
    restored = FlattenRegistry(reg.to_state(), reserved={"message_id"})
    assert restored.register(("a.b",)) == "body_a_b_2"
    assert restored.input_columns == ["body_a_b", "body_a_b_2"] and restored.new_columns == []


def test_registry_restores_from_state_models():
    reg = FlattenRegistry([FlattenColumn(path=["x"], column="body_x")], reserved=set())
    assert reg.register(("x",)) == "body_x" and reg.columns == ["body_x"] and reg.input_columns == ["body_x"]


def test_unmapped_is_reserved():
    assert FlattenRegistry([], reserved=set()).register(("unmapped",)) == "body_unmapped_2"


def test_split_promotion_and_unmapped():
    reg = FlattenRegistry([FlattenColumn(path=["x"], column="body_x")], reserved=set())
    fields = {("x",): "1", ("y",): "2", ("o", "k"): "3"}
    for path in fields:
        reg.register(path)
    assert reg.new_columns == ["body_y", "body_o_k"]
    values, unmapped = reg.split(fields, promote=False)
    assert values == {"body_x": "1"} and json.loads(unmapped) == {"y": "2", "o.k": "3"}
    values, unmapped = reg.split(fields, promote=True)
    assert values == {"body_x": "1", "body_y": "2", "body_o_k": "3"} and unmapped == ""
    reg.register(())
    assert json.loads(reg.split({(): "[1]"}, promote=False)[1]) == {"$": "[1]"}


def test_registry_cap(monkeypatch):
    monkeypatch.setattr(body_mod, "MAX_FLATTEN_COLUMNS", 2)
    reg = FlattenRegistry([], reserved=set())
    reg.register(("a",))
    reg.register(("b",))
    with pytest.raises(FlattenColumnCapError):
        reg.register(("c",))


def test_compact_json_bytes_keys():
    assert json.loads(body_mod.compact_json({b"k": b"v"})) == {"k": "v"}
```

- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** `src/body.py`:
  - Body access: `sections = message.body` inside `try … except Exception as e: raise BodyDecodeError(f"{type(e).__name__}: {e}") from e`; DATA → `raw = b"".join(sections)`; VALUE / SEQUENCE → `to_jsonable(message.body)`; `body_type = message.body_type.name` (or `str(...)` fallback).
  - TEXT: DATA → `raw.decode(charset_of(content_type), errors="replace")`; VALUE / SEQUENCE → `compact_json`. BASE64: DATA → `base64.b64encode(raw).decode("ascii")`; VALUE / SEQUENCE → base64 of `compact_json(...).encode("utf-8")`. Cell over `CELL_LIMIT_BYTES` (UTF-8 length) → `BodyTooLargeError`.
  - JSON_FLATTEN: DATA → `text = raw.decode(charset, errors="strict")` (UnicodeDecodeError → `NotJsonError`), `json.loads(text, parse_float=Decimal)` (`ValueError` → `NotJsonError`); VALUE → value; SEQUENCE → list. `flatten_value`: dict → recurse per key (keys as `str`); non-empty dict leaf recursion; empty dict → `"{}"`; list → `compact_json`; `str` as-is; `bool` → `"true"`/`"false"` (check `bool` before `int`); `None` → `""`; `int` / `Decimal` → `str(v)`; any field over the cell limit → `BodyTooLargeError`.
  - `column_name_for`: `() → ROOT_VALUE_COLUMN`; else `raw = FLATTEN_PREFIX + "_".join(path)`; `unicodedata.normalize("NFKD", raw)`, drop combining marks, `re.sub(r"[^A-Za-z0-9_]", "_", …)`, `.rstrip("_")`; if `len > 64`: `name[:55] + "_" + hashlib.sha1(json.dumps(list(path)).encode()).hexdigest()[:8]`.
  - `FlattenRegistry`: `_by_path: dict[tuple, str]`, `_input: set[tuple]` and `_taken: set[str]` seeded with `reserved`, `UNMAPPED_COLUMN` and the existing entries; `register` returns the known name or builds `column_name_for(path)` then appends `_2`, `_3`, … (truncating the base so the result stays ≤ 64) until free; raises `FlattenColumnCapError` when registering beyond `MAX_FLATTEN_COLUMNS` (read the module global at call time so the test's monkeypatch applies).
- [ ] **Step 4:** PASS; ruff clean.
- [ ] **Step 5:** Commit: `feat: body decoding (text/base64/value/sequence) and JSON flattening with stable column registry`.

---

### Task 8: Column catalogue, metadata mapping, value formats

**Owner skill:** `component-develop`.

**Files:**
- Create: `src/columns.py`
- Test: `tests/unit/test_columns.py`

**Interfaces:**
- Consumes: `EntityRef` (Task 5), `SettlementMode` (Task 2), `compact_json` (Task 7).
- Produces:
  - `METADATA_COLUMNS: tuple[tuple[str, str], ...]` — `(name, base_type)` in the exact order of spec §6.9 (base types `"STRING"`, `"INTEGER"`, `"FLOAT"`, `"BOOLEAN"`, `"TIMESTAMP"`), ending with `extracted_at_utc`; `BODY_COLUMN = "body"`; `metadata_column_names() -> list[str]`.
  - `format_timestamp(value: datetime | int | float | None) -> str` — aware / naive `datetime` → UTC `"%Y-%m-%d %H:%M:%S.%f"`; `int` / `float` = epoch **milliseconds**; `None` / `0` → `""`.
  - `message_metadata(message, *, entity: EntityRef, settlement_mode: SettlementMode, extracted_at: datetime) -> dict[str, str]`.
  - `render_preview(messages: Sequence, body_chars: int = 120) -> str` — markdown table with columns `Sequence Number | Enqueued (UTC) | Message ID | Subject | State | Body`; body text-decoded (errors replaced, newlines → spaces, `|` escaped) and cut to `body_chars`.

- [ ] **Step 1: Write the failing tests** — `tests/unit/test_columns.py`:

```python
import json
from datetime import UTC, datetime, timedelta

from azure.servicebus import ServiceBusMessageState

from columns import BODY_COLUMN, METADATA_COLUMNS, format_timestamp, message_metadata, metadata_column_names, render_preview
from configuration import EntityType, SettlementMode, SubQueue
from entity import EntityRef
from tests.fakes.broker import make_message

Q = EntityRef(EntityType.QUEUE, "orders", None, None, SubQueue.DEAD_LETTER)
NOW = datetime(2026, 9, 23, 10, 0, tzinfo=UTC)


def test_catalogue_order_and_types():
    names = metadata_column_names()
    assert names[0] == "sequence_number" and names[-1] == "extracted_at_utc"
    assert BODY_COLUMN not in names
    types = dict(METADATA_COLUMNS)
    assert types["sequence_number"] == "INTEGER" and types["enqueued_time_utc"] == "TIMESTAMP"
    assert types["time_to_live_seconds"] == "FLOAT" and types["amqp_durable"] == "BOOLEAN"
    assert types["application_properties"] == "STRING" and "to_address" in types and "to" not in types


def test_format_timestamp():
    assert format_timestamp(datetime(2026, 9, 23, 12, 0, tzinfo=UTC)) == "2026-09-23 12:00:00.000000"
    assert format_timestamp(1790157600000) == "2026-09-23 10:00:00.000000"
    assert format_timestamp(None) == "" and format_timestamp(0) == ""


def test_message_metadata_mapping():
    m = make_message(
        b"x",
        sequence_number=929,
        message_id="m-1",
        subject="created",
        to="dest",
        delivery_count=2,
        time_to_live=timedelta(seconds=30),
        application_properties={b"k": b"v", b"n": 1},
        dead_letter_reason="MaxDeliveryCountExceeded",
        state=ServiceBusMessageState.ACTIVE,
    )
    row = message_metadata(m, entity=Q, settlement_mode=SettlementMode.COMPLETE, extracted_at=NOW)
    assert list(row) == metadata_column_names()
    assert row["sequence_number"] == "929" and row["message_id"] == "m-1" and row["to_address"] == "dest"
    assert row["delivery_count"] == "2" and row["time_to_live_seconds"] == "30.0"
    assert json.loads(row["application_properties"]) == {"k": "v", "n": 1}
    assert row["dead_letter_reason"] == "MaxDeliveryCountExceeded" and row["state"] == "ACTIVE"
    assert row["source_entity"] == "orders/$DeadLetterQueue" and row["settlement_mode"] == "complete"
    assert row["extracted_at_utc"] == "2026-09-23 10:00:00.000000"
    assert row["amqp_durable"] in ("true", "false", "")


def test_render_preview():
    text = render_preview([make_message(b"a|b\n" + b"y" * 500, sequence_number=7, subject="s")])
    lines = text.splitlines()
    assert lines[0].startswith("| Sequence Number |") and "| 7 |" in lines[2]
    assert "a\\|b" in lines[2] and "y" * 121 not in lines[2]
```

- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** `src/columns.py`. Mapping: string attributes via `"" if v is None else str(v)`; integers via `str(int)`; `time_to_live` → `str(td.total_seconds())`; timestamps via `format_timestamp`; `application_properties` / `raw_amqp_message.annotations` → `compact_json(...)` or `""` when empty / `None`; `state` → `message.state.name` (peeked / received messages carry it); header / properties extras read defensively with `getattr(obj, name, None)` (`amqp_user_id` bytes → UTF-8 with replacement; booleans → `"true"`/`"false"`; missing → `""`).
- [ ] **Step 4:** PASS; ruff clean.
- [ ] **Step 5:** Commit: `feat: fixed metadata column catalogue, message mapping and preview rendering`.

---

### Task 9: Output table and manifest

**Owner skill:** `component-develop` (consult `keboola-context` native-data-types + output-mapping).

**Files:**
- Create: `src/output.py`
- Test: `tests/unit/test_output.py`

**Interfaces:**
- Consumes: `DestinationConfig`, `BodyFormat`, `PrimaryKey` (Task 2); `FlattenRegistry`, `UNMAPPED_COLUMN` (Task 7); `METADATA_COLUMNS`, `BODY_COLUMN`, `metadata_column_names` (Task 8).
- Produces:
  - `@dataclass(frozen=True) class OutputRow`: `metadata: dict[str, str]`, `body: str | None = None`, `fields: dict[tuple[str, ...], str] | None = None`.
  - `class RowSink(Protocol)`: `write_rows(self, rows: Sequence[OutputRow]) -> None`.
  - `class OutputTable` (implements `RowSink`, context manager): `__init__(self, *, table_name: str, destination: DestinationConfig, body_format: BodyFormat, registry: FlattenRegistry | None, create_definition: Callable[..., TableDefinition], write_manifest: Callable[[TableDefinition], None], staging_dir: Path = Path("/tmp"))`; `open() -> None`; `write_rows(rows) -> None`; `arm_write_always() -> None` (idempotent; rewrites the manifest with `write_always=True`); `write_always_armed: bool`; `close(success: bool = True) -> None` (idempotent); `rows_written: int`; `columns -> list[str]`; `__enter__` calls `open()` and returns `self`; `__exit__` calls `close(success=exc_type is None)`, logs (never raises) a close failure while another exception is in flight, and returns `False`.
  - `schema_for(columns: list[str]) -> OrderedDict[str, ColumnDefinition]` — metadata types from `METADATA_COLUMNS`, everything else `BaseType.string()`.

Behaviour (spec §6.9, §6.10):
- Every manifest: `create_definition(name=f"{table_name}.csv", schema=schema_for(columns), primary_key=destination.primary_key.columns, incremental=destination.incremental, write_always=self.write_always_armed, has_header=True)`; `write_always_armed` starts `False` — **only `arm_write_always()` sets it** (C1 calls it before its first complete, C3 before its first receive — Tasks 11 / 14).
- text / base64 → `open()` creates the CSV at the definition's `full_path`, writes the header (metadata columns + `body`) and **writes the manifest immediately**; `write_rows` appends and flushes (`f.flush()` + `os.fsync`).
- flatten → `open()` writes a header-only CSV and a manifest for `metadata + registry.input_columns + [UNMAPPED_COLUMN]`; `write_rows` stages each row as one JSON line `{"metadata": {...}, "fields": [[path_list, value], ...]}` in `staging_dir / f"{table_name}.jsonl"`; `close(success)` decides **`promote = success and not self.write_always_armed`**, then builds the CSV under `staging_dir` with header `metadata + (registry.columns if promote else registry.input_columns) + [UNMAPPED_COLUMN]`, each row's values from `registry.split(fields, promote=promote)`, `shutil.copyfile`s it over the output file (no scratch file under `/data/out/tables/`), deletes the staging file and rewrites the manifest for the same columns.

- [ ] **Step 1: Write the failing tests** — `tests/unit/test_output.py`:

```python
import csv
import json
from pathlib import Path

import pytest

from body import UNMAPPED_COLUMN, FlattenRegistry
from columns import metadata_column_names
from configuration import BodyFormat, DestinationConfig
from output import OutputRow, OutputTable, schema_for
from state import FlattenColumn


class ManifestSpy:
    def __init__(self, out_dir: Path) -> None:
        self.out_dir = out_dir
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        from keboola.component.dao import TableDefinition

        return TableDefinition(
            name=kwargs["name"], full_path=str(self.out_dir / kwargs["name"]), schema=kwargs["schema"],
            primary_key=kwargs["primary_key"], incremental=kwargs["incremental"],
            write_always=kwargs["write_always"], has_header=kwargs["has_header"],
        )

    def write(self, definition) -> None:
        payload = {"columns": list(definition.schema), "write_always": definition.write_always}
        (self.out_dir / f"{definition.name}.manifest").write_text(json.dumps(payload))

    def manifest(self) -> dict:
        return json.loads((self.out_dir / "orders.csv.manifest").read_text())


def meta(seq: int) -> dict[str, str]:
    base = {name: "" for name in metadata_column_names()}
    base["sequence_number"] = str(seq)
    return base


def make(tmp_path, body_format, registry=None, destination=None):
    spy = ManifestSpy(tmp_path)
    table = OutputTable(
        table_name="orders", destination=destination or DestinationConfig(), body_format=body_format,
        registry=registry, create_definition=spy.create, write_manifest=spy.write, staging_dir=tmp_path / "stage",
    )
    return table, spy


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def header(path: Path) -> list[str]:
    with path.open(newline="") as f:
        return next(csv.reader(f))


def registry(existing=()):
    return FlattenRegistry([FlattenColumn(path=list(p), column=c) for p, c in existing], reserved=set(metadata_column_names()))


def test_text_mode_manifest_first_write_always_false_then_armed(tmp_path):
    table, spy = make(tmp_path, BodyFormat.TEXT)
    with table:
        assert spy.manifest()["write_always"] is False  # written before anything is settled
        first = spy.calls[0]
        assert first["has_header"] is True and first["incremental"] is True
        assert first["primary_key"] == ["sequence_number"]
        table.write_rows([OutputRow(meta(1), body="a"), OutputRow(meta(2), body="b")])
        table.arm_write_always()
        assert spy.manifest()["write_always"] is True
        assert [r["body"] for r in read_csv(tmp_path / "orders.csv")] == ["a", "b"]
    assert table.rows_written == 2 and spy.manifest()["write_always"] is True


def test_never_armed_stays_false(tmp_path):
    table, spy = make(tmp_path, BodyFormat.TEXT)
    with pytest.raises(RuntimeError):
        with table:
            table.write_rows([OutputRow(meta(1), body="a")])
            raise RuntimeError("boom")
    assert spy.manifest()["write_always"] is False


def test_flatten_success_not_armed_promotes_new_keys(tmp_path):
    reg = registry([(("x",), "body_x")])
    table, _ = make(tmp_path, BodyFormat.JSON_FLATTEN, registry=reg)
    with table:
        reg.register(("y",))
        table.write_rows([OutputRow(meta(1), fields={("y",): "2"})])
    cols = header(tmp_path / "orders.csv")
    assert cols[-3:] == ["body_x", "body_y", UNMAPPED_COLUMN] and "body" not in cols
    row = read_csv(tmp_path / "orders.csv")[0]
    assert row["body_x"] == "" and row["body_y"] == "2" and row[UNMAPPED_COLUMN] == ""


def test_flatten_armed_success_defers_promotion(tmp_path):
    reg = registry([(("x",), "body_x")])
    table, spy = make(tmp_path, BodyFormat.JSON_FLATTEN, registry=reg)
    with table:
        reg.register(("y",))
        table.arm_write_always()
        table.write_rows([OutputRow(meta(1), fields={("x",): "1", ("y",): "2"})])
    cols = header(tmp_path / "orders.csv")
    assert "body_y" not in cols and cols[-2:] == ["body_x", UNMAPPED_COLUMN]
    row = read_csv(tmp_path / "orders.csv")[0]
    assert row["body_x"] == "1" and json.loads(row[UNMAPPED_COLUMN]) == {"y": "2"}
    assert spy.manifest()["columns"][-2:] == ["body_x", UNMAPPED_COLUMN]
    assert [c.column for c in reg.to_state()] == ["body_x", "body_y"]  # saved for the next run


def test_flatten_failed_run_puts_new_keys_in_unmapped(tmp_path):
    reg = registry()
    table, _ = make(tmp_path, BodyFormat.JSON_FLATTEN, registry=reg)
    with pytest.raises(RuntimeError):
        with table:
            reg.register(("a",))
            table.write_rows([OutputRow(meta(1), fields={("a",): "1"})])
            raise RuntimeError("boom")
    assert header(tmp_path / "orders.csv")[-1] == UNMAPPED_COLUMN and "body_a" not in header(tmp_path / "orders.csv")
    assert json.loads(read_csv(tmp_path / "orders.csv")[0][UNMAPPED_COLUMN]) == {"a": "1"}


def test_input_registry_columns_always_written(tmp_path):
    reg = registry([(("x",), "body_x"), (("z",), "body_z")])
    table, _ = make(tmp_path, BodyFormat.JSON_FLATTEN, registry=reg)
    with table:
        table.write_rows([OutputRow(meta(1), fields={("x",): "1"})])
    assert header(tmp_path / "orders.csv")[-3:] == ["body_x", "body_z", UNMAPPED_COLUMN]


def test_empty_run_writes_header_only(tmp_path):
    table, _ = make(tmp_path, BodyFormat.TEXT)
    with table:
        pass
    assert read_csv(tmp_path / "orders.csv") == [] and header(tmp_path / "orders.csv")[-1] == "body"


def test_full_load_and_composite_pk(tmp_path):
    dest = DestinationConfig(load_type="full_load", primary_key="source_entity_sequence_number")
    table, spy = make(tmp_path, BodyFormat.TEXT, destination=dest)
    with table:
        pass
    assert spy.calls[0]["incremental"] is False
    assert spy.calls[0]["primary_key"] == ["source_entity", "sequence_number"]


def test_schema_types():
    schema = schema_for(["sequence_number", "body"])
    assert schema["sequence_number"].data_types["base"].dtype == "INTEGER"
    assert schema["body"].data_types["base"].dtype == "STRING"
```

- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** `src/output.py` per the behaviour above. `schema_for` uses `ColumnDefinition(data_types=getattr(BaseType, t.lower())())` (`BaseType.string()` / `.integer()` / `.float()` / `.boolean()` / `.timestamp()`; keboola-component 1.11 returns `{"base": DataType(dtype=…)}`). CSV via `csv.DictWriter(f, fieldnames=columns, extrasaction="raise", restval="")`. The flatten staging dir is created on `open()`.
- [ ] **Step 4:** PASS; ruff clean.
- [ ] **Step 5:** Commit: `feat: output table with write_always switch, flatten promotion rule and body_unmapped`.

---

### Task 10: Run statistics and summary

**Owner skill:** `component-develop`.

**Files:**
- Create: `src/stats.py`
- Test: `tests/unit/test_stats.py`

**Interfaces:**
- Consumes: `Configuration` (Task 2), `EntityRef` (Task 5).
- Produces: `@dataclass class RunStats` with fields `mode: str`, `received`, `written`, `completed`, `deferred`, `deleted_on_receive`, `committed`, `already_gone`, `carried_forward`, `dropped_stale`, `orphans_recovered`, `settlement_failures`, `recoveries`, `unreadable_recycles`, `expired_skipped`, `max_delivery_count` (all `int = 0`), `orphans_guarded: list[int]`, `unreadable: Counter[str]` (key `"<reason>:<disposition>"`), `stop_reason: str = ""`, `warnings: dict[str, str]`; methods `warn(key: str, message: str) -> None` (logs `WARNING` once per key and stores it), `note_delivery_count(n: int) -> None`, `unreadable_total() -> int` (dispositions excluding `"retry"`), `summary_line() -> str`, `log_summary() -> None` (one INFO line + repeats stored warnings count). Function `log_effective_settings(config: Configuration, entity: EntityRef) -> None` (one INFO line; each value followed by ` (default)` when equal to the model default).

- [ ] **Step 1: Write the failing tests** — `tests/unit/test_stats.py`:

```python
import logging

from configuration import Configuration
from entity import EntityRef
from stats import RunStats, log_effective_settings

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"


def test_warn_once_per_key(caplog):
    s = RunStats(mode="complete")
    with caplog.at_level(logging.WARNING):
        s.warn("x", "first")
        s.warn("x", "second")
    assert [r.getMessage() for r in caplog.records] == ["first"]


def test_delivery_count_high_water_and_unreadable_total():
    s = RunStats(mode="complete")
    for n in (1, 5, 3):
        s.note_delivery_count(n)
    s.unreadable["UnreadableBody:retry"] += 1
    s.unreadable["UnreadableBody:dead_lettered"] += 2
    assert s.max_delivery_count == 5 and s.unreadable_total() == 2


def test_summary_line_mentions_counts():
    s = RunStats(mode="defer_commit", received=3, deferred=3, committed=2, stop_reason="idle")
    line = s.summary_line()
    assert "received=3" in line and "deferred=3" in line and "committed=2" in line and "stop=idle" in line


def test_effective_settings_marks_defaults(caplog):
    c = Configuration(**{"#connection_string": SAS, "source": {"entity_type": "queue", "queue_name": "q"}})
    with caplog.at_level(logging.INFO):
        log_effective_settings(c, EntityRef.from_source(c.source))
    text = caplog.records[-1].getMessage()
    assert "settlement_mode=complete (default)" in text and "entity=q" in text and "SharedAccessKey" not in text
```

- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** `src/stats.py` per the interface (no secrets are ever part of the settings line — it lists only source / limits / body / destination / advanced values).
- [ ] **Step 4:** PASS; ruff clean.
- [ ] **Step 5:** Commit: `feat: run statistics, warning aggregation and effective-settings log line`.

---

### Task 11: Settlement, unreadable handling, batch processor

**Owner skill:** `component-develop`.

**Files:**
- Create: `src/settlement.py`, `tests/fakes/recording.py`
- Test: `tests/unit/test_settlement.py`

**Interfaces:**
- Consumes: Tasks 2, 5–10.
- Produces:
  - `LOCK_RENEW_MARGIN = timedelta(seconds=10)`, `UNREADABLE_ABORT_SHARE = 0.10`, `UNREADABLE_ABORT_MIN = 10`, `MAX_UNREADABLE_RECYCLES = 50`.
  - `class Settler(Protocol)`: `settle(self, receiver, message, body_bytes: int) -> None`. Implementations: `CompleteSettler(stats, arm: Callable[[], None])` — calls `arm()` once, **immediately before its first `complete_message`** (the `write_always` switch, spec §6.9); `DeferSettler(pending: PendingSetBuilder, entity: EntityRef, stats)` (never arms); `NoopSettler(stats, count_as_deleted: bool)` (never arms — C3 arms in the receive loop before its first receive, Task 14); factory `make_settler(mode: SettlementMode, *, pending: PendingSetBuilder | None, entity: EntityRef, stats: RunStats, arm: Callable[[], None] = lambda: None) -> Settler`.
  - `safe_settle(action: Callable[[], None], stats: RunStats) -> bool` — runs `action`; `MessageLockLostError` / `MessageAlreadySettled` → `stats.settlement_failures += 1`, returns `False`; other exceptions propagate (a connection error must reach the recycle logic).
  - `renew_if_needed(receiver, message, now: datetime) -> None` — PEEK_LOCK messages whose `locked_until_utc - now < LOCK_RENEW_MARGIN` get `receiver.renew_message_lock(message)`.
  - `class UnreadableAction(StrEnum)`: `RETRY`, `DISPOSED`, `SKIPPED`.
  - `class UnreadableHandler`: `__init__(self, *, policy: UnreadablePolicy, mode: SettlementMode, is_sub_queue: bool, stats: RunStats)`; `handle(self, receiver, message, error: Exception, generation: int) -> UnreadableAction`; `check_abort_share(self) -> None`.
  - `@dataclass class BatchResult`: `written: int`, `needs_recycle: bool`, `progressed: bool` (`written > 0` or at least one unreadable disposition in this batch — the receive loop's no-progress guard counts both, spec §6.5).
  - `class BatchProcessor`: `__init__(self, *, entity: EntityRef, mode: SettlementMode, body_format: BodyFormat, sink: RowSink, registry: FlattenRegistry | None, settler: Settler, unreadable: UnreadableHandler, stats: RunStats, clock: Callable[[], datetime])`; `process(self, receiver, messages: Sequence, generation: int) -> BatchResult`.
  - `tests/fakes/recording.py`: `class RecordingSink` whose `write_rows(rows: Sequence[OutputRow])` appends one dict per row to `rows: list[dict]` — `{**row.metadata, "body": row.body, "fields": row.fields}` — so tests read `r["body"]`, `r["state"]`, `r["fields"]`.

`handle()` rules (spec §6.6): `BodyDecodeError` → first failure for that sequence number **while `stats.unreadable_recycles < MAX_UNREADABLE_RECYCLES`**: remember `(seq → generation)`; C1/C2 `abandon_message` via `safe_settle`; C1/C2/C4 return `RETRY` (count `"<reason>:retry"`); C3 falls through to the final disposition. With the retry budget spent, a first failure goes straight to the final disposition (WARNING key `unreadable_retry_budget`). A repeat failure with a **different** generation, or any `NotJsonError` / `BodyTooLargeError` (deterministic), → final disposition: `FAIL` → `UserException(f"Message {seq} (message id {mid}) has an unreadable body ({reason}) and the unreadable_body policy is 'fail'.")`; C1/C2 with `DEAD_LETTER` on a main entity → `dead_letter_message(message, reason=<reason>, error_description=<exception class name>)` → `"dead_lettered"`; C1/C2 with `LEAVE`, or `DEAD_LETTER` on a sub-queue (WARNING key `dead_letter_on_sub_queue`) → nothing settled → `"left"`; C3 / C4 → WARNING (sequence number + message id, never the body) → `"skipped"`. A sequence number already disposed as `"left"` in this run → `DISPOSED` again without a retry and without counting twice. Reasons: `UnreadableBody`, `NotJson`, `BodyTooLarge`. `check_abort_share`: `stats.unreadable_total() > UNREADABLE_ABORT_SHARE * stats.received and stats.unreadable_total() >= UNREADABLE_ABORT_MIN` → `UserException`.

`process()` order: for each message → `stats.received += 1`, `note_delivery_count`, `encode_body` (errors → `unreadable.handle`, a `RETRY` sets `needs_recycle`, a disposition sets `progressed`), build `OutputRow(metadata=message_metadata(...), body=enc.cell)` or, for flatten, `OutputRow(metadata=..., fields=enc.fields)` after `registry.register(path)` for every path (name + cap; `FlattenColumnCapError` → abandon every lockable message of the batch via `safe_settle`, then `UserException("The message bodies produced more than 1000 distinct JSON keys; use the text body format.")`); `sink.write_rows(good_rows)` (one call per batch, **before** any settle); then per good message `renew_if_needed` + `safe_settle(lambda: settler.settle(...))`; `stats.written += len(good_rows)`; finally `unreadable.check_abort_share()`.

- [ ] **Step 1: Write the failing tests** — `tests/unit/test_settlement.py`:

```python
from datetime import timedelta

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
        entity=entity, mode=mode, body_format=fmt, sink=sink, registry=None,
        settler=make_settler(mode, pending=pending, entity=entity, stats=stats),
        unreadable=UnreadableHandler(policy=policy, mode=mode, is_sub_queue=entity.is_sub_queue, stats=stats),
        stats=stats, clock=broker.clock.now,
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
        entity=Q, mode=SettlementMode.COMPLETE, body_format=BodyFormat.JSON_FLATTEN, sink=RecordingSink(),
        registry=FlattenRegistry([], reserved=set(metadata_column_names())),
        settler=make_settler(SettlementMode.COMPLETE, pending=None, entity=Q, stats=stats),
        unreadable=UnreadableHandler(policy=UnreadablePolicy.DEAD_LETTER, mode=SettlementMode.COMPLETE, is_sub_queue=False, stats=stats),
        stats=stats, clock=broker.clock.now,
    )
    with receiver(broker) as r:
        result = proc.process(r, r.receive_messages(), generation=0)
    assert not result.needs_recycle and q.dead_letter.state_of(seq) == "ACTIVE"
    assert stats.unreadable["NotJson:dead_lettered"] == 1


def test_fail_policy_raises(broker):
    q = broker.add_queue("q")
    q.send(b"a")
    broker.inject_body_error(1, times=5)
    proc, _, _ = processor(broker, SettlementMode.RECEIVE_AND_DELETE, policy=UnreadablePolicy.FAIL)
    with receiver(broker, mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE) as r:
        with pytest.raises(UserException, match="unreadable"):
            proc.process(r, r.receive_messages(), generation=0)


def test_dead_letter_degrades_to_leave_on_sub_queue(broker):
    stats = RunStats(mode="complete")
    handler = UnreadableHandler(policy=UnreadablePolicy.DEAD_LETTER, mode=SettlementMode.COMPLETE, is_sub_queue=True, stats=stats)
    from body import NotJsonError
    from tests.fakes.broker import make_message

    action = handler.handle(receiver=None, message=make_message(b"x"), error=NotJsonError("x"), generation=0)
    assert action is UnreadableAction.DISPOSED and stats.unreadable["NotJson:left"] == 1
    assert "dead_letter_on_sub_queue" in stats.warnings


def test_abort_share():
    stats = RunStats(mode="complete", received=50)
    stats.unreadable["UnreadableBody:dead_lettered"] = 10
    handler = UnreadableHandler(policy=UnreadablePolicy.DEAD_LETTER, mode=SettlementMode.COMPLETE, is_sub_queue=False, stats=stats)
    with pytest.raises(UserException):
        handler.check_abort_share()


def test_complete_settler_arms_before_first_complete(broker):
    q = broker.add_queue("q")
    seqs = [q.send(b"a"), q.send(b"b")]
    events: list[str] = []
    stats = RunStats(mode="complete")
    settler = make_settler(SettlementMode.COMPLETE, pending=None, entity=Q, stats=stats,
                           arm=lambda: events.append(f"arm:{q.state_of(seqs[0])}"))
    proc = BatchProcessor(
        entity=Q, mode=SettlementMode.COMPLETE, body_format=BodyFormat.TEXT, sink=RecordingSink(), registry=None,
        settler=settler,
        unreadable=UnreadableHandler(policy=UnreadablePolicy.DEAD_LETTER, mode=SettlementMode.COMPLETE, is_sub_queue=False, stats=stats),
        stats=stats, clock=broker.clock.now,
    )
    with receiver(broker) as r:
        proc.process(r, r.receive_messages(max_message_count=10), generation=0)
    assert events == ["arm:ACTIVE"]  # armed once, before anything was deleted


@pytest.mark.parametrize("mode", [SettlementMode.DEFER_COMMIT, SettlementMode.RECEIVE_AND_DELETE, SettlementMode.PEEK])
def test_other_settlers_never_arm(mode):
    calls: list[int] = []
    make_settler(mode, pending=PendingSetBuilder(), entity=Q, stats=RunStats(mode=mode.value), arm=lambda: calls.append(1))
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
        entity=Q, mode=SettlementMode.COMPLETE, body_format=BodyFormat.JSON_FLATTEN, sink=RecordingSink(),
        registry=FlattenRegistry([], reserved=set(metadata_column_names())),
        settler=make_settler(SettlementMode.COMPLETE, pending=None, entity=Q, stats=stats),
        unreadable=UnreadableHandler(policy=UnreadablePolicy.DEAD_LETTER, mode=SettlementMode.COMPLETE, is_sub_queue=False, stats=stats),
        stats=stats, clock=broker.clock.now,
    )
    with receiver(broker) as r:
        result = proc.process(r, r.receive_messages(), generation=0)
    assert result.written == 0 and result.progressed and q.state_of(seq) is None
```

- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** `src/settlement.py` per the interface and rules above; `tests/fakes/recording.py` as specified. `BatchProcessor` asserts `registry is not None` when `body_format is JSON_FLATTEN` (constructor `ValueError`).
- [ ] **Step 4:** PASS; ruff clean.
- [ ] **Step 5:** Commit: `feat: write-then-settle batch processor with per-mode settlers and unreadable-body policy`.

---

### Task 12: H5 — pending-set commit

**Owner skill:** `component-develop`.

**Files:**
- Create: `src/commit.py`
- Test: `tests/unit/test_commit.py`

**Interfaces:**
- Consumes: `ServiceBusConnector`, `to_user_exception` (Task 3); `EntityRef` (Task 5); `PendingEntity`, `PendingGroup`, `PendingSetBuilder` (Task 6); `RunStats` (Task 10).
- Produces:
  - `COMMIT_MAX_COUNT = 250`, `COMMIT_MAX_BYTES = 16 * 1024 * 1024`, `TRANSIENT_BACKOFF_SECONDS = (2, 4, 8)`, `TRANSIENT_ERRORS = (ServiceBusServerBusyError, ServiceBusConnectionError, ServiceBusCommunicationError, OperationTimeoutError)`.
  - `commit_chunk_size(max_body_bytes: int) -> int` — `max(1, min(COMMIT_MAX_COUNT, COMMIT_MAX_BYTES // max(1, max_body_bytes)))`.
  - `with_transient_retry(fn: Callable[[], T], *, sleep: Callable[[float], None], extra: tuple[type[Exception], ...] = ()) -> T` — calls `fn`; on `TRANSIENT_ERRORS + extra` sleeps 2 / 4 / 8 s between retries (4 calls max), then re-raises.
  - `receive_deferred_bisect(receiver, seqs: list[int], *, sleep) -> tuple[list, list[int]]` — `(received_messages, not_found_seqs)`; calls `receiver.receive_deferred_messages(seqs, timeout=60)` through `with_transient_retry`; on `MessageNotFoundError` splits in halves recursively; a single failing seq goes to `not_found`.
  - `class PendingCommitter`: `__init__(self, connector: ServiceBusConnector, *, configured: EntityRef, stats: RunStats, sleep: Callable[[float], None] | None = None)`; `commit(self, pending: list[PendingEntity]) -> list[PendingEntity]` (returns the groups carried forward).

Commit rules (spec §6.3): one `commit_client()` for the whole commit; per group a RECEIVE_AND_DELETE receiver (`session_id=group.session_id`, `prefetch_count=1`, `keep_alive=0`, `client_identifier=connector.client_identifier`) — opening + the first call wrapped with `extra=(SessionCannotBeLockedError,)`; chunks of `commit_chunk_size(group.max_body_bytes)` in sequence order; `stats.committed += len(received)`, `stats.already_gone += len(not_found)`. Exhausted transient retries → `UserException(f"Could not delete the {n} message(s) deferred by the previous run on '{path}': {redacted}. Nothing was extracted in this run and the state is unchanged; the next run retries.")`. Exhausted `SessionCannotBeLockedError` → the group is carried forward (`stats.carried_forward += count`, WARNING key `commit_session_locked`). `MessagingEntityNotFoundError` / `ServiceBusAuthenticationError` / `ServiceBusAuthorizationError`: if `entity_ref != configured` → WARNING key `commit_stale_entity` (`f"{n} message(s) deferred by an earlier run stay DEFERRED on '{path}' because that entity is gone or no longer readable with these credentials; run a defer-commit row on that entity to recover them."`), `stats.dropped_stale += count`, group dropped; else `raise to_user_exception(error, path)`.

- [ ] **Step 1: Write the failing tests** — `tests/unit/test_commit.py`:

```python
import pytest
from azure.servicebus import NEXT_AVAILABLE_SESSION
from azure.servicebus.exceptions import ServiceBusServerBusyError
from keboola.component.exceptions import UserException

from client import ServiceBusConnector
from commit import PendingCommitter, commit_chunk_size, receive_deferred_bisect
from configuration import AuthConfiguration, EntityType, SubQueue
from entity import EntityRef
from state import PendingSetBuilder
from stats import RunStats

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"
Q = EntityRef(EntityType.QUEUE, "q", None, None, SubQueue.NONE)


def connector() -> ServiceBusConnector:
    return ServiceBusConnector(AuthConfiguration(**{"#connection_string": SAS}), "kbc-test")


def deferred(entity_fake, n, **send):
    seqs = [entity_fake.send(b"x", **send) for _ in range(n)]
    for s in seqs:
        entity_fake.defer_existing(s)
    return seqs


def pending_for(ref, seqs, session_id=None, body_bytes=10):
    b = PendingSetBuilder()
    for s in seqs:
        b.add(ref, s, session_id, body_bytes)
    return b.build("2026-09-23 10:00:00.000000")


def committer(sleeps=None, configured=Q, stats=None):
    stats = stats or RunStats(mode="complete")
    return PendingCommitter(connector(), configured=configured, stats=stats, sleep=(sleeps.append if sleeps is not None else lambda s: None)), stats


def test_chunk_size():
    assert commit_chunk_size(10) == 250
    assert commit_chunk_size(8 * 1024 * 1024) == 2
    assert commit_chunk_size(100 * 1024 * 1024) == 1


def test_commit_plain_in_chunks_of_250(broker):
    q = broker.add_queue("q")
    seqs = deferred(q, 600)
    c, stats = committer()
    assert c.commit(pending_for(Q, seqs)) == []
    assert q.sequence_numbers() == [] and stats.committed == 600
    calls = [op for op, _ in broker.calls if op == "receive_deferred_messages"]
    assert len(calls) == 3


def test_commit_uses_retry_total_zero_client(broker):
    q = broker.add_queue("q")
    c, _ = committer()
    c.commit(pending_for(Q, deferred(q, 1)))
    assert any(client.kwargs.get("retry_total") == 0 for client in broker.clients)


def test_byte_cap_chunks(broker):
    q = broker.add_queue("q")
    seqs = deferred(q, 5)
    c, _ = committer()
    c.commit(pending_for(Q, seqs, body_bytes=8 * 1024 * 1024))
    assert len([op for op, _ in broker.calls if op == "receive_deferred_messages"]) == 3


def test_partitions_committed_separately(broker):
    p = broker.add_queue("p", partitioned=True)
    ref = EntityRef(EntityType.QUEUE, "p", None, None, SubQueue.NONE)
    seqs = [p.send(b"x", partition=i % 3) for i in range(9)]
    for s in seqs:
        p.defer_existing(s)
    c, stats = committer(configured=ref)
    c.commit(pending_for(ref, seqs))
    assert p.sequence_numbers() == [] and stats.committed == 9


def test_not_found_is_already_gone(broker):
    q = broker.add_queue("q")
    seqs = deferred(q, 4)
    c, stats = committer()
    c.commit(pending_for(Q, seqs + [9999]))
    assert stats.committed == 4 and stats.already_gone == 1


def test_bisect_directly(broker):
    q = broker.add_queue("q")
    seqs = deferred(q, 3)
    from azure.servicebus import ServiceBusReceiveMode

    client = connector().commit_client()
    with client.get_queue_receiver("q", receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE, prefetch_count=1, keep_alive=0) as r:
        received, gone = receive_deferred_bisect(r, [seqs[0], 777, seqs[1], seqs[2]], sleep=lambda s: None)
    assert sorted(m.sequence_number for m in received) == seqs and gone == [777]


def test_transient_retry_then_success(broker):
    q = broker.add_queue("q")
    seqs = deferred(q, 2)
    broker.inject_commit_errors([ServiceBusServerBusyError(message="busy")])
    sleeps: list[float] = []
    c, stats = committer(sleeps)
    c.commit(pending_for(Q, seqs))
    assert sleeps == [2] and stats.committed == 2


def test_transient_exhausted_fails_run_state_intact(broker):
    q = broker.add_queue("q")
    seqs = deferred(q, 2)
    broker.inject_commit_errors([ServiceBusServerBusyError(message="busy")] * 4)
    sleeps: list[float] = []
    c, _ = committer(sleeps)
    with pytest.raises(UserException, match="next run retries"):
        c.commit(pending_for(Q, seqs))
    assert sleeps == [2, 4, 8] and all(q.state_of(s) == "DEFERRED" for s in seqs)


def test_session_group_committed(broker):
    s = broker.add_queue("s", sessions=True)
    ref = EntityRef(EntityType.QUEUE, "s", None, None, SubQueue.NONE)
    seqs = deferred(s, 2, session_id="A")
    c, stats = committer(configured=ref)
    c.commit(pending_for(ref, seqs, session_id="A"))
    assert s.sequence_numbers() == [] and stats.committed == 2


def test_session_locked_is_carried_forward(broker):
    s = broker.add_queue("s", sessions=True)
    ref = EntityRef(EntityType.QUEUE, "s", None, None, SubQueue.NONE)
    seqs = deferred(s, 1, session_id="A")
    s.send(b"active", session_id="A")
    holder = connector().receive_client().get_queue_receiver("s", session_id=NEXT_AVAILABLE_SESSION, prefetch_count=1, keep_alive=0)
    holder.receive_messages()  # holds session A
    c, stats = committer(configured=ref)
    carried = c.commit(pending_for(ref, seqs, session_id="A"))
    assert carried and carried[0].groups[0].session_id == "A" and stats.carried_forward == 1
    assert "commit_session_locked" in stats.warnings


def test_stale_entity_dropped_with_warning(broker):
    broker.add_queue("q")
    old = EntityRef(EntityType.QUEUE, "gone", None, None, SubQueue.NONE)
    c, stats = committer(configured=Q)
    assert c.commit(pending_for(old, [1, 2])) == []
    assert stats.dropped_stale == 2 and "commit_stale_entity" in stats.warnings


def test_configured_entity_missing_is_user_exception(broker):
    c, _ = committer(configured=Q)
    with pytest.raises(UserException):
        c.commit(pending_for(Q, [1]))
```

- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** the H5 half of `src/commit.py` per the interface and rules.
- [ ] **Step 4:** PASS; ruff clean.
- [ ] **Step 5:** Commit: `feat: H5 pending-set commit (partition/session grouping, count+byte chunking, bisection, transient retry)`.

---

### Task 13: H3 — orphan scan (C2) and foreign-deferral probe (C1/C3)

**Owner skill:** `component-develop`.

**Files:**
- Modify: `src/commit.py`
- Test: `tests/unit/test_orphans.py`

**Interfaces:**
- Consumes: Task 12 helpers (`receive_deferred_bisect`, `with_transient_retry`); `EntityInfo`, `partition_of` (Task 5); `BatchProcessor` (Task 11).
- Produces:
  - `ORPHAN_PAGE_SIZE = 250`, `ORPHAN_PAGE_CAP = 20`.
  - `k_stop(batch_size: int, prefetch_count: int) -> int` — `max(100, 2 * (batch_size + prefetch_count + 1))`.
  - `is_qualifying(message, now: datetime) -> bool` — `state == ACTIVE`, `delivery_count == 0`, `expires_at_utc` is `None` or `> now`, `scheduled_enqueue_time_utc` is `None`.
  - `class OrphanScanner`: `__init__(self, connector, *, entity: EntityRef, info: EntityInfo, session_enabled: bool, batch_size: int, prefetch_count: int, processor: BatchProcessor, stats: RunStats, clock: Callable[[], datetime], sleep: Callable[[float], None] | None = None)`; `scan(self) -> None`.
  - `class ForeignDeferralProbe`: `__init__(self, connector, *, entity: EntityRef, info: EntityInfo, session_enabled: bool, stats: RunStats)`; `probe(self) -> None`.

Scan rules (spec §6.4): sessions → WARNING key `orphan_sessions`, no peek. Best-effort mode when `info.is_partitioned` or `entity.is_sub_queue` → WARNING key `orphan_best_effort` every run, full scan, no K rule (partitioned: cursor-mode peek `sequence_number=0`; sub-queue: explicit paging). Plain: explicit paging from 1, K rule. Peek receiver = `entity.open_receiver(connector.receive_client(), receive_mode=PEEK_LOCK, prefetch_count=1, keep_alive=0, client_identifier=…)` (a peek locks nothing). Per page: `info.note_sequence_number(seq)` for every message (a heuristic flip to partitioned switches to best-effort for the rest of the scan); `DEFERRED` → guard check (`delivery_count >= info.orphan_guard_threshold` → `stats.orphans_guarded.append(seq)`, WARNING key `orphan_guard` listing the first 20) else collect as orphan, reset the consecutive count; plain + `is_qualifying` → consecutive += 1, stop at `k_stop(...)`; other messages are skipped without counting. After each page, recover the page's orphans: on `connector.commit_client()` a PEEK_LOCK receiver for the entity, group by `partition_of`, chunks of `min(COMMIT_MAX_COUNT, batch_size)`, `receive_deferred_bisect`, then `processor.process(receiver, received, generation=0)` (rows written, then `DeferSettler` re-defers and records them); `stats.orphans_recovered += len(received)`. After `ORPHAN_PAGE_CAP` pages without a stop → WARNING key `orphan_scan_incomplete`. Probe: sessions → return; one page (cursor mode if partitioned, else from 1); any `DEFERRED` → WARNING key `foreign_deferrals` ("N deferred message(s) on '<path>' are not owned by this configuration's state; if they came from a failed defer-commit run of this configuration, run it once in defer-commit mode to recover them."). The probe never receives or settles.

- [ ] **Step 1: Write the failing tests** — `tests/unit/test_orphans.py`:

```python
from datetime import UTC, datetime, timedelta

from azure.servicebus import ServiceBusMessageState

from client import ServiceBusConnector
from commit import ForeignDeferralProbe, OrphanScanner, is_qualifying, k_stop
from configuration import AuthConfiguration, BodyFormat, EntityType, SettlementMode, SubQueue, UnreadablePolicy
from entity import EntityInfo, EntityRef
from settlement import BatchProcessor, UnreadableHandler, make_settler
from state import PendingSetBuilder
from stats import RunStats
from tests.fakes.broker import make_message
from tests.fakes.recording import RecordingSink

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"
Q = EntityRef(EntityType.QUEUE, "q", None, None, SubQueue.NONE)


def connector() -> ServiceBusConnector:
    return ServiceBusConnector(AuthConfiguration(**{"#connection_string": SAS}), "kbc-test")


def scanner(broker, *, entity=Q, info=None, session_enabled=False, batch_size=100):
    stats = RunStats(mode="defer_commit")
    pending = PendingSetBuilder()
    sink = RecordingSink()
    mode = SettlementMode.DEFER_COMMIT
    proc = BatchProcessor(
        entity=entity, mode=mode, body_format=BodyFormat.TEXT, sink=sink, registry=None,
        settler=make_settler(mode, pending=pending, entity=entity, stats=stats),
        unreadable=UnreadableHandler(policy=UnreadablePolicy.DEAD_LETTER, mode=mode, is_sub_queue=entity.is_sub_queue, stats=stats),
        stats=stats, clock=broker.clock.now,
    )
    scan = OrphanScanner(
        connector(), entity=entity, info=info or EntityInfo(), session_enabled=session_enabled,
        batch_size=batch_size, prefetch_count=1, processor=proc, stats=stats, clock=broker.clock.now,
        sleep=lambda s: None,
    )
    return scan, pending, sink, stats


def peeks(broker) -> int:
    return len([op for op, _ in broker.calls if op == "peek_messages"])


def test_k_stop():
    assert k_stop(100, 1) == 204
    assert k_stop(10, 1) == 100


def test_is_qualifying():
    now = datetime(2026, 9, 23, 10, 0, tzinfo=UTC)
    active = ServiceBusMessageState.ACTIVE
    assert is_qualifying(make_message(b"x", state=active, delivery_count=0), now)
    assert not is_qualifying(make_message(b"x", state=active, delivery_count=1), now)
    assert not is_qualifying(make_message(b"x", state=active, expires_at_utc=now - timedelta(seconds=1)), now)
    assert not is_qualifying(make_message(b"x", state=active, scheduled_enqueue_time_utc=now), now)
    assert not is_qualifying(make_message(b"x", state=ServiceBusMessageState.SCHEDULED), now)


def test_plain_orphan_recovered_and_redeferred(broker):
    q = broker.add_queue("q")
    orphan = q.send(b"o")
    q.defer_existing(orphan)
    q.send(b"active")
    scan, pending, sink, stats = scanner(broker)
    scan.scan()
    assert [r["body"] for r in sink.rows] == ["o"] and q.state_of(orphan) == "DEFERRED"
    assert pending.count == 1 and stats.orphans_recovered == 1


def test_scan_stops_after_k(broker):
    q = broker.add_queue("q")
    for _ in range(150):
        q.send(b"a")
    late = q.send(b"late")
    q.defer_existing(late)
    scan, pending, _, _ = scanner(broker, batch_size=10)  # K = 100
    scan.scan()
    assert pending.count == 0 and peeks(broker) == 1


def test_locked_cluster_does_not_stop_scan_early(broker):
    q = broker.add_queue("q")
    for _ in range(60):
        q.lock_existing(q.send(b"l"), 60)
    orphan = q.send(b"o")
    q.defer_existing(orphan)
    scan, pending, _, _ = scanner(broker, batch_size=10)  # K = 100 > 60 locked
    scan.scan()
    assert pending.count == 1


def test_page_cap_warning(broker):
    q = broker.add_queue("q")
    for _ in range(20 * 250 + 1):
        q.send(b"r", delivery_count=1)  # redelivered stragglers never count toward K
    scan, _, _, stats = scanner(broker)
    scan.scan()
    assert "orphan_scan_incomplete" in stats.warnings and peeks(broker) == 20


def test_orphan_guard(broker):
    q = broker.add_queue("q", max_delivery_count=10)
    o = q.send(b"o", delivery_count=9)
    q.defer_existing(o)
    scan, pending, _, stats = scanner(broker, info=EntityInfo(max_delivery_count=10))
    scan.scan()
    assert pending.count == 0 and stats.orphans_guarded == [o] and "orphan_guard" in stats.warnings


def test_partitioned_best_effort(broker):
    p = broker.add_queue("p", partitioned=True)
    ref = EntityRef(EntityType.QUEUE, "p", None, None, SubQueue.NONE)
    for i in range(3):
        p.defer_existing(p.send(b"o", partition=i))
    scan, pending, _, stats = scanner(broker, entity=ref, info=EntityInfo(partitioned=True))
    scan.scan()
    assert pending.count == 3 and "orphan_best_effort" in stats.warnings


def test_sub_queue_best_effort(broker):
    q = broker.add_queue("q")
    seq = q.send(b"x")
    q.dead_letter_existing(seq, "r", "d")
    q.dead_letter.defer_existing(seq)
    ref = EntityRef(EntityType.QUEUE, "q", None, None, SubQueue.DEAD_LETTER)
    scan, pending, _, stats = scanner(broker, entity=ref)
    scan.scan()
    assert pending.count == 1 and "orphan_best_effort" in stats.warnings


def test_sessions_warn_without_peeking(broker):
    broker.add_queue("s", sessions=True)
    ref = EntityRef(EntityType.QUEUE, "s", None, None, SubQueue.NONE)
    scan, _, _, stats = scanner(broker, entity=ref, session_enabled=True)
    scan.scan()
    assert "orphan_sessions" in stats.warnings and peeks(broker) == 0


def test_foreign_deferral_probe_warns_and_touches_nothing(broker):
    q = broker.add_queue("q")
    f = q.send(b"f")
    q.defer_existing(f)
    stats = RunStats(mode="complete")
    ForeignDeferralProbe(connector(), entity=Q, info=EntityInfo(), session_enabled=False, stats=stats).probe()
    assert "foreign_deferrals" in stats.warnings and q.state_of(f) == "DEFERRED"


def test_probe_skips_sessions(broker):
    broker.add_queue("s", sessions=True)
    ref = EntityRef(EntityType.QUEUE, "s", None, None, SubQueue.NONE)
    stats = RunStats(mode="complete")
    ForeignDeferralProbe(connector(), entity=ref, info=EntityInfo(), session_enabled=True, stats=stats).probe()
    assert stats.warnings == {} and peeks(broker) == 0
```

- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** per the rules. Add a comment at the K computation stating the derivation (spec §6.4 step 4: interruption cluster ≤ `batch + prefetch + 1`, no-progress guard, factor 2) and label the scheduled-property skip `[inferred]` in a comment.
- [ ] **Step 4:** PASS; ruff clean.
- [ ] **Step 5:** Commit: `feat: C2 orphan scan (bounded K rule, best-effort variants, delivery-count guard) and C1/C3 foreign-deferral probe`.

---

### Task 14: Receive loop (C1/C2/C3)

**Owner skill:** `component-develop`.

**Files:**
- Create: `src/receiver.py`
- Test: `tests/unit/test_receive_loop.py`

**Interfaces:**
- Consumes: `ServiceBusConnector`, `to_user_exception` (Task 3); `EntityRef`, `EntityInfo` (Task 5); `BatchProcessor`, `safe_settle` (Task 11); `RunStats` (Task 10); `Configuration` (Task 2); `STATE_BUDGET_BYTES` (Task 6).
- Produces:
  - `MAX_RECOVERIES = 5`.
  - `class StopReason(StrEnum)`: `IDLE="idle"`, `MAX_MESSAGES="max_messages"`, `MAX_DURATION="max_duration"`, `WATERMARK="watermark"`, `STATE_BUDGET="state_budget"`, `NO_MORE_SESSIONS="no_more_sessions"`, `SESSION_REVISITED="session_revisited"`, `END_OF_ENTITY="end_of_entity"`.
  - `SESSION_ACCEPT_WAIT_SECONDS = 5` (session receivers outside this loop: peek, `testConnection`, `previewMessages`).
  - `FATAL_ERRORS = (ServiceBusAuthenticationError, ServiceBusAuthorizationError, MessagingEntityNotFoundError, MessagingEntityDisabledError)` plus any `ServiceBusError` whose text mentions sessions (mapped via `to_user_exception`, never recycled). `ValueError` is **not** in it (Global Constraints).
  - `class RecoveryTracker`: `__init__(self, max_recoveries: int = MAX_RECOVERIES)`; `generation: int` (starts 0; +1 on **every** reopen — connection or unreadable retry); `count: int` (connection recoveries only); `progress(self) -> None`; `unreadable_retry(self) -> None` (`generation += 1` only — never touches `count` or the progress flag); `failure(self, error: Exception) -> None` — if the error is fatal → `raise to_user_exception(error, entity_path) from error`; if no progress since the previous connection failure, or `count == max_recoveries` → re-raise (a `ServiceBusError` as `UserException(f"Lost the connection to Service Bus {count + 1} times in this run (limit {MAX_RECOVERIES}, or twice without writing a row or disposing an unreadable message in between); last error: {redacted}. Messages that were not settled redeliver on the next run.")`, anything else unchanged → exit 2); else `count += 1`, `generation += 1`.
  - `class ReceiveLoop`: `__init__(self, *, connector, entity: EntityRef, info: EntityInfo, config: Configuration, processor: BatchProcessor, stats: RunStats, t0: datetime, state_size: Callable[[], int], arm_write_always: Callable[[], None], clock: Callable[[], datetime], monotonic: Callable[[], float] | None = None, sleep: Callable[[float], None] | None = None)`; `run(self) -> StopReason`. `state_size` returns the size of the **whole projected output state** (Task 16).

Loop rules (spec §6.5): receive mode PEEK_LOCK for C1/C2, RECEIVE_AND_DELETE for C3; receiver kwargs `prefetch_count=config.advanced.prefetch_count`, `keep_alive=0`, `client_identifier=connector.client_identifier`, session receivers `session_id=NEXT_AVAILABLE_SESSION`, `max_wait_time=idle_timeout_seconds`. Per batch: stop checks first (remaining messages when `max_messages > 0`; `monotonic() >= deadline`; C2 and `state_size() >= STATE_BUDGET_BYTES` → WARNING key `state_budget`); C3: `arm_write_always()` immediately before the first `receive_messages` call of the run (RECEIVE_AND_DELETE deletes on delivery); `receive_messages(max_message_count=min(batch_size, remaining), max_wait_time=idle)`; empty → `IDLE`; `processor.process(receiver, batch, tracker.generation)`; `tracker.progress()` when `result.progressed`; `result.needs_recycle` → close + reopen, `tracker.unreadable_retry()`, `stats.unreadable_recycles += 1` (**not** a connection failure); watermark (`stop_at_job_start` and every non-`SCHEDULED` message of the batch has `enqueued_time_utc >= t0`) → `WATERMARK` (WARNING key `watermark_approximate` if `info.is_partitioned`). Sessions: loop until `OperationTimeoutError` (`NO_MORE_SESSIONS`); a session id seen before → `SESSION_REVISITED`; `SessionCannotBeLockedError` → skip; renew the session lock when `receiver.session.locked_until_utc - clock() < 10 s`; per-session `IDLE` / `WATERMARK` continue with the next session; global stops end the run. Stop drain on every stop except `IDLE` / `NO_MORE_SESSIONS`: `receive_messages(max_message_count=prefetch_count + 1, max_wait_time=1)` → C1/C2 `safe_settle(abandon)`, C3 `processor.process(...)`. Any other exception → close receiver + client, `tracker.failure(e)`, `stats.recoveries = tracker.count`, reopen. Session lock: before every receive in a session, `receiver.session.renew_lock()` when `receiver.session.locked_until_utc - clock() < LOCK_RENEW_MARGIN`. Catch-up: `recovery_wait_seconds > 0` and `tracker.count > 0` → poll with `max_wait_time=1` until `min(recovery_wait, remaining duration)` elapses, `sleep(1)` after each empty poll, processing whatever arrives. `stats.stop_reason = reason.value`.

- [ ] **Step 1: Write the failing tests** — `tests/unit/test_receive_loop.py`:

```python
from datetime import timedelta

import pytest
from azure.servicebus.exceptions import ServiceBusError
from keboola.component.exceptions import UserException

from client import ServiceBusConnector
from configuration import AuthConfiguration, Configuration
from entity import EntityInfo, EntityRef
from receiver import ReceiveLoop, StopReason
from settlement import BatchProcessor, UnreadableHandler, make_settler
from state import PendingSetBuilder
from stats import RunStats
from tests.fakes.recording import RecordingSink

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"


def build(broker, *, source=None, limits=None, advanced=None, t0=None, state_size=lambda: 0, info=None):
    params = {"#connection_string": SAS, "source": source or {"entity_type": "queue", "queue_name": "q"}}
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
        entity=entity, mode=mode, body_format=config.body.body_format, sink=sink, registry=None,
        settler=make_settler(mode, pending=pending, entity=entity, stats=stats),
        unreadable=UnreadableHandler(policy=config.body.unreadable_body, mode=mode, is_sub_queue=entity.is_sub_queue, stats=stats),
        stats=stats, clock=broker.clock.now,
    )
    loop = ReceiveLoop(
        connector=ServiceBusConnector(AuthConfiguration(**{"#connection_string": SAS}), "kbc-1-2"),
        entity=entity, info=info or EntityInfo(), config=config, processor=proc, stats=stats,
        t0=t0 or broker.clock.now() + timedelta(hours=1), state_size=state_size, arm_write_always=lambda: None,
        clock=broker.clock.now, monotonic=lambda: broker.clock.now().timestamp(), sleep=broker.clock.advance,
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
    loop, sink, stats, _ = build(
        broker, source={"entity_type": "queue", "queue_name": "q", "settlement_mode": "receive_and_delete"},
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
        broker, source={"entity_type": "queue", "queue_name": "q", "settlement_mode": "defer_commit"},
        state_size=lambda: 10**9,
    )
    assert loop.run() is StopReason.STATE_BUDGET and "state_budget" in stats.warnings


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
    with pytest.raises(UserException, match="repeatedly"):
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


def test_c3_arms_before_first_receive(broker):
    q = broker.add_queue("q")
    seq = q.send(b"a")
    events: list[str] = []
    loop, _, _, _ = build(broker, source={"entity_type": "queue", "queue_name": "q", "settlement_mode": "receive_and_delete"})
    loop.arm_write_always = lambda: events.append(f"arm:{q.state_of(seq)}")
    loop.run()
    assert events[0] == "arm:ACTIVE" and q.state_of(seq) is None


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
    loop, sink, _, _ = build(broker, source={"entity_type": "queue", "queue_name": "s", "session_enabled": True},
                             advanced={"batch_size": 1})
    original = loop.processor.process

    def slow(*args, **kwargs):
        broker.clock.advance(25)
        return original(*args, **kwargs)

    loop.processor.process = slow
    loop.run()
    assert len(sink.rows) == 3
    assert [op for op, _ in broker.calls].count("session_renew_lock") >= 1
```

- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** `src/receiver.py` (`ReceiveLoop`, `RecoveryTracker`, `StopReason`, `SESSION_ACCEPT_WAIT_SECONDS`) per the rules. Expose `loop.processor` and `loop.arm_write_always` as public attributes (tests replace them). The fake records `("session_renew_lock", path)` in `broker.calls` for `receiver.session.renew_lock()` (Task 4). Close receivers / clients in `finally` blocks; never swallow the error that ends the run.
- [ ] **Step 4:** PASS; ruff clean.
- [ ] **Step 5:** Commit: `feat: receive loop with stop rules, sessions, stop drain, capped recycling and catch-up`.

---

### Task 15: Peek pager (C4)

**Owner skill:** `component-develop`.

**Files:**
- Modify: `src/receiver.py`
- Test: `tests/unit/test_peek_pager.py`

**Interfaces:**
- Consumes: Task 14 (`RecoveryTracker`, `StopReason`, `SESSION_ACCEPT_WAIT_SECONDS`); `PeekCursor` (Task 6).
- Produces: `class PeekPager`: `__init__(self, *, connector, entity: EntityRef, info: EntityInfo, config: Configuration, processor: BatchProcessor, stats: RunStats, cursor: PeekCursor | None, t0: datetime, clock: Callable[[], datetime], monotonic: Callable[[], float] | None = None)`; `run(self) -> PeekCursor | None` (incremental → the new cursor; full → `None`).

Rules (spec §6.7): incremental on `info.partitioned is True` → `UserException("Incremental Fetch cannot page a partitioned entity reliably; use Full Fetch.")` before peeking; heuristic partitioned detection mid-run → the same `UserException` (nothing written for that page, cursor unchanged). Start: cursor for the same `entity.path` → `last_sequence_number + 1`, a different path → 1 + INFO log. Pages: `peek_messages(250, sequence_number=next)` (full fetch on a partitioned entity: `sequence_number=0` cursor mode). Per message: skip `expires_at_utc < clock()` (`stats.expired_skipped += 1`); `stop_at_job_start` and a **non-`SCHEDULED`** message with `enqueued_time_utc >= t0` → stop before it (`WATERMARK`; WARNING `watermark_approximate` on partitioned / session entities; `SCHEDULED` messages are exported and never stop the peek); `max_messages`; `max_duration`. Write via `processor.process(receiver, page_messages, tracker.generation)` (the processor's settler is `NoopSettler`). Keep `written: set[int]` of the sequence numbers written in this run's current page; `needs_recycle` → reopen (`tracker.unreadable_retry()`, `stats.unreadable_recycles += 1`) and re-peek **from the first unreadable message's sequence number**, dropping every message whose sequence number is already in `written` before calling the processor — no duplicate rows. New cursor = highest written sequence number (unchanged when nothing was written). Session entities (full fetch only — the model refuses incremental): loop `NEXT_AVAILABLE_SESSION` receivers opened with `max_wait_time=SESSION_ACCEPT_WAIT_SECONDS` (**never** `idle_timeout_seconds`, which is hidden and ignored in peek mode), peek each from 1, stop on a revisited session or `OperationTimeoutError`, WARNING key `peek_sessions` once.

- [ ] **Step 1: Write the failing tests** — `tests/unit/test_peek_pager.py`:

```python
from datetime import timedelta

import pytest
from keboola.component.exceptions import UserException

from client import ServiceBusConnector
from configuration import AuthConfiguration, Configuration
from entity import EntityInfo, EntityRef
from receiver import PeekPager
from settlement import BatchProcessor, UnreadableHandler, make_settler
from state import PeekCursor
from stats import RunStats
from tests.fakes.recording import RecordingSink

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"


def pager(broker, *, fetch_mode="incremental_fetch", queue="q", cursor=None, info=None, limits=None, t0=None, session=False):
    source = {"entity_type": "queue", "queue_name": queue, "settlement_mode": "peek", "fetch_mode": fetch_mode, "session_enabled": session}
    params = {"#connection_string": SAS, "source": source}
    if limits:
        params["limits"] = limits
    config = Configuration(**params)
    entity = EntityRef.from_source(config.source)
    stats = RunStats(mode="peek")
    sink = RecordingSink()
    proc = BatchProcessor(
        entity=entity, mode=config.source.settlement_mode, body_format=config.body.body_format, sink=sink,
        registry=None, settler=make_settler(config.source.settlement_mode, pending=None, entity=entity, stats=stats),
        unreadable=UnreadableHandler(policy=config.body.unreadable_body, mode=config.source.settlement_mode, is_sub_queue=False, stats=stats),
        stats=stats, clock=broker.clock.now,
    )
    p = PeekPager(
        connector=ServiceBusConnector(AuthConfiguration(**{"#connection_string": SAS}), "kbc-test"),
        entity=entity, info=info or EntityInfo(), config=config, processor=proc, stats=stats, cursor=cursor,
        t0=t0 or broker.clock.now() + timedelta(hours=1), clock=broker.clock.now,
        monotonic=lambda: broker.clock.now().timestamp(),
    )
    return p, sink, stats


def test_incremental_two_runs_nothing_settled(broker):
    q = broker.add_queue("q")
    first = [q.send(b"a"), q.send(b"b")]
    p, sink, _ = pager(broker)
    cursor = p.run()
    assert cursor == PeekCursor(entity_path="q", last_sequence_number=first[-1]) and len(sink.rows) == 2
    assert q.sequence_numbers() == first  # nothing removed or locked
    third = q.send(b"c")
    p2, sink2, _ = pager(broker, cursor=cursor)
    assert p2.run().last_sequence_number == third and [r["body"] for r in sink2.rows] == ["c"]


def test_cursor_for_other_entity_resets(broker):
    q = broker.add_queue("q")
    q.send(b"a")
    p, sink, _ = pager(broker, cursor=PeekCursor(entity_path="other", last_sequence_number=999))
    p.run()
    assert len(sink.rows) == 1


def test_full_fetch_states_and_expired_skipped(broker):
    q = broker.add_queue("q")
    d = q.send(b"d")
    q.defer_existing(d)
    q.send(b"s", scheduled_at=broker.clock.now() + timedelta(hours=1))
    q.send(b"e", ttl_seconds=1)
    broker.clock.advance(5)
    p, sink, stats = pager(broker, fetch_mode="full_fetch")
    assert p.run() is None
    assert sorted(r["state"] for r in sink.rows) == ["DEFERRED", "SCHEDULED"] and stats.expired_skipped == 1


def test_incremental_refused_on_partitioned(broker):
    broker.add_queue("q", partitioned=True).send(b"a", partition=2)
    p, sink, _ = pager(broker, info=EntityInfo(partitioned=True))
    with pytest.raises(UserException, match="Full Fetch"):
        p.run()
    assert sink.rows == []


def test_incremental_refused_by_heuristic(broker):
    broker.add_queue("q", partitioned=True).send(b"a", partition=2)
    p, sink, _ = pager(broker)
    with pytest.raises(UserException, match="Full Fetch"):
        p.run()
    assert sink.rows == []


def test_watermark_and_max_messages(broker):
    q = broker.add_queue("q")
    for _ in range(3):
        q.send(b"old")
    broker.clock.advance(10)
    t0 = broker.clock.now()
    q.send(b"new")
    p, sink, _ = pager(broker, t0=t0)
    p.run()
    assert [r["body"] for r in sink.rows] == ["old", "old", "old"]
    p2, sink2, _ = pager(broker, limits={"max_messages": 2}, t0=t0)
    p2.run()
    assert len(sink2.rows) == 2


def test_full_fetch_sessions(broker):
    s = broker.add_queue("s", sessions=True)
    s.send(b"a", session_id="A")
    s.send(b"b", session_id="B")
    p, sink, stats = pager(broker, fetch_mode="full_fetch", queue="s", session=True)
    p.run()
    assert sorted(r["body"] for r in sink.rows) == ["a", "b"] and "peek_sessions" in stats.warnings
    assert all(r.kwargs.get("max_wait_time") == 5 for r in broker.receivers if r.kwargs.get("session_id"))


def test_session_wait_ignores_hidden_idle_timeout(broker):
    broker.add_queue("s", sessions=True).send(b"a", session_id="A")
    source = {"entity_type": "queue", "queue_name": "s", "settlement_mode": "peek", "fetch_mode": "full_fetch",
              "session_enabled": True, "idle_timeout_seconds": 99}
    config = Configuration(**{"#connection_string": SAS, "source": source})
    assert config.source.idle_timeout_seconds == 99  # kept by the model, but must not drive peek
    p, _, _ = pager(broker, fetch_mode="full_fetch", queue="s", session=True)
    p.config = config
    p.run()
    assert {r.kwargs.get("max_wait_time") for r in broker.receivers if r.kwargs.get("session_id")} == {5}


def test_repeek_after_unreadable_retry_does_not_duplicate(broker):
    q = broker.add_queue("q")
    seqs = [q.send(b"a"), q.send(b"b"), q.send(b"c")]
    broker.inject_body_error(seqs[1], times=1)
    p, sink, stats = pager(broker)
    cursor = p.run()
    assert [r["body"] for r in sink.rows].count("a") == 1
    assert sorted(r["sequence_number"] for r in sink.rows) == [str(s) for s in seqs]
    assert stats.unreadable_recycles == 1 and cursor.last_sequence_number == seqs[-1]


def test_scheduled_message_does_not_stop_watermark(broker):
    q = broker.add_queue("q")
    q.send(b"sched", scheduled_at=broker.clock.now() + timedelta(hours=2))
    q.send(b"old")
    t0 = broker.clock.now() + timedelta(seconds=1)
    p, sink, _ = pager(broker, fetch_mode="full_fetch", t0=t0)
    p.run()
    assert sorted(r["body"] for r in sink.rows) == ["old", "sched"]
```

- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** `PeekPager` per the rules; keep `config` a public attribute (a test swaps it).
- [ ] **Step 4:** PASS; ruff clean.
- [ ] **Step 5:** Commit: `feat: peek pager (incremental cursor, full fetch, partition/session handling, expired skip)`.

---

### Task 16: Component orchestrator, dev-branch guard, sync actions

**Owner skill:** `component-develop`.

**Files:**
- Modify: `src/component.py`
- Test: `tests/unit/test_component.py`

**Interfaces:**
- Consumes: everything above.
- Produces: `class Component(ComponentBase)` with `run()` and sync actions `testConnection`, `listQueues`, `listTopics`, `listSubscriptions`, `previewMessages`, `entityInfo`; `@dataclass class RunContext` (`config`, `entity`, `state`, `stats`, `info`, `registry`, `pending`, `t0`, `peek_cursor`, `output`); constant `DEV_BRANCH_MESSAGE`.

`run()` (≤ 30 lines) exactly:

```python
def run(self) -> None:
    """Extract one Service Bus entity per row into one Storage table (spec §6.1)."""
    config = self._load_run_config()
    self._guard_dev_branch(config)
    run = self._start_run(config)
    with self._open_output(config, run) as output:
        processor = self._build_processor(config, run, output)
        if config.source.settlement_mode.is_destructive:
            self._commit_pending(config, run)
            self._reconcile_deferrals(config, run, processor)
            self._consume(config, run, processor)
        else:
            self._peek(config, run, processor)
    self._finish(config, run)
```

Private methods (each small; spec §6.1–§6.12):
- `__init__`: `super().__init__()`; `self._auth = AuthConfiguration(**self.configuration.parameters)`; `self._connector = ServiceBusConnector(self._auth, self._client_identifier())`; `configure_logging(self._connector.secrets, debug=logging.getLogger().isEnabledFor(logging.DEBUG))`. No network.
- `_client_identifier()`: `f"kbc-{env.config_id or 'local'}-{env.config_row_id or 'root'}"[:64]` from `self.environment_variables`.
- `_guard_dev_branch(config)`: destructive mode and `self.environment_variables.branch_id` and not `config.destructive_in_branch` → `UserException(DEV_BRANCH_MESSAGE.format(mode=...))`, where `DEV_BRANCH_MESSAGE = "Settlement mode '{mode}' deletes or hides messages on the production Service Bus entity, and this job runs in a development branch. Use Peek mode in branches, or, to consume production messages from this branch on purpose, open the configuration in debug mode and add \"destructive_in_branch\": true under parameters."`.
- `_start_run(config)`: `t0 = datetime.now(UTC)`; `EntityRef.from_source`; `ExtractorState.load(self.get_state_file())`; `RunStats`; `load_entity_info` + `log_entity_counts(info, entity)` (L1); session mismatch (`info.requires_session is not None and not entity.is_sub_queue and info.requires_session != config.source.session_enabled`) → `UserException` (enable / disable Sessions); `log_effective_settings`; `FlattenRegistry(state.flatten_columns, reserved=metadata_column_names())` only for flatten; `PendingSetBuilder()`.
- `_open_output(config, run)`: `OutputTable(table_name=config.destination.table_name or run.entity.default_table_name(), destination=config.destination, body_format=config.body.body_format, registry=run.registry, create_definition=self.create_out_table_definition, write_manifest=self.write_manifest)`; the `with` block exits through `OutputTable.__exit__`, which materialises with `success = exc_type is None`.
- `_build_processor`: `run.output = output`; `BatchProcessor` with `make_settler(mode, pending=run.pending, entity=run.entity, stats=run.stats, arm=output.arm_write_always)` (only `CompleteSettler` uses `arm`) and `UnreadableHandler(policy=config.body.unreadable_body, mode=mode, is_sub_queue=run.entity.is_sub_queue, stats=run.stats)`, `clock=lambda: datetime.now(UTC)`.
- `_commit_pending`: `run.pending.carry(PendingCommitter(self._connector, configured=run.entity, stats=run.stats).commit(run.state.pending_commit))`.
- `_reconcile_deferrals`: C2 → `OrphanScanner(...).scan()`; C1 / C3 → `ForeignDeferralProbe(...).probe()`.
- `_consume`: `ReceiveLoop(..., state_size=lambda: self._projected_state(run).size_bytes(), arm_write_always=run.output.arm_write_always)` `.run()` (the loop arms only in C3); C3 → WARNING key `at_most_once` ("receive_and_delete deletes messages on delivery: a failure before the rows reach Storage loses them."); `prefetch_count > 1` → WARNING key `prefetch`.
- `_peek`: non-empty `run.state.pending_commit` → WARNING key `pending_carried` ("N message(s) deferred by an earlier defer-commit run are still pending; they are deleted by the next run in a destructive mode."); `run.peek_cursor = PeekPager(...).run()`.
- `_projected_state(run) -> ExtractorState`: `state = run.state.model_copy(deep=True)`; destructive → `state.pending_commit = run.pending.build(format_timestamp(datetime.now(UTC)))`; peek + incremental → `state.peek_cursor = run.peek_cursor`; flatten → `state.flatten_columns = run.registry.to_state()`; returns it — the **whole** output state, so the C2 budget (spec §6.5 / §6.8) counts the registry, cursor and carried keys too.
- `_finish`: `self.write_state_file(self._projected_state(run).to_dict())`; `run.stats.log_summary()`.
- Sync actions (all wrap SDK errors with `to_user_exception`):
  - `testConnection`: parameters contain `source` → `Configuration`; open the entity receiver (PEEK_LOCK, prefetch 1, keep_alive 0; sessions `NEXT_AVAILABLE_SESSION`, `max_wait_time=SESSION_ACCEPT_WAIT_SECONDS`) and `peek_messages(1)`; `OperationTimeoutError` → `ValidationResult("Connected to Azure Service Bus. No session with messages is available right now.", MessageType.SUCCESS)`; success → `ValidationResult("Connected to Azure Service Bus and read '<path>'.", MessageType.SUCCESS)`. No `source` → `probe_management(self._connector)` → `ValidationResult("Connected to the Service Bus namespace.", MessageType.SUCCESS)`.
  - `listQueues` / `listTopics` → `[SelectElement(value=n, label=n) for n in list_entity_names(...)]`; `listSubscriptions` → needs `source.topic_name` (missing → `UserException("Select a topic first.")`).
  - `previewMessages` → same receiver rules as `testConnection` → peek 10 → `ValidationResult(render_preview(messages), MessageType.TABLE)` (empty → `ValidationResult("The entity has no messages to preview.", MessageType.INFO)`).
  - `entityInfo` → `ValidationResult(describe_entity(...), MessageType.INFO)`.
- `__main__`: `UserException` → `logger.error(redact_secrets(str(e)))`, `sys.exit(1)`; other → `logger.exception("Component failed with an unexpected error")`, `sys.exit(2)`.

- [ ] **Step 1: Write the failing tests** — `tests/unit/test_component.py`:

```python
import inspect
import json
from pathlib import Path

import pytest
from keboola.component.exceptions import UserException

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"


def datadir(tmp_path: Path, parameters: dict, action: str = "run", state: dict | None = None) -> Path:
    for sub in ("in/tables", "out/tables", "out/files"):
        (tmp_path / sub).mkdir(parents=True, exist_ok=True)
    (tmp_path / "config.json").write_text(json.dumps({"action": action, "parameters": parameters, "storage": {}}))
    if state is not None:
        (tmp_path / "in" / "state.json").write_text(json.dumps(state))
    return tmp_path


def component(tmp_path, monkeypatch, parameters, **kwargs):
    monkeypatch.setenv("KBC_DATADIR", str(datadir(tmp_path, parameters, **kwargs)))
    from component import Component

    return Component()


PARAMS = {"#connection_string": SAS, "source": {"entity_type": "queue", "queue_name": "q"}}


def test_run_is_a_thin_orchestrator():
    from component import Component

    assert len(inspect.getsource(Component.run).splitlines()) <= 30


def test_run_c1_writes_table_manifest_and_state(broker, tmp_path, monkeypatch):
    broker.add_queue("q").send(b"hello")
    comp = component(tmp_path, monkeypatch, PARAMS)
    comp.execute_action()
    rows = (tmp_path / "out/tables/q.csv").read_text().splitlines()
    assert rows[0].startswith("sequence_number,") and rows[1].endswith(",hello")
    manifest = json.loads((tmp_path / "out/tables/q.csv.manifest").read_text())
    assert manifest["write_always"] is True and manifest["incremental"] is True
    assert json.loads((tmp_path / "out/state.json").read_text())["pending_commit"] == []


def test_dev_branch_guard(broker, tmp_path, monkeypatch):
    broker.add_queue("q").send(b"x")
    monkeypatch.setenv("KBC_BRANCHID", "123")
    comp = component(tmp_path, monkeypatch, PARAMS)
    with pytest.raises(UserException, match="destructive_in_branch"):
        comp.execute_action()


def test_dev_branch_override_and_peek(broker, tmp_path, monkeypatch):
    broker.add_queue("q").send(b"x")
    monkeypatch.setenv("KBC_BRANCHID", "123")
    component(tmp_path / "a", monkeypatch, {**PARAMS, "destructive_in_branch": True}).execute_action()
    peek = {**PARAMS, "source": {**PARAMS["source"], "settlement_mode": "peek"}}
    component(tmp_path / "b", monkeypatch, peek).execute_action()


def test_list_queues_returns_select_elements(broker, tmp_path, monkeypatch, capsys):
    broker.add_queue("b")
    broker.add_queue("a")
    component(tmp_path, monkeypatch, {"#connection_string": SAS}, action="listQueues").execute_action()
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert [item["value"] for item in out] == ["a", "b"]


def test_session_mismatch_detected_from_metadata(broker, tmp_path, monkeypatch):
    broker.add_queue("q", sessions=True)
    comp = component(tmp_path, monkeypatch, PARAMS)
    with pytest.raises(UserException, match="Sessions"):
        comp.execute_action()


@pytest.mark.parametrize(
    "mode, armed", [("complete", True), ("receive_and_delete", True), ("defer_commit", False), ("peek", False)]
)
def test_write_always_switch_per_mode(broker, tmp_path, monkeypatch, mode, armed):
    broker.add_queue("q").send(b"x")
    params = {**PARAMS, "source": {**PARAMS["source"], "settlement_mode": mode}}
    component(tmp_path, monkeypatch, params).execute_action()
    manifest = json.loads((tmp_path / "out/tables/q.csv.manifest").read_text())
    assert manifest.get("write_always", False) is armed


def test_c1_empty_entity_never_arms(broker, tmp_path, monkeypatch):
    broker.add_queue("q")
    component(tmp_path, monkeypatch, PARAMS).execute_action()
    assert json.loads((tmp_path / "out/tables/q.csv.manifest").read_text()).get("write_always", False) is False


def test_counts_logged_when_management_available(broker, tmp_path, monkeypatch, caplog):
    import logging

    broker.add_queue("q").send(b"x")
    with caplog.at_level(logging.INFO):
        component(tmp_path, monkeypatch, PARAMS).execute_action()
    assert any("holds 1 active" in r.getMessage() for r in caplog.records)


def test_state_budget_counts_whole_state(broker, tmp_path, monkeypatch):
    import state as state_mod

    broker.add_queue("q").send(b"x")
    registry = [{"path": [f"k{i}"], "column": f"body_k{i}"} for i in range(50)]
    monkeypatch.setattr(state_mod, "STATE_BUDGET_BYTES", 1500)
    import receiver as receiver_mod

    monkeypatch.setattr(receiver_mod, "STATE_BUDGET_BYTES", 1500)
    params = {**PARAMS, "source": {**PARAMS["source"], "settlement_mode": "defer_commit"}}
    comp = component(tmp_path, monkeypatch, params, state={"version": 1, "flatten_columns": registry})
    comp.execute_action()
    assert json.loads((tmp_path / "out/state.json").read_text())["pending_commit"] == []  # stopped at the budget
```

- [ ] **Step 2:** FAIL.
- [ ] **Step 3: Implement** `src/component.py` per the interface.
- [ ] **Step 4:** `uv run pytest -q` → all unit + fake tests PASS; `uv run ruff check src tests` and `uv run ty check` clean.
- [ ] **Step 5:** Commit: `feat: thin run() orchestrator, dev-branch guard and six sync actions`.

---

### Task 17: Config UI, component_config, README

**Owner skill:** `component-build-ui` (schemas, uiOptions); `component-develop` for README / descriptions (consult `component-dev-portal` for which `component_config/` files the CI sync writes).

**Files:**
- Modify: `component_config/configSchema.json`, `component_config/configRowSchema.json`, `component_config/uiOptions.md`, `component_config/component_short_description.md`, `component_config/component_long_description.md`, `component_config/configuration_description.md`, `component_config/sample-config/config.json` (+ remove the scaffold's sample `in/` table), `README.md`
- Test: `tests/unit/test_schemas.py`

**Interfaces:** Consumes the field list of spec §5 (names, enums, defaults, gating) and the sync-action names of Task 16.

Schema content (spec §5.1–§5.6): root = `auth_type` (enum + `enum_titles` "Connection string (SAS)" / "Service principal (Entra ID)", default `connection_string`), `#connection_string` (password, gated on SAS, tooltip naming *Shared access policies* and Listen rights), `tenant_id`, `client_id`, `#client_secret`, `fully_qualified_namespace` (gated on SP, tooltips as in the writer plus the **Azure Service Bus Data Receiver** role), and a `test_connection` button (`format: test-connection`). Row = `source` (`grid-strict`), `limits`, `body`, `destination` sections, `advanced_options` checkbox and gated `advanced` section, with the fields, enums, defaults and `options.dependencies` of §5.2 / §5.5; creatable async selects with `autoload` for `queue_name` (`listQueues`), `topic_name` (`listTopics`), `subscription_name` (`listSubscriptions`, reloading on `topic_name`), each declaring `"enum": []` (required for async selects); buttons `test_connection` (`format: test-connection`), `preview_messages` (`previewMessages`, label "Preview Messages"), `entity_info` (`entityInfo`, label "Show Entity Details"). Tooltips: `settlement_mode` (one line per mode with its guarantee / loss window; C2 exclusive-consumer rule; C3 at-most-once), `body_format` and `primary_key` ("changes the output columns / key — drop the existing table first"); the `primary_key` description names why the picker exists (sequence numbers are unique only per entity — use the composite key when several entities share a table) and its `message_id` option warns "Producer-set: may be empty or reused, in which case upserts merge different messages."; `json_flatten` behaviour and limits (`body_unmapped` holds keys not yet promoted — in complete / receive-and-delete modes a key becomes a column one run after it first appears; state reset and shared tables; prefer defer-commit), `session_enabled`, `stop_at_job_start`, `max_messages` ("0 = no limit"), `idle_timeout_seconds`, `table_name` (empty = derived pattern), the dropdown tooltip about SP / Manage rights. `destructive_in_branch` appears **nowhere**. `uiOptions.md` = `["genericDockerUI", "genericDockerUI-rows"]`. Descriptions and README: what it does, auth + provisioning (Listen / Data Receiver, IP allowlisting of the stack egress IPs with the help-page link, `disableLocalAuth` → SP, private endpoints unsupported), settlement guarantees table, C2 limits, dev-branch override, body formats + flatten rules, the `body_unmapped` known behaviour (spec §6.10) and limits, the `write_always` per-mode table (spec §6.9), large-body guidance (lower batch size), output columns + types, state, the K2 WebSocket note (spec §11), sync-action caveat, how to run the tests.

- [ ] **Step 1: Write the failing tests** — `tests/unit/test_schemas.py`:

```python
import json
from pathlib import Path

import configuration
from component import Component

ROOT = Path(__file__).resolve().parents[2] / "component_config"
ROOT_SCHEMA = json.loads((ROOT / "configSchema.json").read_text())
ROW_SCHEMA = json.loads((ROOT / "configRowSchema.json").read_text())
SYNC_ACTIONS = {"testConnection", "listQueues", "listTopics", "listSubscriptions", "previewMessages", "entityInfo"}


def walk(schema: dict, path: str = ""):
    for name, prop in schema.get("properties", {}).items():
        yield f"{path}{name}", prop, schema
        if prop.get("type") == "object":
            yield from walk(prop, f"{path}{name}.")


def test_hidden_parameter_absent():
    text = json.dumps(ROOT_SCHEMA) + json.dumps(ROW_SCHEMA)
    assert "destructive_in_branch" not in text


def test_every_enum_has_matching_titles():
    for _, prop, _ in list(walk(ROOT_SCHEMA)) + list(walk(ROW_SCHEMA)):
        if "enum" in prop and prop.get("enum"):
            titles = prop.get("options", {}).get("enum_titles")
            assert titles and len(titles) == len(prop["enum"]), prop


def test_enum_values_match_model():
    expected = {
        "source.entity_type": configuration.EntityType,
        "source.sub_queue": configuration.SubQueue,
        "source.settlement_mode": configuration.SettlementMode,
        "source.fetch_mode": configuration.FetchMode,
        "body.body_format": configuration.BodyFormat,
        "body.unreadable_body": configuration.UnreadablePolicy,
        "destination.load_type": configuration.LoadType,
        "destination.primary_key": configuration.PrimaryKey,
    }
    props = {path: prop for path, prop, _ in walk(ROW_SCHEMA)}
    for path, enum_cls in expected.items():
        assert props[path]["enum"] == [e.value for e in enum_cls], path
    root = {path: prop for path, prop, _ in walk(ROOT_SCHEMA)}
    assert root["auth_type"]["enum"] == [e.value for e in configuration.AuthType]


def test_secret_fields_are_hash_prefixed_passwords():
    root = {path: prop for path, prop, _ in walk(ROOT_SCHEMA)}
    for key in ("#connection_string", "#client_secret"):
        assert root[key]["format"] == "password"


def test_dependencies_reference_siblings():
    for _, prop, parent in list(walk(ROOT_SCHEMA)) + list(walk(ROW_SCHEMA)):
        for dep in prop.get("options", {}).get("dependencies", {}):
            assert dep in parent["properties"], dep


def test_async_actions_exist_in_code():
    actions = {
        prop["options"]["async"]["action"]
        for _, prop, _ in list(walk(ROOT_SCHEMA)) + list(walk(ROW_SCHEMA))
        if "async" in prop.get("options", {})
    }
    assert actions <= SYNC_ACTIONS and {"listQueues", "listTopics", "listSubscriptions"} <= actions
    from keboola.component import base

    assert Component is not None and SYNC_ACTIONS <= set(base._SYNC_ACTION_MAPPING)


def test_async_selects_declare_empty_enum():
    for _, prop, _ in walk(ROW_SCHEMA):
        if "async" in prop.get("options", {}):
            assert prop.get("enum") == [], prop


def test_message_id_primary_key_warns():
    props = {path: prop for path, prop, _ in walk(ROW_SCHEMA)}
    text = json.dumps(props["destination.primary_key"])
    assert "may be empty or reused" in text


def test_required_is_array_form():
    for _, prop, _ in list(walk(ROOT_SCHEMA)) + list(walk(ROW_SCHEMA)):
        assert not isinstance(prop.get("required"), bool)


def test_ui_options():
    assert json.loads((ROOT / "uiOptions.md").read_text()) == ["genericDockerUI", "genericDockerUI-rows"]


def test_root_schema_not_empty():
    assert ROOT_SCHEMA.get("properties")
```

- [ ] **Step 2:** FAIL (scaffold schemas).
- [ ] **Step 3: Implement** the schemas (via `component-build-ui`; run its schema tester if available), uiOptions, descriptions, sample config (merged root + row example with dummy secrets), README. No customer names; no concrete namespace / tenant / SP ids.
- [ ] **Step 4:** PASS; ruff clean; `grep -rniE "sharedaccesskey=[A-Za-z0-9+/=]{20,}" component_config README.md` → only the dummy `c2VjcmV0`-style values.
- [ ] **Step 5:** Commit: `feat(config-ui): root auth + row source/limits/body/destination schemas, sync-action buttons, README`.

---

## Phase 5 — Functional tests (owner skill: `component-test`)

### Task 18: Functional harness + sync-action cases

**Owner skill:** `component-test` (AMQP note: the SDK-mock harness replaces VCR cassettes — writer precedent; tracker Phase 5).

**Files:**
- Create: `tests/functional/conftest.py`, `tests/functional/test_sync_actions.py`, `tests/setup/configs.json`
- Test: the files above

**Interfaces:**
- Consumes: `FakeBroker`, `install` (Task 4); the component (Task 16).
- Produces (in `tests/functional/conftest.py`):
  - autouse fixture `fake_broker(monkeypatch) -> FakeBroker` (builds + installs; also `monkeypatch.delenv("KBC_BRANCHID", raising=False)`).
  - `CONFIGS: dict[str, dict]` loaded from `tests/setup/configs.json` (wrapped format: `[{"name", "description", "config"}]`).
  - `@dataclass class CaseResult`: `exit_code: int`, `out_dir: Path`, `stdout: str`, `stderr: str`, `state: dict | None`, `tables: dict[str, list[dict]]` (CSV rows per output file), `manifests: dict[str, dict]`.
  - `run_case(name: str, tmp_path: Path, monkeypatch, capsys, *, state: dict | None = None, env: dict[str, str] | None = None, overrides: dict | None = None) -> CaseResult` — materialises `data/config.json` (deep-merging `overrides` into `parameters`), optional `in/state.json`, sets `KBC_DATADIR` (+ `env`), runs `runpy.run_path("src/component.py", run_name="__main__")`, captures `SystemExit` (no exit → 0), reads outputs.
  - `run_twice(name, tmp_path, monkeypatch, capsys, **kw) -> tuple[CaseResult, CaseResult]` — second run gets the first run's `out/state.json` as `in/state.json` (or the first run's *input* state when the first run wrote none — a failed run), same broker.
  - `run_chain(names: list[str], tmp_path, monkeypatch, capsys, *, before_each: Callable[[int], None] | None = None) -> list[CaseResult]` — the same chaining rule over N runs; `before_each(i)` seeds / injects faults before run `i`.
  - `sync_result(result: CaseResult) -> object` — parses the last stdout line as JSON.

`tests/setup/configs.json` contains one wrapped entry per case of spec §8 with dummy credentials only (SAS `SharedAccessKey=ZmFrZWtleWZha2VrZXlmYWtla2V5ZmFrZWtleTEyMzQ1Njc4OTA=`, SP `tenant_id` / `client_id` = `00000000-0000-0000-0000-000000000000`, `#client_secret` = `dummy-secret`).

- [ ] **Step 1: Write the failing tests** — `tests/functional/test_sync_actions.py` implementing cases `01`–`16` of spec §8. Each test seeds the broker, calls `run_case`, and asserts exactly:

| Case | Seed | Assertions |
|---|---|---|
| `01_testConnection_queue` | queue `orders` with 2 messages | exit 0; result `status == "success"`, message mentions `orders`; both messages still `ACTIVE` with `delivery_count 0` |
| `02_testConnection_bad_conn_string` | — (`#connection_string = "not-a-valid-connection-string"`) | exit 1; stderr contains "connection string"; no `SharedAccessKey=` value in stdout / stderr |
| `03_testConnection_auth_or_missing` | `broker.auth_failure = True` | exit 1; stderr contains "IP firewall" and the entity name |
| `04_testConnection_session_empty` | session queue `sess`, no messages | exit 0; message contains "No session" |
| `05_testConnection_root_sp` | queue `a`; SP auth; no `source` | exit 0; "Connected to the Service Bus namespace" |
| `06_testConnection_root_listen_sas` | `management_denied = True`; no `source` | exit 1; stderr contains "only from a row" |
| `07_listQueues_sp` | queues `b`, `a` | values `["a", "b"]` |
| `08_listQueues_listen_sas_empty` | `management_denied = True` | result `[]`, exit 0 |
| `09_listTopics_sp_auth_failure` | SP, `management_denied = True` | exit 1; stderr contains "Data Receiver" |
| `10_listTopics_sp` | subscriptions on topics `t1`, `t2` | values `["t1", "t2"]` |
| `11_listSubscriptions_sp` | topic `t` with subs `x`, `y`; `source.topic_name = "t"` | values `["x", "y"]` |
| `12_listSubscriptions_missing_topic` | `source.topic_name = "nope"` | exit 1 |
| `13_previewMessages` | queue with 3 messages | result `type == "table"`, message has a header row + 3 data rows; all 3 still `ACTIVE`, `delivery_count 0` |
| `14_previewMessages_missing_entity` | subscription `t/missing` | exit 1; stderr contains "was not found" |
| `15_entityInfo_sp` | subscription with one rule `r1: amount > 10` | message contains "Requires session", "r1" and "amount > 10" |
| `16_entityInfo_listen_sas` | `management_denied = True` | exit 1; stderr contains "Manage" |

  Write each row as its own test function named `test_<case>` using `run_case` / `sync_result`, e.g.:

```python
def test_01_testConnection_queue(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("orders")
    seqs = [q.send(b"a"), q.send(b"b")]
    result = run_case("01_testConnection_queue", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    payload = sync_result(result)
    assert payload["status"] == "success" and "orders" in payload["message"]
    assert all(q.state_of(s) == "ACTIVE" and q.delivery_count(s) == 0 for s in seqs)
```

- [ ] **Step 2:** `uv run pytest tests/functional/test_sync_actions.py -v` → FAIL until the harness exists, then each case passes against the Task-16 component (fix component bugs the cases reveal in the owning module, test-first).
- [ ] **Step 3: Implement** `tests/functional/conftest.py` and `tests/setup/configs.json` per the interface.
- [ ] **Step 4:** PASS; ruff clean.
- [ ] **Step 5:** Commit: `test: functional datadir harness on FakeBroker + sync-action cases (pass and fail per action)`.

---

### Task 19: Functional run cases — modes, entities, state

**Owner skill:** `component-test`.

**Files:**
- Create: `tests/functional/test_runs.py`, `tests/functional/expected/20_run_c1_queue/q.csv`, `tests/functional/expected/20_run_c1_queue/q.csv.manifest`
- Modify: `tests/setup/configs.json`

- [ ] **Step 1: Write the failing tests** — one test function per row (cases `20`–`45` of spec §8):

| Case | Seed / setup | Assertions |
|---|---|---|
| `20_run_c1_queue` | queue `q`: 2 JSON-ish text bodies with fixed `message_id`s, fixed clock | exit 0; `out/tables/q.csv` and its manifest equal the golden files byte-for-byte (after replacing `extracted_at_utc` by the frozen clock value); queue empty; state `pending_commit == []` |
| `21_run_c1_subscription_sp` | SP auth; subscription `t/s` with 2 messages | exit 0; 2 rows; `source_entity == "t/Subscriptions/s"`; `FakeCredential` used |
| `22_run_c2_first_run_defers` | queue with 3 messages, `defer_commit` | exit 0; 3 rows; all 3 `DEFERRED`; state `pending_commit[0].groups[0].ranges == [[1, 3]]` |
| `23_run_c2_second_run_commits` | `run_twice`: 3 messages before run 1, 2 more before run 2 | run 2: the first 3 are gone, the 2 new are `DEFERRED`, state ranges `[[4, 5]]`, `committed=3` in the summary log |
| `24_run_c2_commit_not_found_bisection` | state pending `[[1, 4]]` where seq 2 no longer exists | exit 0; summary shows `already_gone=1`, `committed=3` |
| `25_run_c2_commit_transient_retry_exhausted` | pending set + 4 injected `ServiceBusServerBusyError`; `time.sleep` patched to no-op | exit 1; stderr contains "next run retries"; nothing received (queue's active message untouched); no `out/state.json` written |
| `26_run_c2_orphan_recovery_plain` | orphan `DEFERRED` at seq 1 not in state, 2 active | exit 0; 3 rows; the orphan is in the new pending set |
| `27_run_c2_orphan_scan_locked_cluster` | 60 locked messages then an orphan | orphan recovered |
| `28_run_c2_orphan_scan_cap_warning` | 5,001 messages with `delivery_count=1`, `max_messages=1` | log contains "orphan scan incomplete" |
| `29_run_c2_orphan_guard_delivery_count` | orphan with `delivery_count=9` | not recovered; WARNING lists its sequence number |
| `30_run_c2_partitioned` | partitioned queue, 3 messages in 3 partitions, `run_twice` | run 1 state has 3 groups (partitions); run 2 commits all; both runs log the best-effort WARNING |
| `31_run_c2_session_entity` | session queue, sessions A and B, `session_enabled`, `run_twice` | run 2 commits both sessions' groups; WARNING "orphan recovery is not available for session entities" |
| `32_run_c2_dlq` | 2 messages dead-lettered on `q`, `sub_queue = dead_letter`, `run_twice` | run 2 leaves the DLQ empty; best-effort WARNING |
| `33_run_c2_state_budget` | `STATE_BUDGET_BYTES` monkeypatched to 1 | exit 0; log "state budget"; stop reason `state_budget` |
| `34_run_c3_receive_and_delete` | 3 messages, `max_messages=1` | exit 0; 3 rows (1 + drained buffer of 2); at-most-once WARNING |
| `35_run_c3_prefetch_refused` | `advanced_options` + `prefetch_count=5`, C3 | exit 1; stderr mentions `prefetch` |
| `36_run_c4_incremental_two_runs` | `run_twice`, 2 messages then 1 more | run 1 two rows, run 2 one row; nothing removed; state cursor = 3 |
| `37_run_c4_full_fetch` | one DEFERRED, one SCHEDULED, one expired | rows' `state` values `DEFERRED`, `SCHEDULED`; the expired one absent |
| `38_run_c4_incremental_partitioned_refused` | partitioned queue, peek incremental | exit 1; "Full Fetch" |
| `39_run_c4_leftover_pending_carried` | peek; input state with a pending set | exit 0; out state still has the identical `pending_commit`; WARNING "still pending"; deferred messages untouched |
| `40_run_c1_leftover_pending_committed` | C1; input state pending for 2 deferred messages | both deleted; out state `pending_commit == []` |
| `41_run_c1_foreign_deferral_probe` | C1; a `DEFERRED` message not in state | WARNING "not owned by this configuration"; the deferred message untouched |
| `42_run_sessions_loop` | session queue: A×2, B×1; C1 | 3 rows; queue empty |
| `43_run_session_mismatch` | session queue, `session_enabled` false | exit 1; "Sessions" |
| `44_run_dlq_c1` | 1 message dead-lettered with reason `R` / description `D` | row has `dead_letter_reason == "R"`, `source_entity == "q/$DeadLetterQueue"`; DLQ empty |
| `45_run_tdlq_c1` | message placed in the transfer DLQ | row `source_entity == "q/$Transfer/$DeadLetterQueue"` |

  The golden files for `20` are written once from a reviewed run (inspect the CSV by eye: header, typed values, body) and then compared byte-for-byte.
- [ ] **Step 2:** run → FAIL for missing configs / golden files; fix component bugs the cases reveal in the owning module, test-first.
- [ ] **Step 3:** add the configs to `tests/setup/configs.json`; create the golden files.
- [ ] **Step 4:** PASS; ruff clean.
- [ ] **Step 5:** Commit: `test: functional run cases for every settlement mode, entity type and state transition`.

---

### Task 20: Functional body / robustness / branch cases, sanitisation gate, full suite

**Owner skill:** `component-test`.

**Files:**
- Create: `tests/functional/test_bodies_and_robustness.py`, `tests/functional/test_sanitisation.py`
- Modify: `tests/setup/configs.json`

- [ ] **Step 1: Write the failing tests** — cases `46`–`67` of spec §8:

| Case | Seed / setup | Assertions |
|---|---|---|
| `46_run_body_base64` | body `b"\x00\x01"` | `body == "AAE="` |
| `47_run_body_value_sequence` | VALUE `{"k": "v"}`, SEQUENCE `[[1, "a"]]` | bodies `{"k":"v"}` / `[[1,"a"]]`; `body_type` `VALUE` / `SEQUENCE` |
| `48_run_body_charset_multisection` | DATA sections `[b"p\xe9", b"x"]`, content type `text/plain; charset=latin-1` | `body == "péx"` |
| `49_run_flatten_json` | body `{"order":{"id":1,"items":[1,2]},"a.b":1,"a":{"b":2}}` | no `body` column; columns `body_order_id`, `body_order_items` (`[1,2]`), `body_a_b`, `body_a_b_2`; state `flatten_columns` has 4 entries |
| `50_run_flatten_new_keys_second_run` | `run_twice`: run 1 `{"x":1}`, run 2 `{"y":2}` | run 2 header contains both `body_x` and `body_y` (`body_x` empty) |
| `51_run_flatten_not_json` | body `b"plain"`, flatten, C1 | 0 rows; message in DLQ with reason `NotJson` |
| `52_run_unreadable_body_two_connections` | `inject_body_error(seq, times=2)` | message dead-lettered `UnreadableBody`; ≥ 2 clients opened; 0 rows for it |
| `53_run_unreadable_on_dlq_degrades` | DLQ source + `inject_body_error(times=2)` | message stays in the DLQ; WARNING about sub-queues |
| `54_run_unreadable_share_abort` | 12 messages all with body errors, policy `leave` | exit 1; stderr mentions unreadable share |
| `55_run_receive_failure_recycle` | 3 messages, `inject_receive_error(TypeError, on_call=2)`, batch 1 | exit 0; 3 rows, each sequence number once; `recoveries=1` logged |
| `56_run_recoveries_exhausted` | errors on calls 1 and 2 (no progress) | exit 2 |
| `57_run_watermark_stop` | 2 messages before T0, 4 after (clock controlled), batch 2 | 4 rows written, 2 messages left |
| `58_run_max_messages` | 5 messages, `max_messages=2` | 2 rows |
| `59_run_empty_entity` | empty queue | exit 0; header-only CSV; manifest present |
| `60_run_full_load_composite_pk` | `full_load`, PK `source_entity_sequence_number` | manifest `incremental false`, PK `["source_entity","sequence_number"]` |
| `61_run_dev_branch_guard` | env `KBC_BRANCHID=1`, C1 | exit 1; stderr contains `destructive_in_branch`; messages untouched |
| `62_run_dev_branch_override` | env `KBC_BRANCHID=1`, `destructive_in_branch: true` | exit 0; messages consumed |
| `63_run_dev_branch_peek` | env `KBC_BRANCHID=1`, peek | exit 0 |
| `64_run_missing_creds` | no `#connection_string` | exit 1; stderr contains `connection_string` |
| `65_run_failure_write_always_per_mode` | 3 messages, batch 1, a non-recoverable error injected on receive call 3, parametrised over C1 / C2 / C3 / C4, plus C1 failing on call 1 | exit ≠ 0 in every variant; C1 / C3: `out/tables/q.csv` holds the 2 settled rows and the manifest has `write_always: true`; C2, C4 and C1-on-call-1: manifest `write_always` is `false` (a failed job uploads nothing) |
| `66_run_flatten_failed_run_new_keys` | C2, flatten; run 1: 2 messages `{"a":1}` then a non-recoverable error on call 2; run 2 (same broker, run 1's input state because run 1 wrote none): 1 message `{"a":2,"b":3}` | run 1: exit ≠ 0, CSV header ends with `body_unmapped` and has no `body_a`, the written row's `body_unmapped` is `{"a":"1"}`, no `out/state.json`; run 2: exit 0, header has `body_a`, `body_b`, `body_unmapped`, the row's `body_unmapped` empty, state registry `[a, b]` |
| `67_run_flatten_c1_promotion_lag` | C1, flatten, three runs chaining state; run 1 fails after one settled batch with key `k`; run 2 succeeds with key `k`; run 3 succeeds with key `k` | run 1: `write_always` true, no `body_k`, `body_unmapped` filled; run 2: still no `body_k`, `body_unmapped` filled, state registry contains `k`; run 3: header has `body_k`, `body_unmapped` empty |

- [ ] **Step 2:** FAIL → fix revealed component bugs test-first in the owning module.
- [ ] **Step 3: Sanitisation gate** — add `tests/functional/test_sanitisation.py`:

```python
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DUMMY_KEYS = {"ZmFrZWtleWZha2VrZXlmYWtla2V5ZmFrZWtleTEyMzQ1Njc4OTA=", "c2VjcmV0"}


def test_committed_fixtures_hold_only_dummy_secrets():
    for path in list((ROOT / "tests").rglob("*.json")) + list((ROOT / "tests").rglob("*.csv")) + list((ROOT / "component_config").rglob("*")):
        if not path.is_file():
            continue
        text = path.read_text(errors="ignore")
        for key in re.findall(r"SharedAccessKey=([^;\"\s]+)", text):
            assert key in DUMMY_KEYS, path
        assert "sig=" not in text or "sig=***" in text, path
```

  Also assert in `02_testConnection_bad_conn_string` and `03_…` that the dummy key never appears in stderr (redaction).
- [ ] **Step 4: Full suite:** `uv run pytest -q` → paste the `N passed` line into the tracker's Phase-5 evidence; `uv run ruff check src tests`; `uv run ty check`; `docker compose run --rm test` (the Dockerfile's test stage) green.
- [ ] **Step 5:** Commit: `test: body formats, flattening (body_unmapped promotion), robustness, dev-branch and write_always cases + sanitisation gate`.

---

## Later phases (not plan tasks — driven by the lifecycle tracker)

- **Phase 4 gate:** scoped `component-checklist-review` (`architecture`, `typing`, `configuration`, `error-handling`, `logging`, `output-state`, `infra`) — spec §14.
- **Phase 5 gate:** `testing`, `credentials`, `output-state`.
- **Phase 6 (`component-dev-portal` via `kbagent`):** register the six sync actions, `dataTypeSupport = authoritative`, `defaultBucket = true` (stage `in`), uiOptions, descriptions; fresh GET to confirm. Only portal-owned properties are patched by hand; `component_config/` values reach the portal through the CI sync.
- **Phase 7 (`component-test`, tier 4):** spec §9 smoke matrix on the `ex-*` entities with `runtime.tag` = newest branch build, seeding through the writer component; fresh-config UI acceptance; probes of the [inferred] items; one debug job + cassette secret grep.
- **Phase 8:** full `component-checklist-review`, then the single PR `initial-implementation → main` (never merged by the factory).

---

## Self-Review

1. **Spec coverage:** gate-1 amendments — `write_always` switch (spec §6.9) → Tasks 9, 11, 14, 16, 20 (case 65); `body_unmapped` + promotion rule (§6.10) → Tasks 7, 9, 11, 20 (cases 66–67); separate unreadable retry budget (§6.5–§6.6) → Tasks 11, 14, 15. §2.3–§2.5 → Tasks 2, 11, 14–16; §3 (auth, profile, pin) → Tasks 1, 3, 14; §4 in-scope rows → A (Tasks 5, 14, 15), B (3), C (11–15), D (2, 14, 15), E (8), F (7), G (9), H (6, 12, 13, 15), I (16, 17), J (3, 11–16), K (1, 3), L (5, 16); §5 → Tasks 2, 16, 17; §6.1–§6.12 → Tasks 5–16; §8 cases → Tasks 18–20 (+ unit tests per task); §9 → Phase 7; §11 → README (Task 17). Excluded rows have no task by design.
2. **Placeholder scan:** no placeholder markers; every test step has code or an exact per-case assertion table.
3. **Type consistency:** `EntityRef`, `EntityInfo`, `PendingSetBuilder`, `PendingEntity`, `FlattenRegistry` (`register` / `split` / `input_columns`), `OutputRow` / `OutputTable` / `RowSink` (`arm_write_always`, `close(success)`), `BatchProcessor` / `BatchResult.progressed`, `RecoveryTracker.unreadable_retry`, `RunStats`, `ServiceBusConnector` names and signatures match across Tasks 2–20.

## Execution Handoff

Plan complete. Execute with **superpowers:subagent-driven-development** (one fresh subagent per task, review between tasks), each subagent loading the owner skill named in its task. Tick the tracker's Phase 4 / Phase 5 boxes only on their independent gates, not when the last task is checked off.
