import csv
import inspect
import json
import logging
import runpy
import threading
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from azure.servicebus import NEXT_AVAILABLE_SESSION, ServiceBusReceiveMode
from keboola.component.exceptions import UserException

SAS = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=c2VjcmV0"
SRC = Path(__file__).resolve().parents[2] / "src"

_PLATFORM_ENV = (
    "KBC_BRANCHID",
    "KBC_CONFIGID",
    "KBC_CONFIGROWID",
    "KBC_STACKID",
    "KBC_PROJECT_FEATURE_GATES",
    "KBC_DATA_TYPE_SUPPORT",
    "KBC_COMPONENT_RUN_MODE",
    "KBC_LOGGER_ADDR",
)


@pytest.fixture(autouse=True)
def _isolate_platform(monkeypatch):
    """Hermetic platform env; undo what ComponentBase / configure_logging / a sync action leave on
    the root logger (keboola-owned handlers, the redaction filter, the FATAL level of a sync action)."""
    for name in _PLATFORM_ENV:
        monkeypatch.delenv(name, raising=False)
    from client import RedactingFilter

    root, servicebus = logging.getLogger(), logging.getLogger("azure.servicebus")
    level, sb_state = root.level, (servicebus.level, servicebus.propagate, list(servicebus.handlers))
    yield
    root.setLevel(level)
    servicebus.setLevel(sb_state[0])
    servicebus.propagate = sb_state[1]
    servicebus.handlers[:] = sb_state[2]
    for handler in list(root.handlers):
        if getattr(handler, "_keboola_owned", False):
            root.removeHandler(handler)
        for installed in [f for f in handler.filters if isinstance(f, RedactingFilter)]:
            handler.removeFilter(installed)


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


PARAMS: dict[str, Any] = {"#connection_string": SAS, "source": {"entity_type": "queue", "queue_name": "q"}}


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


@pytest.mark.parametrize("mode", ["complete", "defer_commit", "receive_and_delete", "peek"])
def test_every_mode_runs_when_branch_id_is_set(broker, tmp_path, monkeypatch, mode):
    # KBC_BRANCHID is set for default-branch jobs too on current stacks, so it must never block a run
    q = broker.add_queue("q")
    seq = q.send(b"x")
    monkeypatch.setenv("KBC_BRANCHID", "123")
    params = {**PARAMS, "source": {**PARAMS["source"], "settlement_mode": mode}}
    component(tmp_path, monkeypatch, params).execute_action()
    assert (tmp_path / "out/tables/q.csv").read_text().splitlines()[1].endswith(",x")
    assert (
        q.state_of(seq)
        == {"complete": None, "defer_commit": "DEFERRED", "receive_and_delete": None, "peek": "ACTIVE"}[mode]
    )


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


@pytest.mark.parametrize(
    "mode, warned", [("complete", True), ("receive_and_delete", True), ("defer_commit", False), ("peek", False)]
)
def test_legacy_queue_warns_and_leaves_manifest_to_library(broker, tmp_path, monkeypatch, caplog, mode, warned):
    # amendment 3: never hand-override the library's legacy-queue exclusion of write_always
    import logging

    import component as component_mod

    broker.add_queue("q").send(b"x")
    monkeypatch.setattr(component_mod.Component, "is_legacy_queue", property(lambda self: True))
    params = {**PARAMS, "source": {**PARAMS["source"], "settlement_mode": mode}}
    with caplog.at_level(logging.WARNING):
        component(tmp_path, monkeypatch, params).execute_action()
    manifest = json.loads((tmp_path / "out/tables/q.csv.manifest").read_text())
    assert "write_always" not in manifest  # the library's legacy exclusion stands
    assert any("legacy job queue" in r.getMessage() for r in caplog.records) is warned


def test_state_budget_counts_whole_state(broker, tmp_path, monkeypatch):
    import state as state_mod

    broker.add_queue("q").send(b"x")
    import hashlib

    registry = [{"path_sha1": hashlib.sha1(str(i).encode()).hexdigest(), "column": f"body_k{i}"} for i in range(50)]
    monkeypatch.setattr(state_mod, "STATE_BUDGET_BYTES", 1500)
    import receiver as receiver_mod

    monkeypatch.setattr(receiver_mod, "STATE_BUDGET_BYTES", 1500)
    params = {**PARAMS, "source": {**PARAMS["source"], "settlement_mode": "defer_commit"}}
    comp = component(tmp_path, monkeypatch, params, state={"version": 1, "flatten_columns": registry})
    comp.execute_action()
    assert json.loads((tmp_path / "out/state.json").read_text())["pending_commit"] == []  # stopped at the budget


# --- beyond the brief: identity, session rules, merge rule, warnings -------------------------------------


def with_source(**source) -> dict:
    return {**PARAMS, "source": {**PARAMS["source"], **source}}


def out_state(tmp_path: Path) -> dict:
    return json.loads((tmp_path / "out/state.json").read_text())


def warnings_logged(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


def test_client_identifier_from_platform_ids(broker, tmp_path, monkeypatch):
    broker.add_queue("q").send(b"x")
    monkeypatch.setenv("KBC_CONFIGID", "123")
    monkeypatch.setenv("KBC_CONFIGROWID", "456")
    component(tmp_path, monkeypatch, PARAMS).execute_action()
    assert {r.client_identifier for r in broker.receivers} == {"kbc-123-456"}


def test_client_identifier_defaults_and_is_capped(broker, tmp_path, monkeypatch):
    local = component(tmp_path / "a", monkeypatch, PARAMS)
    assert local._connector.client_identifier == "kbc-local-root"
    monkeypatch.setenv("KBC_CONFIGID", "f" * 100)  # an inline-config job: a sha1-like hash, used only as a label
    capped = component(tmp_path / "b", monkeypatch, PARAMS)
    assert capped._connector.client_identifier == ("kbc-" + "f" * 100)[:64]


def test_reverse_session_mismatch(broker, tmp_path, monkeypatch):
    broker.add_queue("q")
    comp = component(tmp_path, monkeypatch, with_source(session_enabled=True))
    with pytest.raises(UserException, match="disable Sessions"):
        comp.execute_action()


def test_sub_queue_skips_the_session_check(broker, tmp_path, monkeypatch):
    q = broker.add_queue("q", sessions=True)
    q.dead_letter_existing(q.send(b"x", session_id="a"), "reason", "description")
    component(tmp_path, monkeypatch, with_source(sub_queue="dead_letter")).execute_action()
    rows = (tmp_path / "out/tables/q_dead_letter.csv").read_text().splitlines()
    assert len(rows) == 2 and rows[1].endswith(",x")


def test_c2_defers_then_next_run_commits(broker, tmp_path, monkeypatch):
    q = broker.add_queue("q")
    q.send(b"x")
    params = with_source(settlement_mode="defer_commit")
    component(tmp_path / "a", monkeypatch, params).execute_action()
    first = out_state(tmp_path / "a")
    assert first["pending_commit"][0]["groups"][0]["ranges"] == [[1, 1]]
    assert q.state_of(1) == "DEFERRED"
    component(tmp_path / "b", monkeypatch, params, state=first).execute_action()
    assert q.sequence_numbers() == [] and out_state(tmp_path / "b")["pending_commit"] == []


def test_peek_carries_pending_set_with_warning_and_saves_cursor(broker, tmp_path, monkeypatch, caplog):
    broker.add_queue("q").send(b"x")
    pending = [
        {
            "entity": {
                "entity_type": "queue",
                "queue_name": "q",
                "topic_name": None,
                "subscription_name": None,
                "sub_queue": "none",
            },
            "groups": [{"session_id": None, "partition": 0, "max_body_bytes": 1, "ranges": [[5, 7]]}],
            "deferred_at_utc": "2026-09-23 09:00:00.000000",
        }
    ]
    state = {"version": 1, "pending_commit": pending}
    with caplog.at_level(logging.WARNING):
        component(tmp_path, monkeypatch, with_source(settlement_mode="peek"), state=state).execute_action()
    saved = out_state(tmp_path)
    assert saved["pending_commit"] == pending
    assert saved["peek_cursor"] == {"entity_path": "q", "last_sequence_number": 1}
    assert any(m.startswith("3 message(s) deferred by an earlier defer-commit run") for m in warnings_logged(caplog))
    assert ("receive_deferred_messages", "q") not in broker.calls  # C4 never commits


def test_peek_full_fetch_keeps_the_input_cursor(broker, tmp_path, monkeypatch):
    broker.add_queue("q").send(b"x")
    cursor = {"entity_path": "q", "last_sequence_number": 42}
    params = with_source(settlement_mode="peek", fetch_mode="full_fetch")
    component(tmp_path, monkeypatch, params, state={"version": 1, "peek_cursor": cursor}).execute_action()
    assert out_state(tmp_path)["peek_cursor"] == cursor


def test_destructive_run_carries_cursor_and_registry(broker, tmp_path, monkeypatch):
    broker.add_queue("q").send(b"x")
    cursor = {"entity_path": "q", "last_sequence_number": 42}
    registry = [{"path_sha1": "a" * 40, "column": "body_a"}]
    state = {"version": 1, "peek_cursor": cursor, "flatten_columns": registry}
    component(tmp_path, monkeypatch, PARAMS, state=state).execute_action()
    saved = out_state(tmp_path)
    assert saved == {"version": 1, "pending_commit": [], "peek_cursor": cursor, "flatten_columns": registry}


def test_flatten_writes_input_columns_and_saves_new_keys(broker, tmp_path, monkeypatch):
    broker.add_queue("q").send(b'{"a": 1}')
    params = {**PARAMS, "body": {"body_format": "json_flatten"}}
    component(tmp_path / "a", monkeypatch, params).execute_action()
    header, row = (tmp_path / "a/out/tables/q.csv").read_text().splitlines()
    assert header.endswith(",extracted_at_utc,body_unmapped") and "body_a" not in header.split(",")
    assert row.endswith(',"{""body_a"":""1""}"')
    first = out_state(tmp_path / "a")
    assert [entry["column"] for entry in first["flatten_columns"]] == ["body_a"]

    broker.entity("q").send(b'{"a": 2}')
    component(tmp_path / "b", monkeypatch, params, state=first).execute_action()
    header, row = (tmp_path / "b/out/tables/q.csv").read_text().splitlines()
    assert header.endswith(",body_a,body_unmapped") and row.endswith(",2,")


def test_c3_warns_at_most_once(broker, tmp_path, monkeypatch, caplog):
    broker.add_queue("q").send(b"x")
    with caplog.at_level(logging.WARNING):
        component(tmp_path, monkeypatch, with_source(settlement_mode="receive_and_delete")).execute_action()
    assert any(m.startswith("receive_and_delete deletes messages on delivery") for m in warnings_logged(caplog))


def test_prefetch_above_one_warns(broker, tmp_path, monkeypatch, caplog):
    broker.add_queue("q").send(b"x")
    params = {**PARAMS, "advanced_options": True, "advanced": {"prefetch_count": 5}}
    with caplog.at_level(logging.WARNING):
        component(tmp_path, monkeypatch, params).execute_action()
    assert any("prefetch_count is 5" in m for m in warnings_logged(caplog))


def test_throttled_empty_receive_is_counted_retried_and_summarised(broker, tmp_path, monkeypatch, caplog):
    """Phase 8: a throttled, empty receive with messages left is retried on a fresh connection; the
    SDK's server-busy report is counted into the summary (with the tier hint), never printed."""
    q = broker.add_queue("q")
    for _ in range(3):
        q.send(b"x")
    broker.inject_empty_receive(on_call=1, throttled=True)
    monkeypatch.setattr("receiver.EMPTY_RECEIVE_BACKOFF_SECONDS", (0, 0, 0, 0, 0))
    with caplog.at_level(logging.INFO):
        component(tmp_path, monkeypatch, PARAMS).execute_action()
    messages = [r.getMessage() for r in caplog.records]
    summary = next(m for m in messages if m.startswith("mode=complete "))
    assert "received=3 written=3" in summary and "empty_receive_retries=1 throttled=1" in summary
    assert "stop=idle" in summary
    assert any(
        m.startswith(
            "Service Bus throttled 1 request(s) (ServerBusy; the SDK retried them) and returned no messages to 1"
        )
        for m in messages
    )
    assert not any("AMQP error occurred" in m for m in messages)  # the SDK's own line stays out of the log
    assert q.sequence_numbers() == []


def test_summary_counts_the_run_warnings(broker, tmp_path, monkeypatch, caplog):
    broker.add_queue("q").send(b"x")
    with caplog.at_level(logging.INFO):
        component(tmp_path, monkeypatch, with_source(settlement_mode="receive_and_delete")).execute_action()
    messages = [r.getMessage() for r in caplog.records]
    assert any(m.startswith("mode=receive_and_delete received=1 written=1") for m in messages)
    assert "1 warning(s) were raised during this run." in messages


# --- sync actions ----------------------------------------------------------------------------------------


def sync_result(capsys):
    return json.loads(capsys.readouterr().out.strip().splitlines()[-1])


def sync_failure(capsys, comp) -> str:
    with pytest.raises(SystemExit) as excinfo:
        comp.execute_action()
    assert excinfo.value.code == 1
    return capsys.readouterr().err


@pytest.mark.parametrize("source", [{}, {"entity_type": "queue"}, PARAMS["source"]], ids=["empty", "partial", "full"])
def test_test_connection_reads_only_the_auth_block(broker, tmp_path, monkeypatch, capsys, source):
    """Test Connection is a root button only (spec §5.4): a row's source, complete or not, changes
    nothing -- the management probe runs and no entity is opened (Preview Messages does that)."""
    broker.add_queue("q")
    params = {"#connection_string": SAS, "source": source}
    component(tmp_path, monkeypatch, params, action="testConnection").execute_action()
    assert sync_result(capsys)["message"] == "Connected to the Service Bus namespace."
    assert broker.admin_calls == [("list_queues", "")] and broker.receivers == []


def test_test_connection_root_probes_management(broker, tmp_path, monkeypatch, capsys):
    component(tmp_path, monkeypatch, {"#connection_string": SAS}, action="testConnection").execute_action()
    assert sync_result(capsys)["message"] == "Connected to the Service Bus namespace."
    assert broker.admin_calls == [("list_queues", "")] and broker.receivers == []


def test_test_connection_root_listen_sas(broker, tmp_path, monkeypatch, capsys):
    broker.management_denied = True
    err = sync_failure(capsys, component(tmp_path, monkeypatch, {"#connection_string": SAS}, action="testConnection"))
    assert err.startswith("The connection string has no Manage rights") and "Preview Messages in a row" in err


def test_list_topics_and_subscriptions(broker, tmp_path, monkeypatch, capsys):
    broker.add_subscription("t", "s2")
    broker.add_subscription("t", "s1")
    broker.add_topic("a")
    component(tmp_path / "a", monkeypatch, {"#connection_string": SAS}, action="listTopics").execute_action()
    assert sync_result(capsys) == [{"value": "a", "label": "a"}, {"value": "t", "label": "t"}]
    params = {"#connection_string": SAS, "source": {"entity_type": "subscription", "topic_name": "t"}}
    component(tmp_path / "b", monkeypatch, params, action="listSubscriptions").execute_action()
    assert [item["value"] for item in sync_result(capsys)] == ["s1", "s2"]


def test_list_subscriptions_needs_a_topic(broker, tmp_path, monkeypatch, capsys):
    params = {"#connection_string": SAS, "source": {"entity_type": "subscription"}}
    err = sync_failure(capsys, component(tmp_path, monkeypatch, params, action="listSubscriptions"))
    assert err == "Select a topic first."


def test_list_queues_listen_sas_is_empty(broker, tmp_path, monkeypatch, capsys):
    broker.add_queue("a")
    broker.management_denied = True
    component(tmp_path, monkeypatch, {"#connection_string": SAS}, action="listQueues").execute_action()
    assert sync_result(capsys) == []


def test_preview_messages_peeks_ten_as_a_table(broker, tmp_path, monkeypatch, capsys):
    q = broker.add_queue("q")
    for i in range(12):
        q.send(f"body {i}".encode(), subject="s")
    component(tmp_path, monkeypatch, PARAMS, action="previewMessages").execute_action()
    result = sync_result(capsys)
    lines = result["message"].splitlines()
    assert result["type"] == "table" and lines[0].startswith("| Sequence Number |") and len(lines) == 12
    assert lines[2].startswith("| 1 |") and lines[2].endswith("| ACTIVE | body 0 |")
    (receiver,) = broker.receivers
    assert receiver.operations == [("peek_messages", {"max_message_count": 10, "sequence_number": 0})]
    assert receiver.kwargs == {
        "receive_mode": ServiceBusReceiveMode.PEEK_LOCK,
        "prefetch_count": 1,
        "keep_alive": 0,
        "client_identifier": "kbc-local-root",
    }
    assert receiver.closed
    assert all(q.delivery_count(seq) == 0 for seq in q.sequence_numbers())


@pytest.mark.parametrize("sessions", [False, True])
def test_preview_messages_empty_entity(broker, tmp_path, monkeypatch, capsys, sessions):
    broker.add_queue("q", sessions=sessions)
    component(tmp_path, monkeypatch, with_source(session_enabled=sessions), action="previewMessages").execute_action()
    assert sync_result(capsys) == {
        "message": "The entity has no messages to preview.",
        "type": "info",
        "status": "success",
    }
    # Regression (Phase 8, live): the SDK's default retries turned a session accept that finds no
    # session into ~34 s -- past the platform's 30-second limit -- so the session peek never retries.
    (client,) = broker.clients
    assert client.kwargs.get("retry_total") == (0 if sessions else None)


def test_preview_messages_session_entity_releases_the_session(broker, tmp_path, monkeypatch, capsys):
    q = broker.add_queue("q", sessions=True)
    q.send(b"x", session_id="a")
    component(tmp_path, monkeypatch, with_source(session_enabled=True), action="previewMessages").execute_action()
    assert sync_result(capsys)["type"] == "table"
    kwargs = broker.receivers[0].kwargs
    assert kwargs["session_id"] is NEXT_AVAILABLE_SESSION and kwargs["max_wait_time"] == 5
    assert broker.receivers[0].closed and q.state_of(1) == "ACTIVE" and q.delivery_count(1) == 0


def test_preview_messages_sp_credentials_rejected(broker, tmp_path, monkeypatch, capsys):
    broker.add_queue("q")
    broker.credential_failure = "AADSTS700016: Application with identifier 'c' was not found in the directory."
    params = {
        "auth_type": "service_principal",
        "tenant_id": "t",
        "client_id": "c",
        "#client_secret": "sp-s3cr3t",
        "fully_qualified_namespace": "ns.servicebus.windows.net",
        "source": PARAMS["source"],
    }
    err = sync_failure(capsys, component(tmp_path, monkeypatch, params, action="previewMessages"))
    assert err == (
        "The service principal credentials were rejected (check tenant ID, client ID and client secret). (details: "
        "Handler failed: Authentication failed: AADSTS700016: Application with identifier 'c' was not found in the "
        "directory..)"
    )


def test_preview_messages_maps_sdk_errors(broker, tmp_path, monkeypatch, capsys):
    err = sync_failure(capsys, component(tmp_path, monkeypatch, PARAMS, action="previewMessages"))
    assert "'q'" in err and "c2VjcmV0" not in err  # a missing queue via SAS reads as unauthorized


@pytest.mark.parametrize(
    ("action", "params"),
    [
        ("testConnection", {"#connection_string": SAS}),
        ("listQueues", {"#connection_string": SAS}),
        ("listTopics", {"#connection_string": SAS}),
        ("listSubscriptions", {"#connection_string": SAS, "source": {"topic_name": "t"}}),
        ("previewMessages", PARAMS),
    ],
)
def test_sync_action_gives_up_before_the_platform_limit(broker, tmp_path, monkeypatch, capsys, action, params):
    """Regression (Phase 8, UI "Internal Server Error"): the platform stops a sync action after 30 s
    and answers a generic HTTP 500. A namespace that does not answer -- the SDK retrying a throttled
    one -- must end as the action's own user error within the deadline instead."""
    import component as component_module

    monkeypatch.setattr(component_module, "SYNC_ACTION_DEADLINE_SECONDS", 0.2)
    broker.add_queue("q")
    broker.add_subscription("t", "s")
    release = threading.Event()
    broker.unresponsive = release
    started = time.monotonic()
    try:
        err = sync_failure(capsys, component(tmp_path, monkeypatch, params, action=action))
    finally:
        elapsed = time.monotonic() - started
        release.set()
        for worker in [t for t in threading.enumerate() if t.name.startswith("sync-action-")]:
            worker.join(5)
    assert err.startswith("Azure Service Bus did not respond within 0.2 seconds")
    assert elapsed < 5  # well before the fake's 10-second cap: the action gave up, the call did not return


# --- sync actions validate only auth + source (spec §5.4); auth is parsed lazily ---------------------------

UNRELATED_INVALID = {
    "table_name": {"destination": {"table_name": "_not a table name_"}},
    "batch_size": {"advanced_options": True, "advanced": {"batch_size": 0}},
}


@pytest.mark.parametrize("unrelated", UNRELATED_INVALID.values(), ids=UNRELATED_INVALID.keys())
@pytest.mark.parametrize("action", ["testConnection", "previewMessages"])
def test_entity_sync_actions_ignore_unrelated_invalid_fields(broker, tmp_path, monkeypatch, capsys, action, unrelated):
    broker.add_queue("q").send(b"x")
    component(tmp_path, monkeypatch, {**PARAMS, **unrelated}, action=action).execute_action()
    assert sync_result(capsys)["status"] == "success"


@pytest.mark.parametrize("unrelated", UNRELATED_INVALID.values(), ids=UNRELATED_INVALID.keys())
def test_run_still_validates_the_whole_row(broker, tmp_path, monkeypatch, unrelated):
    broker.add_queue("q").send(b"x")
    with pytest.raises(UserException, match="Validation Error"):
        component(tmp_path, monkeypatch, {**PARAMS, **unrelated}).execute_action()


def test_preview_messages_still_validates_the_source(broker, tmp_path, monkeypatch, capsys):
    params = {"#connection_string": SAS, "source": {"entity_type": "queue"}}
    err = sync_failure(capsys, component(tmp_path, monkeypatch, params, action="previewMessages"))
    assert err == (
        "Validation Error: configuration: Value error, `source.queue_name` is required when `entity_type` is `queue`."
    )


def test_preview_normalises_the_source_like_a_run(broker, tmp_path, monkeypatch, capsys):
    q = broker.add_queue("q", sessions=True)
    q.dead_letter_existing(q.send(b"x", session_id="a"), "reason", "description")
    params = with_source(sub_queue="dead_letter", session_enabled=True)  # a sub-queue has no sessions
    component(tmp_path, monkeypatch, params, action="previewMessages").execute_action()
    assert sync_result(capsys)["type"] == "table"
    assert "session_id" not in broker.receivers[0].kwargs


@pytest.mark.parametrize("action", ["listQueues", "listTopics", "testConnection"])
def test_missing_credentials_fail_through_the_sync_action(broker, tmp_path, monkeypatch, capsys, action):
    comp = component(tmp_path, monkeypatch, {}, action=action)  # auth is no longer parsed in __init__
    err = sync_failure(capsys, comp)  # the sync-action wrapper writes the message to stderr and exits 1
    assert (
        err
        == "Validation Error: configuration: Value error, `#connection_string` is required for connection_string auth."
    )


def test_run_without_credentials_is_a_validation_error(broker, tmp_path, monkeypatch):
    comp = component(tmp_path, monkeypatch, {"source": PARAMS["source"]})
    with pytest.raises(UserException, match="`#connection_string` is required"):
        comp.execute_action()


def test_building_the_connector_installs_log_redaction(broker, tmp_path, monkeypatch):
    from client import RedactingFilter

    root = logging.getLogger()
    comp = component(tmp_path, monkeypatch, PARAMS)
    assert not any(isinstance(f, RedactingFilter) for h in root.handlers for f in h.filters)
    assert comp._connector.secrets == (SAS,)
    assert root.handlers and all(any(isinstance(f, RedactingFilter) for f in h.filters) for h in root.handlers)


# --- the __main__ guard ----------------------------------------------------------------------------------


def run_main(tmp_path, monkeypatch, parameters) -> str | int | None:
    monkeypatch.setenv("KBC_DATADIR", str(datadir(tmp_path, parameters)))
    with pytest.raises(SystemExit) as excinfo:
        runpy.run_path(str(SRC / "component.py"), run_name="__main__")
    return excinfo.value.code


def test_main_user_error_exits_1_with_a_logged_message(broker, tmp_path, monkeypatch, caplog):
    broker.add_queue("q", sessions=True).send(b"x", session_id="A")  # the row does not enable Sessions
    with caplog.at_level(logging.ERROR):
        assert run_main(tmp_path, monkeypatch, PARAMS) == 1
    (record,) = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert "enable Sessions" in record.getMessage()


def test_main_unexpected_error_exits_2(broker, tmp_path, monkeypatch, caplog):
    from entity import EntityRef

    def boom(self):
        raise RuntimeError("bug")

    broker.add_queue("q").send(b"x")
    monkeypatch.setattr(EntityRef, "default_table_name", boom)
    with caplog.at_level(logging.ERROR):
        assert run_main(tmp_path, monkeypatch, PARAMS) == 2
    assert any(r.getMessage() == "Component failed with an unexpected error" for r in caplog.records)


def test_main_redacts_the_user_error(broker, tmp_path, monkeypatch, caplog):
    from entity import EntityRef

    def leak(self):
        raise UserException(f"failed with {SAS}")

    broker.add_queue("q").send(b"x")
    monkeypatch.setattr(EntityRef, "default_table_name", leak)
    with caplog.at_level(logging.ERROR):
        assert run_main(tmp_path, monkeypatch, PARAMS) == 1
    (message,) = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert message.startswith("failed with Endpoint=") and "c2VjcmV0" not in message


# --- one wall-clock source (Phase 5: the functional suite aligns it with the fake broker's clock) ----------


def test_t0_and_extracted_at_come_from_utc_now(broker, tmp_path, monkeypatch):
    import component as component_mod

    frozen = broker.clock.now() + timedelta(seconds=5)
    monkeypatch.setattr(component_mod, "utc_now", lambda: frozen)
    q = broker.add_queue("q")
    q.send(b"early")
    q.send(b"late", enqueued_at=frozen + timedelta(seconds=1))  # at or after T0: the peek stops before it
    component(tmp_path, monkeypatch, with_source(settlement_mode="peek")).execute_action()
    rows = list(csv.DictReader((tmp_path / "out/tables/q.csv").open()))
    assert [(r["body"], r["extracted_at_utc"]) for r in rows] == [("early", "2026-09-23 10:00:05.000000")]


def test_pending_set_timestamp_comes_from_utc_now(broker, tmp_path, monkeypatch):
    import component as component_mod

    frozen = broker.clock.now() + timedelta(seconds=5)
    monkeypatch.setattr(component_mod, "utc_now", lambda: frozen)
    broker.add_queue("q").send(b"x")
    component(tmp_path, monkeypatch, with_source(settlement_mode="defer_commit")).execute_action()
    assert out_state(tmp_path)["pending_commit"][0]["deferred_at_utc"] == "2026-09-23 10:00:05.000000"
