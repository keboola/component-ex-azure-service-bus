"""Functional sync-action cases 01-14 (spec §8, §5.4): one success and one failure path per action.

Every case runs the real component (``runpy`` as ``__main__``) against the FakeBroker; the configs
are in ``tests/setup/configs.json``. A sync action prints its JSON result on stdout (exit 0) or its
error on stderr (exit 1).
"""

from tests.functional.conftest import DUMMY_CLIENT_SECRET, DUMMY_SAS_KEY, run_case, sync_result


def _values(payload: object) -> list[str]:
    assert isinstance(payload, list)
    return [item["value"] for item in payload]


def test_01_testConnection_manage_sas_ignores_source(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("orders")
    seqs = [q.send(b"a"), q.send(b"b")]
    result = run_case("01_testConnection_manage_sas_ignores_source", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    payload = sync_result(result)
    assert payload["status"] == "success" and payload["message"] == "Connected to the Service Bus namespace."
    assert fake_broker.admin_calls == [("list_queues", "")] and fake_broker.receivers == []
    assert all(q.state_of(s) == "ACTIVE" and q.delivery_count(s) == 0 for s in seqs)


def test_02_testConnection_bad_conn_string(fake_broker, tmp_path, monkeypatch, capsys):
    result = run_case("02_testConnection_bad_conn_string", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "connection string" in result.stderr
    assert "SharedAccessKey=" not in result.stdout + result.stderr
    assert DUMMY_SAS_KEY not in result.stderr


def test_03_previewMessages_auth_or_missing(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.auth_failure = True
    result = run_case("03_previewMessages_auth_or_missing", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "IP firewall" in result.stderr and "orders" in result.stderr
    assert DUMMY_SAS_KEY not in result.stderr


def test_04_previewMessages_session_empty(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("sess", sessions=True)
    result = run_case("04_previewMessages_session_empty", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert sync_result(result)["message"] == "The entity has no messages to preview."


def test_05_testConnection_root_sp(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("a")
    result = run_case("05_testConnection_root_sp", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert "Connected to the Service Bus namespace" in sync_result(result)["message"]
    assert fake_broker.credentials and fake_broker.receivers == []  # the SP path, management plane only


def test_06_testConnection_root_listen_sas(fake_broker, tmp_path, monkeypatch, capsys):
    """A Listen-only key with no entity to probe is not a failure: extraction can still work (CFTL-784)."""
    fake_broker.management_denied = True
    result = run_case("06_testConnection_root_listen_sas", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    payload = sync_result(result)
    assert payload["type"] == "info"
    assert "no Manage rights" in payload["message"] and "Extraction can still work" in payload["message"]
    assert "Preview Messages" in payload["message"] and fake_broker.receivers == []


def test_07_listQueues_sp(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("b")
    fake_broker.add_queue("a")
    result = run_case("07_listQueues_sp", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert _values(sync_result(result)) == ["a", "b"]


def test_08_listQueues_listen_sas_denied(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("a")
    fake_broker.management_denied = True
    result = run_case("08_listQueues_listen_sas_denied", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1 and result.stdout == ""
    # the fake raises Azure's real 401 text; the toast keeps its meaning without the tracking noise (P4-15)
    assert result.stderr.strip() == (
        "Listing queues needs a connection string with Manage rights, or a service principal. "
        "With this connection string, type the queue name instead. "
        "(details: Unauthorized: Manage,EntityRead claims required for this operation)"
    )
    assert DUMMY_SAS_KEY not in result.stderr


def test_08_listQueues_without_credentials(fake_broker, tmp_path, monkeypatch, capsys):
    """Variant of case 08: the auth block is parsed inside the action (not in ``__init__``), so the
    sync-action wrapper reports the validation error on stderr -- the channel the UI reads."""
    overrides = {"#connection_string": ""}
    result = run_case("08_listQueues_listen_sas_denied", tmp_path, monkeypatch, capsys, overrides=overrides)
    assert result.exit_code == 1
    assert result.stderr == (
        "Validation Error: configuration: Value error, `#connection_string` is required for connection_string auth."
    )


def test_09_listTopics_sp_auth_failure(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.management_denied = True
    result = run_case("09_listTopics_sp_auth_failure", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "Data Receiver" in result.stderr


def test_09_listTopics_sp_credentials_rejected(fake_broker, tmp_path, monkeypatch, capsys):
    """Variant of case 09: Entra ID rejects the secret -- not a missing role (spec §6.11)."""
    fake_broker.credential_failure = "AADSTS7000222: The provided client secret keys for app 'c' are expired."
    result = run_case("09_listTopics_sp_auth_failure", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert result.stderr.startswith("The service principal credentials were rejected")
    assert "AADSTS7000222" in result.stderr and "Data Receiver" not in result.stderr
    assert DUMMY_CLIENT_SECRET not in result.stderr


def test_10_listTopics_sp(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_subscription("t2", "s")
    fake_broker.add_subscription("t1", "s")
    result = run_case("10_listTopics_sp", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert _values(sync_result(result)) == ["t1", "t2"]


def test_11_listSubscriptions_sp(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_subscription("t", "y")
    fake_broker.add_subscription("t", "x")
    result = run_case("11_listSubscriptions_sp", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert _values(sync_result(result)) == ["x", "y"]


def test_12_listSubscriptions_missing_topic(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_subscription("t", "x")
    result = run_case("12_listSubscriptions_missing_topic", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "'nope'" in result.stderr


def test_13_previewMessages(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    seqs = [q.send(f"body {i}".encode(), message_id=f"m-{i}") for i in range(3)]
    result = run_case("13_previewMessages", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    payload = sync_result(result)
    assert payload["type"] == "table"
    header, separator, *data = payload["message"].splitlines()
    assert header.startswith("| Sequence Number |") and separator.startswith("|---|")
    assert [line.split(" | ")[0] for line in data] == ["| 1", "| 2", "| 3"]
    assert all(q.state_of(s) == "ACTIVE" and q.delivery_count(s) == 0 for s in seqs)


def test_14_previewMessages_missing_entity(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_topic("t")
    result = run_case("14_previewMessages_missing_entity", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "was not found" in result.stderr and "t/Subscriptions/missing" in result.stderr


def test_70_testConnection_listen_only_queue_key_probes_the_queue(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    seq = q.send(b"a")
    fake_broker.management_denied = True
    result = run_case("70_testConnection_listen_only_queue_row", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    payload = sync_result(result)
    assert payload["status"] == "success" and "'q' can be read" in payload["message"]
    assert [r.entity_path for r in fake_broker.receivers] == ["q"]
    assert q.state_of(seq) == "ACTIVE" and q.delivery_count(seq) == 0  # peeked, never locked


def test_71_testConnection_topic_scoped_key_probes_the_subscription(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_subscription("t", "s")
    fake_broker.management_denied = True
    result = run_case("71_testConnection_listen_only_topic_scoped_row", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    payload = sync_result(result)
    assert payload["status"] == "success" and "'t/Subscriptions/s' can be read" in payload["message"]
    assert fake_broker.clients[-1].entity_name == "t"  # the key is scoped to the topic


def test_72_testConnection_topic_scoped_key_without_subscription(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_subscription("t", "s")
    fake_broker.management_denied = True
    result = run_case("72_testConnection_topic_scoped_without_subscription", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert sync_result(result)["type"] == "info" and fake_broker.receivers == []


def test_73_testConnection_rejected_key_still_fails(fake_broker, tmp_path, monkeypatch, capsys):
    """Azure answers a bad key with 401 on the management plane too, so the denial alone proves
    nothing: the data-plane probe must still fail with the authentication message."""
    fake_broker.management_denied = True
    fake_broker.auth_failure = True
    result = run_case("73_testConnection_rejected_key_row", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "IP firewall" in result.stderr and "'q'" in result.stderr
    assert DUMMY_SAS_KEY not in result.stdout + result.stderr


def test_74_testConnection_entity_path_mismatch(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("q")
    fake_broker.management_denied = True
    result = run_case("74_testConnection_entity_path_mismatch_row", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "EntityPath" in result.stderr and "'q'" in result.stderr


def test_75_testConnection_missing_entity_fails(fake_broker, tmp_path, monkeypatch, capsys):
    """Service Bus reports a missing entity as unauthorized to a SAS key; the probe fails like Preview Messages."""
    fake_broker.management_denied = True
    result = run_case("70_testConnection_listen_only_queue_row", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1 and "Authentication to Azure Service Bus failed for 'q'" in result.stderr
