"""Functional sync-action cases 01-16 (spec §8, §5.4): one success and one failure path per action.

Every case runs the real component (``runpy`` as ``__main__``) against the FakeBroker; the configs
are in ``tests/setup/configs.json``. A sync action prints its JSON result on stdout (exit 0) or its
error on stderr (exit 1).
"""

from tests.functional.conftest import DUMMY_SAS_KEY, run_case, sync_result


def _values(payload: object) -> list[str]:
    assert isinstance(payload, list)
    return [item["value"] for item in payload]


def test_01_testConnection_queue(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("orders")
    seqs = [q.send(b"a"), q.send(b"b")]
    result = run_case("01_testConnection_queue", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    payload = sync_result(result)
    assert payload["status"] == "success" and "orders" in payload["message"]
    assert all(q.state_of(s) == "ACTIVE" and q.delivery_count(s) == 0 for s in seqs)


def test_02_testConnection_bad_conn_string(fake_broker, tmp_path, monkeypatch, capsys):
    result = run_case("02_testConnection_bad_conn_string", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "connection string" in result.stderr
    assert "SharedAccessKey=" not in result.stdout + result.stderr
    assert DUMMY_SAS_KEY not in result.stderr


def test_03_testConnection_auth_or_missing(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.auth_failure = True
    result = run_case("03_testConnection_auth_or_missing", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "IP firewall" in result.stderr and "orders" in result.stderr
    assert DUMMY_SAS_KEY not in result.stderr


def test_04_testConnection_session_empty(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("sess", sessions=True)
    result = run_case("04_testConnection_session_empty", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert "No session" in sync_result(result)["message"]


def test_05_testConnection_root_sp(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("a")
    result = run_case("05_testConnection_root_sp", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert "Connected to the Service Bus namespace" in sync_result(result)["message"]
    assert fake_broker.credentials and fake_broker.receivers == []  # the SP path, management plane only


def test_06_testConnection_root_listen_sas(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.management_denied = True
    result = run_case("06_testConnection_root_listen_sas", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "only from a row" in result.stderr


def test_07_listQueues_sp(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("b")
    fake_broker.add_queue("a")
    result = run_case("07_listQueues_sp", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert _values(sync_result(result)) == ["a", "b"]


def test_08_listQueues_listen_sas_empty(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("a")
    fake_broker.management_denied = True
    result = run_case("08_listQueues_listen_sas_empty", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert sync_result(result) == []


def test_09_listTopics_sp_auth_failure(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.management_denied = True
    result = run_case("09_listTopics_sp_auth_failure", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "Data Receiver" in result.stderr


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


def test_15_entityInfo_sp(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_subscription("t", "s").add_rule("r1", "amount > 10")
    result = run_case("15_entityInfo_sp", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    message = sync_result(result)["message"]
    assert "Requires session" in message and "r1" in message and "amount > 10" in message


def test_16_entityInfo_listen_sas(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("q")
    fake_broker.management_denied = True
    result = run_case("16_entityInfo_listen_sas", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "Manage" in result.stderr
