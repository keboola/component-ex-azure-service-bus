from tests.functional.conftest import DUMMY_CLIENT_SECRET, DUMMY_SAS_KEY, run_case, sync_result

ROW_NAMESPACE = "rowns.servicebus.windows.net"


def test_75_run_with_only_a_row_key(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_subscription("t", "s")
    q.send(b"hello")
    result = run_case("75_run_row_connection_string_only", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert [row["body"] for row in result.tables["t_s.csv"]] == ["hello"]
    assert {c.fully_qualified_namespace for c in fake_broker.clients} == {ROW_NAMESPACE}
    assert fake_broker.clients[0].entity_name == "t"
    assert DUMMY_SAS_KEY not in result.log


def test_76_row_key_overrides_a_root_service_principal(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("q").send(b"x")
    result = run_case("76_run_row_connection_string_overrides_service_principal", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert len(result.tables["q.csv"]) == 1
    assert {c.fully_qualified_namespace for c in fake_broker.clients + fake_broker.admin_clients} == {ROW_NAMESPACE}
    assert not fake_broker.credentials
    assert DUMMY_CLIENT_SECRET not in result.log and DUMMY_SAS_KEY not in result.log


def test_77_preview_messages_uses_the_row_key(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("q").send(b"preview me", message_id="m-1")
    result = run_case("77_previewMessages_row_connection_string", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert sync_result(result)["type"] == "table"
    assert {c.fully_qualified_namespace for c in fake_broker.clients} == {ROW_NAMESPACE}


def test_78_list_action_uses_the_row_key(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("a")
    fake_broker.management_denied = True
    result = run_case("78_listQueues_row_connection_string_denied", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "type the queue name instead" in result.stderr
    assert {c.fully_qualified_namespace for c in fake_broker.admin_clients} == {ROW_NAMESPACE}
    assert not fake_broker.credentials
    assert DUMMY_SAS_KEY not in result.stderr


def test_79_run_without_any_connection_string(fake_broker, tmp_path, monkeypatch, capsys):
    result = run_case("79_run_without_any_connection_string", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "`#connection_string` is required for connection_string auth." in result.stderr
    assert fake_broker.clients == []


def test_row_key_is_ignored_when_empty(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("q").send(b"x")
    overrides = {"#row_connection_string": "   "}
    result = run_case("20_run_c1_queue", tmp_path, monkeypatch, capsys, overrides=overrides)
    assert result.exit_code == 0
    assert {c.fully_qualified_namespace for c in fake_broker.clients} == {"ns.servicebus.windows.net"}


def test_80_root_test_connection_without_credentials(fake_broker, tmp_path, monkeypatch, capsys):
    result = run_case("80_testConnection_root_without_credentials", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1 and result.stdout == ""
    assert result.stderr.strip() == (
        "No credentials are set on the configuration. "
        "If each row has its own row connection string, use Test Connection in that row."
    )
    assert fake_broker.clients == [] and fake_broker.admin_clients == []


def test_81_row_test_connection_peeks_with_a_listen_only_row_key(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("q").send(b"x")
    fake_broker.management_denied = True
    result = run_case("81_testConnection_row_key_listen_only", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert sync_result(result)["type"] == "success"
    assert {c.fully_qualified_namespace for c in fake_broker.clients + fake_broker.admin_clients} == {ROW_NAMESPACE}
    assert not fake_broker.credentials
    assert DUMMY_SAS_KEY not in result.stdout + result.stderr


def test_82_row_test_connection_entity_path_mismatch_names_the_row_key(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("q")
    fake_broker.management_denied = True
    result = run_case("82_testConnection_row_key_entity_path_mismatch", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert result.stderr.startswith(
        "The row connection string (#row_connection_string) is scoped to another entity (EntityPath) than 'q'."
    )
    assert DUMMY_SAS_KEY not in result.stderr
