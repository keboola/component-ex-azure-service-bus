"""Functional cases 46-69 (spec §8): body formats, JSON flattening with ``body_unmapped`` promotion,
unreadable bodies, connection recycling, run bounds, branch runs and the ``write_always`` switch.

Every case runs the real component against the FakeBroker (see ``conftest.py``).
"""

import json
from datetime import timedelta

import pytest
from azure.servicebus import ServiceBusSubQueue
from azure.servicebus.exceptions import ServiceBusAuthenticationError

from tests.functional.conftest import csv_header, peek_stored, run_case, run_chain, run_twice, summary

FLATTEN_KEYS = ["body_order_id", "body_order_items", "body_a_b", "body_a_b_2"]


def _auth_error() -> ServiceBusAuthenticationError:
    return ServiceBusAuthenticationError(message="CBS token authentication failed: unauthorized.")


def _loop_receivers(broker) -> list:
    return [r for r in broker.receivers if any(op == "receive_messages" for op, _ in r.operations)]


# --- body formats (F2-F5) ------------------------------------------------------------------------------


def test_46_run_body_base64(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("q").send(b"\x00\x01")
    result = run_case("46_run_body_base64", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert [row["body"] for row in result.tables["q.csv"]] == ["AAE="]


def test_47_run_body_value_sequence(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    q.send({"k": "v"}, body_type="VALUE")
    q.send([[1, "a"]], body_type="SEQUENCE")
    result = run_case("47_run_body_value_sequence", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert [(row["body"], row["body_type"]) for row in result.tables["q.csv"]] == [
        ('{"k":"v"}', "VALUE"),
        ('[[1,"a"]]', "SEQUENCE"),
    ]


def test_48_run_body_charset_multisection(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("q").send([b"p\xe9", b"x"], content_type="text/plain; charset=latin-1")
    result = run_case("48_run_body_charset_multisection", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert [row["body"] for row in result.tables["q.csv"]] == ["péx"]


# --- JSON flattening (F1, F6; spec §6.10 column rule) ----------------------------------------------------


def test_49_run_flatten_json(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    body = b'{"order":{"id":1,"items":[1,2]},"a.b":1,"a":{"b":2}}'
    first, second = run_twice("49_run_flatten_json", tmp_path, monkeypatch, capsys, before_each=lambda _: q.send(body))
    assert first.exit_code == 0 and second.exit_code == 0
    header = csv_header(first)
    assert "body" not in header and not set(FLATTEN_KEYS) & set(header) and header[-1] == "body_unmapped"
    (row,) = first.tables["q.csv"]
    assert row["body_unmapped"] == '{"body_order_id":"1","body_order_items":"[1,2]","body_a_b":"1","body_a_b_2":"2"}'
    assert first.state is not None
    registry = first.state["flatten_columns"]
    assert [entry["column"] for entry in registry] == FLATTEN_KEYS
    assert all(len(entry["path_sha1"]) == 40 for entry in registry)

    assert csv_header(second)[-5:] == [*FLATTEN_KEYS, "body_unmapped"]
    (row,) = second.tables["q.csv"]
    assert [row[key] for key in FLATTEN_KEYS] == ["1", "[1,2]", "1", "2"] and row["body_unmapped"] == ""


def test_50_run_flatten_new_keys_second_run(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    bodies = [b'{"x":1}', b'{"y":2}', b'{"y":3}']
    runs = run_chain(
        ["50_run_flatten_new_keys_second_run"] * 3,
        tmp_path,
        monkeypatch,
        capsys,
        before_each=lambda index: q.send(bodies[index]),
    )
    assert [run.exit_code for run in runs] == [0, 0, 0]
    second, third = runs[1], runs[2]
    assert "body_x" in csv_header(second) and "body_y" not in csv_header(second)
    (row,) = second.tables["q.csv"]  # the first run's deferral was committed, not received again
    assert row["body_x"] == "" and row["body_unmapped"] == '{"body_y":"2"}'
    assert {"body_x", "body_y"} <= set(csv_header(third))
    (row,) = third.tables["q.csv"]
    assert row["body_y"] == "3" and row["body_unmapped"] == ""


def test_51_run_flatten_not_json(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    seq = q.send(b"plain")
    result = run_case("51_run_flatten_not_json", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert result.tables["q.csv"] == []
    assert q.sequence_numbers() == [] and q.dead_letter.sequence_numbers() == [seq]
    (dead,) = peek_stored("q", ServiceBusSubQueue.DEAD_LETTER)
    assert dead.dead_letter_reason == "NotJson"


# --- unreadable bodies (C6 / F7, spec §6.6) --------------------------------------------------------------


def test_52_run_unreadable_body_two_connections(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    bad = q.send(b"unreadable")
    good = q.send(b"good")
    fake_broker.inject_body_error(bad, times=2)
    result = run_case("52_run_unreadable_body_two_connections", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert [row["sequence_number"] for row in result.tables["q.csv"]] == [str(good)]  # no row for the bad one
    assert q.dead_letter.sequence_numbers() == [bad]
    (dead,) = peek_stored("q", ServiceBusSubQueue.DEAD_LETTER)
    assert dead.dead_letter_reason == "UnreadableBody"
    assert len(fake_broker.clients) >= 2 and len(_loop_receivers(fake_broker)) >= 2  # retried on a fresh connection
    assert summary(result)["unreadable_recycles"] == "1"


def test_53_run_unreadable_on_dlq_degrades(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    seq = q.send(b"unreadable")
    q.dead_letter_existing(seq, "R", "D")
    fake_broker.inject_body_error(seq, times=2)
    result = run_case("53_run_unreadable_on_dlq_degrades", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert result.tables["q_dead_letter.csv"] == []
    assert q.dead_letter.sequence_numbers() == [seq]  # left on the sub-queue
    assert "a message on a dead-letter sub-queue cannot be dead-lettered" in result.stderr


def test_54_run_unreadable_share_abort(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    for _ in range(12):
        fake_broker.inject_body_error(q.send(b"unreadable"), times=2)
    result = run_case("54_run_unreadable_share_abort", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "had unreadable bodies (more than 10% and at least 10)" in result.stderr
    assert result.tables["q.csv"] == [] and len(q.sequence_numbers()) == 12


# --- connection recycling (J2) ----------------------------------------------------------------------------


def test_55_run_receive_failure_recycle(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    for i in range(3):
        q.send(f"m{i}".encode())
    fake_broker.inject_receive_error(TypeError("injected link failure"), on_call=2)
    result = run_case("55_run_receive_failure_recycle", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert sorted(row["sequence_number"] for row in result.tables["q.csv"]) == ["1", "2", "3"]
    assert summary(result)["recoveries"] == "1"
    assert q.sequence_numbers() == []


def test_56_run_recoveries_exhausted(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    q.send(b"x")
    fake_broker.inject_receive_error(TypeError("injected link failure"), on_call=1)
    fake_broker.inject_receive_error(TypeError("injected link failure"), on_call=2)
    result = run_case("56_run_recoveries_exhausted", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 2
    assert result.tables["q.csv"] == [] and q.state_of(1) == "ACTIVE" and q.delivery_count(1) == 0


# --- run bounds and edges (D1, D4, G2, G3, G5) -------------------------------------------------------------


def test_57_run_watermark_stop(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    later = fake_broker.clock.now() + timedelta(minutes=5)  # enqueued after the job's T0
    for i in range(2):
        q.send(f"before{i}".encode())
    for i in range(4):
        q.send(f"after{i}".encode(), enqueued_at=later)
    result = run_case("57_run_watermark_stop", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    # The watermark batch is still written; the stop drain abandons the buffered rest.
    assert [row["body"] for row in result.tables["q.csv"]] == ["before0", "before1", "after0", "after1"]
    assert q.sequence_numbers() == [5, 6]
    assert summary(result)["stop"] == "watermark"


def test_58_run_max_messages(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    for i in range(5):
        q.send(f"m{i}".encode())
    result = run_case("58_run_max_messages", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert len(result.tables["q.csv"]) == 2 and len(q.sequence_numbers()) == 3


def test_59_run_empty_entity(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("q")
    result = run_case("59_run_empty_entity", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    assert result.tables["q.csv"] == [] and csv_header(result)[0] == "sequence_number"
    manifest = result.manifests["q.csv"]
    assert manifest["has_header"] is True and manifest["write_always"] is False


def test_60_run_full_load_composite_pk(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("q").send(b"x")
    result = run_case("60_run_full_load_composite_pk", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 0
    manifest = result.manifests["q.csv"]
    assert manifest["incremental"] is False
    # The native-types manifest flags the key per column (so in column order, not configuration order).
    primary_key = [column["name"] for column in manifest["schema"] if column.get("primary_key")]
    assert sorted(primary_key) == sorted(["source_entity", "sequence_number"])


# --- branches (J11: no automatic guard -- the platform gives no dev-branch signal, spec §2.5) --------------


def test_61_run_branch_id_c1(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    q.send(b"x")
    result = run_case("61_run_branch_id_c1", tmp_path, monkeypatch, capsys, env={"KBC_BRANCHID": "1"})
    assert result.exit_code == 0
    assert len(result.tables["q.csv"]) == 1 and q.sequence_numbers() == []


def test_62_run_removed_override_key_ignored(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    q.send(b"x")
    result = run_case("62_run_removed_override_key_ignored", tmp_path, monkeypatch, capsys, env={"KBC_BRANCHID": "1"})
    assert result.exit_code == 0
    assert len(result.tables["q.csv"]) == 1 and q.sequence_numbers() == []


def test_63_run_dev_branch_peek(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    q.send(b"x")
    result = run_case("63_run_dev_branch_peek", tmp_path, monkeypatch, capsys, env={"KBC_BRANCHID": "1"})
    assert result.exit_code == 0
    assert len(result.tables["q.csv"]) == 1 and q.state_of(1) == "ACTIVE" and q.delivery_count(1) == 0


def test_64_run_missing_creds(fake_broker, tmp_path, monkeypatch, capsys):
    result = run_case("64_run_missing_creds", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "connection_string" in result.stderr


# --- write_always per mode (spec §6.9 table) --------------------------------------------------------------


@pytest.mark.parametrize(
    "mode, on_call, rows, armed",
    [
        ("complete", 3, 2, True),
        ("defer_commit", 3, 2, False),
        ("receive_and_delete", 3, 2, True),
        ("peek", 2, 1, False),
        ("complete", 1, 0, False),
        ("receive_and_delete", 1, 0, False),
    ],
    ids=["c1_call3", "c2_call3", "c3_call3", "c4_peek2", "c1_call1", "c3_call1"],
)
def test_65_run_failure_write_always_per_mode(fake_broker, tmp_path, monkeypatch, capsys, mode, on_call, rows, armed):
    q = fake_broker.add_queue("q")
    for i in range(3):
        q.send(f"m{i}".encode())
    if mode == "peek":
        monkeypatch.setattr("peek.PEEK_PAGE_SIZE", 1)
        fake_broker.inject_peek_error(_auth_error(), on_call=on_call)
    else:
        fake_broker.inject_receive_error(_auth_error(), on_call=on_call)
    overrides = {"source": {"settlement_mode": mode}}
    result = run_case("65_run_failure_write_always_per_mode", tmp_path, monkeypatch, capsys, overrides=overrides)
    assert result.exit_code != 0
    assert len(result.tables["q.csv"]) == rows
    assert result.manifests["q.csv"]["write_always"] is armed
    assert result.state is None


# --- flatten column rule across failed runs and discarded state (spec §6.10) ------------------------------


def test_66_run_flatten_failed_run_new_keys(fake_broker, tmp_path, monkeypatch, capsys):
    q = fake_broker.add_queue("q")
    first_seq: list[int] = []

    def seed(index: int) -> None:
        if index == 0:
            first_seq.append(q.send(b'{"a":1}'))
            fake_broker.inject_receive_error(_auth_error(), on_call=2)  # after the first batch: not recoverable
        else:
            q.send(b'{"a":2}')

    runs = run_chain(["66_run_flatten_failed_run_new_keys"] * 3, tmp_path, monkeypatch, capsys, before_each=seed)
    first, second, third = runs
    assert first.exit_code != 0 and first.state is None
    assert "body_a" not in csv_header(first)
    assert [row["body_unmapped"] for row in first.tables["q.csv"]] == ['{"body_a":"1"}']

    assert second.exit_code == 0 and "body_a" not in csv_header(second)
    unmapped = {row["sequence_number"]: row["body_unmapped"] for row in second.tables["q.csv"]}
    # The failed run's deferral is an orphan the second run recovers (spec §6.9 table), plus the new message.
    assert unmapped == {str(first_seq[0]): '{"body_a":"1"}', "2": '{"body_a":"2"}'}
    assert second.state is not None and [entry["column"] for entry in second.state["flatten_columns"]] == ["body_a"]

    assert third.exit_code == 0 and "body_a" in csv_header(third)
    (row,) = third.tables["q.csv"]
    assert row["body_a"] == "2" and row["body_unmapped"] == ""


def test_67_run_flatten_cross_row_state_discard(fake_broker, tmp_path, monkeypatch, capsys):
    fake_broker.add_queue("q").send(b'{"k":1}')
    first, second = run_chain(
        ["67_run_flatten_cross_row_state_discard"] * 2, tmp_path, monkeypatch, capsys, discard_state={0}
    )
    assert first.exit_code == 0 and second.exit_code == 0
    assert first.state is not None and [entry["column"] for entry in first.state["flatten_columns"]] == ["body_k"]
    assert "body_k" not in csv_header(first)
    assert csv_header(second) == csv_header(first)  # started from the discarded run's input state
    assert [row["body_unmapped"] for row in second.tables["q.csv"]] == ['{"body_k":"1"}']


# --- write first, fail after (spec §6.6, §6.10) ------------------------------------------------------------


@pytest.mark.parametrize("buffered", [0, 2], ids=["one_batch", "with_buffer"])
def test_68_run_unreadable_fail_writes_rest_first(fake_broker, tmp_path, monkeypatch, capsys, buffered):
    """``with_buffer`` (beyond the brief): two more messages wait behind the failing batch of 3; the
    C3 stop drain writes them before the error propagates (they are already deleted on the broker)."""
    q = fake_broker.add_queue("q")
    seqs = [q.send(f"m{i}".encode()) for i in range(3 + buffered)]
    fake_broker.inject_body_error(seqs[1], times=1)
    overrides = {"advanced_options": True, "advanced": {"batch_size": 3}} if buffered else None
    result = run_case("68_run_unreadable_fail_writes_rest_first", tmp_path, monkeypatch, capsys, overrides=overrides)
    assert result.exit_code == 1
    assert "unreadable_body policy is 'fail'" in result.stderr
    assert [row["body"] for row in result.tables["q.csv"]] == ["m0", "m2", *[f"m{i}" for i in range(3, 3 + buffered)]]
    assert result.manifests["q.csv"]["write_always"] is True
    assert q.sequence_numbers() == []


def test_69_run_flatten_cap_writes_rest_first(fake_broker, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("body.MAX_FLATTEN_COLUMNS", 1)
    q = fake_broker.add_queue("q")
    q.send(b'{"a":1}')
    q.send(b'{"b":2}')
    result = run_case("69_run_flatten_cap_writes_rest_first", tmp_path, monkeypatch, capsys)
    assert result.exit_code == 1
    assert "distinct JSON keys" in result.stderr
    assert [json.loads(row["body_unmapped"]) for row in result.tables["q.csv"]] == [{"body_a": "1"}, {"body_b": "2"}]
    assert result.manifests["q.csv"]["write_always"] is True
