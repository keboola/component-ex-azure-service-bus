import csv
import json
from pathlib import Path

import pytest

from body import UNMAPPED_COLUMN, FlattenRegistry, path_hash
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
            name=kwargs["name"],
            full_path=str(self.out_dir / kwargs["name"]),
            schema=kwargs["schema"],
            primary_key=kwargs["primary_key"],
            incremental=kwargs["incremental"],
            write_always=kwargs["write_always"],
            has_header=kwargs["has_header"],
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
        table_name="orders",
        destination=destination or DestinationConfig(),
        body_format=body_format,
        registry=registry,
        create_definition=spy.create,
        write_manifest=spy.write,
    )
    return table, spy


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def header(path: Path) -> list[str]:
    with path.open(newline="") as f:
        return next(csv.reader(f))


def registry(existing=()):
    entries = [FlattenColumn(path_sha1=path_hash(p), column=c) for p, c in existing]
    return FlattenRegistry(entries, reserved=set(metadata_column_names()))


def test_text_mode_manifest_first_write_always_false_then_armed(tmp_path):
    table, spy = make(tmp_path, BodyFormat.TEXT)
    with table:
        assert spy.manifest()["write_always"] is False  # written before anything is settled
        first = spy.calls[0]
        assert first["has_header"] is True and first["incremental"] is True
        assert first["primary_key"] == ["sequence_number"]
        table.write_rows([OutputRow(meta(1), body="a"), OutputRow(meta(2), body="b")])
        assert [r["body"] for r in read_csv(tmp_path / "orders.csv")] == ["a", "b"]  # streamed + flushed
        table.arm_write_always()
        assert spy.manifest()["write_always"] is True
    assert table.rows_written == 2 and spy.manifest()["write_always"] is True


def test_never_armed_stays_false(tmp_path):
    table, spy = make(tmp_path, BodyFormat.TEXT)
    with pytest.raises(RuntimeError), table:
        table.write_rows([OutputRow(meta(1), body="a")])
        raise RuntimeError("boom")
    assert spy.manifest()["write_always"] is False
    assert read_csv(tmp_path / "orders.csv")[0]["body"] == "a"


def test_flatten_columns_are_the_input_registry_only(tmp_path):
    reg = registry([(("x",), "body_x"), (("z",), "body_z")])
    table, spy = make(tmp_path, BodyFormat.JSON_FLATTEN, registry=reg)
    with table:
        reg.register(("x",))
        reg.register(("y",))  # discovered in this run
        table.write_rows([OutputRow(meta(1), fields={("x",): "1", ("y",): "2"})])
    cols = header(tmp_path / "orders.csv")
    assert cols[-3:] == ["body_x", "body_z", UNMAPPED_COLUMN] and "body_y" not in cols and "body" not in cols
    row = read_csv(tmp_path / "orders.csv")[0]
    assert row["body_x"] == "1" and row["body_z"] == "" and json.loads(row[UNMAPPED_COLUMN]) == {"body_y": "2"}
    assert spy.manifest()["columns"][-3:] == ["body_x", "body_z", UNMAPPED_COLUMN]
    assert [c.column for c in reg.to_state()] == ["body_x", "body_z", "body_y"]  # saved for the next run


def test_flatten_first_run_everything_unmapped(tmp_path):
    reg = registry()
    table, _ = make(tmp_path, BodyFormat.JSON_FLATTEN, registry=reg)
    with table:
        reg.register(("a.b",))
        reg.register(("a", "b"))
        table.write_rows([OutputRow(meta(1), fields={("a.b",): "1", ("a", "b"): "2"})])
    assert header(tmp_path / "orders.csv")[-1] == UNMAPPED_COLUMN
    assert json.loads(read_csv(tmp_path / "orders.csv")[0][UNMAPPED_COLUMN]) == {"body_a_b": "1", "body_a_b_2": "2"}


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
    # data_types is Optional per the stub; schema_for always sets a BaseType, never None.
    schema = schema_for(["sequence_number", "body"])
    assert schema["sequence_number"].data_types["base"].dtype == "INTEGER"  # ty: ignore[not-subscriptable]
    assert schema["body"].data_types["base"].dtype == "STRING"  # ty: ignore[not-subscriptable]
