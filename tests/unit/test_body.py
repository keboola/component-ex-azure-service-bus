import base64
import json

import pytest

import body as body_mod
from body import (
    BodyDecodeError,
    BodyTooLargeError,
    FlattenRegistry,
    NotJsonError,
    charset_of,
    column_name_for,
    encode_body,
    flatten_value,
    path_hash,
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
    assert encode_body(make_message({"a": {"b": "c"}}, body_type="VALUE"), BodyFormat.JSON_FLATTEN).fields == {
        ("a", "b"): "c"
    }


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


def test_registry_state_is_hashed_and_bounded():
    reg = FlattenRegistry([], reserved=set())
    reg.register(("k" * 5000,))
    entry = reg.to_state()[0]
    assert entry.path_sha1 == path_hash(("k" * 5000,)) and len(entry.path_sha1) == 40
    assert len(entry.model_dump_json()) <= 133


def test_registry_restores_from_state_models():
    reg = FlattenRegistry([FlattenColumn(path_sha1=path_hash(("x",)), column="body_x")], reserved=set())
    assert reg.register(("x",)) == "body_x" and reg.columns == ["body_x"] and reg.input_columns == ["body_x"]


def test_unmapped_is_reserved():
    assert FlattenRegistry([], reserved=set()).register(("unmapped",)) == "body_unmapped_2"


def test_split_keys_unmapped_by_column_name():
    reg = FlattenRegistry([FlattenColumn(path_sha1=path_hash(("x",)), column="body_x")], reserved=set())
    fields = {("x",): "1", ("a.b",): "2", ("a", "b"): "3", (): "[1]"}
    for path in fields:
        reg.register(path)
    assert reg.new_columns == ["body_a_b", "body_a_b_2", "body_value"]
    values, unmapped = reg.split(fields)
    assert values == {"body_x": "1"}
    assert json.loads(unmapped) == {"body_a_b": "2", "body_a_b_2": "3", "body_value": "[1]"}
    assert reg.split({("x",): "9"}) == ({"body_x": "9"}, "")


def test_registry_cap_overflow_is_provisional(monkeypatch):
    monkeypatch.setattr(body_mod, "MAX_FLATTEN_COLUMNS", 2)
    reg = FlattenRegistry([], reserved=set())
    reg.register(("a",))
    reg.register(("b",))
    assert not reg.overflowed
    assert reg.register(("c",)) == "body_c" and reg.overflowed
    assert [e.column for e in reg.to_state()] == ["body_a", "body_b"]
    assert json.loads(reg.split({("c",): "3"})[1]) == {"body_c": "3"}


def test_registry_cap_provisional_name_reused_for_the_run(monkeypatch):
    monkeypatch.setattr(body_mod, "MAX_FLATTEN_COLUMNS", 1)
    reg = FlattenRegistry([], reserved=set())
    reg.register(("a",))
    first = reg.register(("c",))
    assert reg.register(("c",)) == first == "body_c"  # same path hash → same provisional name
    assert reg.register(("d",)) == "body_d" and reg.register(("d",)) == "body_d"
    assert [e.column for e in reg.to_state()] == ["body_a"]


def test_type_key_avoids_reserved_body_type():
    # the metadata column `body_type` shares the `body_` prefix (amendment 5)
    assert FlattenRegistry([], reserved={"body_type"}).register(("type",)) == "body_type_2"


def test_compact_json_bytes_keys():
    assert json.loads(body_mod.compact_json({b"k": b"v"})) == {"k": "v"}
