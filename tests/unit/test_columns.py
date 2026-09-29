import json
import re
from datetime import UTC, datetime, timedelta, timezone

from azure.servicebus import ServiceBusMessageState

from columns import (
    BODY_COLUMN,
    METADATA_COLUMNS,
    format_timestamp,
    message_metadata,
    metadata_column_names,
    render_preview,
    utc_now,
)
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


def test_format_timestamp_is_strict_iso_8601_utc():
    # P4-17: every TIMESTAMP column is `YYYY-MM-DDTHH:MM:SS.ffffffZ` in UTC
    assert format_timestamp(datetime(2026, 9, 24, 14, 3, 15, 123456, tzinfo=UTC)) == "2026-09-24T14:03:15.123456Z"
    assert format_timestamp(datetime(2026, 9, 23, 12, 0, tzinfo=UTC)) == "2026-09-23T12:00:00.000000Z"
    assert format_timestamp(datetime(2026, 9, 23, 12, 0, tzinfo=UTC).replace(tzinfo=None)) == (
        "2026-09-23T12:00:00.000000Z"  # naive is UTC
    )
    plus_two = timezone(timedelta(hours=2))
    assert format_timestamp(datetime(2026, 9, 24, 1, 30, tzinfo=plus_two)) == "2026-09-23T23:30:00.000000Z"
    assert format_timestamp(datetime(999, 1, 2, 3, 4, 5, tzinfo=UTC)) == "0999-01-02T03:04:05.000000Z"


def test_format_timestamp_epoch_milliseconds_and_unset():
    assert format_timestamp(1790157600000) == "2026-09-23T10:00:00.000000Z"
    assert format_timestamp(1790157600123) == "2026-09-23T10:00:00.123000Z"
    # .NET's DateTimeOffset.MaxValue: exact integer arithmetic, no float rounding in the last millisecond
    assert format_timestamp(253402300799999) == "9999-12-31T23:59:59.999000Z"
    assert format_timestamp(None) == "" and format_timestamp(0) == ""


def test_every_timestamp_column_uses_the_iso_form():
    m = make_message(
        b"x",
        sequence_number=1,
        expires_at_utc=datetime(2026, 9, 23, 11, 0, tzinfo=UTC),
        scheduled_enqueue_time_utc=datetime(2026, 9, 23, 9, 0, tzinfo=UTC),
        creation_time=1790157600000,  # AMQP properties: epoch milliseconds
        absolute_expiry_time=1790161200000,
    )
    row = message_metadata(m, entity=Q, settlement_mode=SettlementMode.COMPLETE, extracted_at=NOW)
    timestamps = [name for name, base_type in METADATA_COLUMNS if base_type == "TIMESTAMP"]
    assert timestamps == [
        "enqueued_time_utc",
        "expires_at_utc",
        "scheduled_enqueue_time_utc",
        "amqp_creation_time_utc",
        "amqp_absolute_expiry_time_utc",
        "extracted_at_utc",
    ]
    iso = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z")
    for name in timestamps:
        assert iso.fullmatch(row[name]), (name, row[name])
    assert row["expires_at_utc"] == row["amqp_absolute_expiry_time_utc"] == "2026-09-23T11:00:00.000000Z"
    assert row["scheduled_enqueue_time_utc"] == "2026-09-23T09:00:00.000000Z"
    assert row["amqp_creation_time_utc"] == row["extracted_at_utc"] == "2026-09-23T10:00:00.000000Z"


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
    assert row["extracted_at_utc"] == "2026-09-23T10:00:00.000000Z"
    assert row["amqp_durable"] in ("true", "false", "")


def test_json_key_type_never_collides_with_metadata_body_type():
    # amendment 5: the `body_` prefix does not avoid every metadata collision — `body_type` is a metadata column
    from body import FlattenRegistry

    assert "body_type" in metadata_column_names()
    assert FlattenRegistry([], reserved=set(metadata_column_names())).register(("type",)) == "body_type_2"


def test_render_preview():
    text = render_preview([make_message(b"a|b\n" + b"y" * 500, sequence_number=7, subject="s")])
    lines = text.splitlines()
    assert lines[0].startswith("| Sequence Number |") and "| 7 |" in lines[2]
    assert "a\\|b" in lines[2] and "y" * 121 not in lines[2]


def test_render_preview_shows_the_same_timestamp_form_as_the_table():
    message = make_message(
        b"x", sequence_number=7, enqueued_time_utc=datetime(2026, 9, 24, 14, 3, 15, 123456, tzinfo=UTC)
    )
    assert "| 7 | 2026-09-24T14:03:15.123456Z |" in render_preview([message])


def test_render_preview_escapes_every_text_cell():
    message = make_message(b"l1\r\nl2|l3\rl4\nl5", sequence_number=3, message_id="id|1\n2", subject="s\r\nt|u")
    _header, _separator, row = render_preview([message]).split("\n")  # one row: no line break leaked
    assert "\r" not in row
    cells = row.split(" | ")
    assert cells[2:4] == ["id\\|1 2", "s t\\|u"]  # message id, subject
    assert cells[-1] == "l1 l2\\|l3 l4 l5 |"  # CRLF -> one space, a lone CR -> a space


def test_utc_now_is_the_aware_utc_wall_clock():
    before = datetime.now(UTC)
    value = utc_now()
    assert value.tzinfo is UTC and before <= value <= datetime.now(UTC)
