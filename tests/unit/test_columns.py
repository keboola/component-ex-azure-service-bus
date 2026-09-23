import json
from datetime import UTC, datetime, timedelta

from azure.servicebus import ServiceBusMessageState

from columns import (
    BODY_COLUMN,
    METADATA_COLUMNS,
    format_timestamp,
    message_metadata,
    metadata_column_names,
    render_preview,
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
