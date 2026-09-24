"""Fixed column catalogue, message-to-row metadata mapping, value formats and preview rendering
(Task 8, spec §6.9, §4-E, §5.4).

``METADATA_COLUMNS`` is the extractor's fixed output schema (order and native types, §6.9); the
column set never varies by mode or config -- unused columns simply stay empty. ``message_metadata``
reads a broker message defensively (the AMQP header / properties objects themselves can be ``None``)
and never touches ``message.body``: bodies are decoded exactly once, in ``body.py``, and streamed
into the ``body`` / flattened ``body_*`` columns that follow this fixed set (§6.10).
``render_preview`` is the ``previewMessages`` sync action's markdown table (§5.4): it decodes each
body itself, for display only, and must never fail on a bad body.
"""

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from body import charset_of, compact_json
from configuration import SettlementMode
from entity import EntityRef

BODY_COLUMN = "body"

# (name, base_type) in spec §6.9 column order; base types beyond STRING are spelled out there
# (INTEGER / FLOAT / BOOLEAN / TIMESTAMP), every other column defaults to STRING.
METADATA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("sequence_number", "INTEGER"),
    ("message_id", "STRING"),
    ("enqueued_time_utc", "TIMESTAMP"),
    ("enqueued_sequence_number", "INTEGER"),
    ("session_id", "STRING"),
    ("partition_key", "STRING"),
    ("subject", "STRING"),
    ("correlation_id", "STRING"),
    ("content_type", "STRING"),
    ("reply_to", "STRING"),
    ("reply_to_session_id", "STRING"),
    ("to_address", "STRING"),
    ("application_properties", "STRING"),
    ("delivery_count", "INTEGER"),
    ("state", "STRING"),
    ("time_to_live_seconds", "FLOAT"),
    ("expires_at_utc", "TIMESTAMP"),
    ("scheduled_enqueue_time_utc", "TIMESTAMP"),
    ("dead_letter_reason", "STRING"),
    ("dead_letter_error_description", "STRING"),
    ("dead_letter_source", "STRING"),
    ("body_type", "STRING"),
    ("message_annotations", "STRING"),
    ("amqp_durable", "BOOLEAN"),
    ("amqp_priority", "INTEGER"),
    ("amqp_first_acquirer", "BOOLEAN"),
    ("amqp_user_id", "STRING"),
    ("amqp_content_encoding", "STRING"),
    ("amqp_creation_time_utc", "TIMESTAMP"),
    ("amqp_absolute_expiry_time_utc", "TIMESTAMP"),
    ("amqp_group_sequence", "INTEGER"),
    ("amqp_reply_to_group_id", "STRING"),
    ("source_entity", "STRING"),
    ("settlement_mode", "STRING"),
    ("extracted_at_utc", "TIMESTAMP"),
)


def metadata_column_names() -> list[str]:
    return [name for name, _ in METADATA_COLUMNS]


def utc_now() -> datetime:
    """The run's one wall clock (aware UTC). The component reads T0, ``extracted_at_utc``, the
    pending set's ``deferred_at_utc`` and every collaborator's lock / expiry clock from it, so the
    functional suite can align it with the fake broker's clock (Global Constraints: injectable time)."""
    return datetime.now(UTC)


def format_timestamp(value: datetime | int | float | None) -> str:  # noqa: PYI041 -- the two numeric
    # arms are semantically distinct (epoch milliseconds, always integral in practice) and the task
    # interface spells out `int | float` explicitly.
    """Spec §6.9 value formats: an aware or naive ``datetime`` (naive is already UTC, as every
    broker-supplied datetime is) renders as ``YYYY-MM-DD HH:MM:SS.ffffff`` UTC; an ``int`` / ``float``
    is epoch **milliseconds** (the AMQP ``creation-time`` / ``absolute-expiry-time`` shape); ``None``
    and ``0`` (AMQP's "unset" timestamp) render as empty."""
    if value is None or value == 0:
        return ""
    if isinstance(value, datetime):
        aware = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return aware.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")
    return datetime.fromtimestamp(value / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S.%f")


def _str(value: object) -> str:
    """A plain STRING / INTEGER cell: ``None`` -> empty, everything else -> ``str(value)``."""
    return "" if value is None else str(value)


def _bool_str(value: bool | None) -> str:
    if value is None:
        return ""
    return "true" if value else "false"


def _bytes_str(value: bytes | str | None) -> str:
    """An AMQP header / properties extra that pyamqp may hand back as bytes (§6.9 E24)."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _json_or_empty(value: object) -> str:
    """``application_properties`` / ``raw_amqp_message.annotations``: compact JSON, or empty when
    the map is ``None`` or empty (§6.9 step 3)."""
    return compact_json(value) if value else ""


def message_metadata(
    message: Any,
    *,
    entity: EntityRef,
    settlement_mode: SettlementMode,
    extracted_at: datetime,
) -> dict[str, str]:
    """Map one received / peeked message to its output row (spec §4-E, §6.9), in
    ``metadata_column_names()`` order. Never touches ``message.body``. The AMQP header / properties
    objects on ``raw_amqp_message`` are read defensively (``getattr(obj, name, None)``) since either
    can itself be ``None`` on the real SDK."""
    raw = message.raw_amqp_message
    header = getattr(raw, "header", None)
    properties = getattr(raw, "properties", None)
    body_type = message.body_type
    ttl = getattr(message, "time_to_live", None)
    return {
        "sequence_number": _str(message.sequence_number),
        "message_id": _str(getattr(message, "message_id", None)),
        "enqueued_time_utc": format_timestamp(getattr(message, "enqueued_time_utc", None)),
        "enqueued_sequence_number": _str(getattr(message, "enqueued_sequence_number", None)),
        "session_id": _str(getattr(message, "session_id", None)),
        "partition_key": _str(getattr(message, "partition_key", None)),
        "subject": _str(getattr(message, "subject", None)),
        "correlation_id": _str(getattr(message, "correlation_id", None)),
        "content_type": _str(getattr(message, "content_type", None)),
        "reply_to": _str(getattr(message, "reply_to", None)),
        "reply_to_session_id": _str(getattr(message, "reply_to_session_id", None)),
        "to_address": _str(getattr(message, "to", None)),
        "application_properties": _json_or_empty(getattr(message, "application_properties", None)),
        "delivery_count": _str(getattr(message, "delivery_count", None)),
        "state": message.state.name,
        "time_to_live_seconds": "" if ttl is None else str(ttl.total_seconds()),
        "expires_at_utc": format_timestamp(getattr(message, "expires_at_utc", None)),
        "scheduled_enqueue_time_utc": format_timestamp(getattr(message, "scheduled_enqueue_time_utc", None)),
        "dead_letter_reason": _str(getattr(message, "dead_letter_reason", None)),
        "dead_letter_error_description": _str(getattr(message, "dead_letter_error_description", None)),
        "dead_letter_source": _str(getattr(message, "dead_letter_source", None)),
        "body_type": body_type.name if hasattr(body_type, "name") else str(body_type),
        "message_annotations": _json_or_empty(getattr(raw, "annotations", None)),
        "amqp_durable": _bool_str(getattr(header, "durable", None)),
        "amqp_priority": _str(getattr(header, "priority", None)),
        "amqp_first_acquirer": _bool_str(getattr(header, "first_acquirer", None)),
        "amqp_user_id": _bytes_str(getattr(properties, "user_id", None)),
        "amqp_content_encoding": _bytes_str(getattr(properties, "content_encoding", None)),
        "amqp_creation_time_utc": format_timestamp(getattr(properties, "creation_time", None)),
        "amqp_absolute_expiry_time_utc": format_timestamp(getattr(properties, "absolute_expiry_time", None)),
        "amqp_group_sequence": _str(getattr(properties, "group_sequence", None)),
        "amqp_reply_to_group_id": _bytes_str(getattr(properties, "reply_to_group_id", None)),
        "source_entity": entity.path,
        "settlement_mode": settlement_mode.value,
        "extracted_at_utc": format_timestamp(extracted_at),
    }


def _preview_body_text(message: Any) -> str:
    """The display-only body decode for ``render_preview``: DATA is text-decoded (charset from
    ``content_type``, errors replaced), VALUE / SEQUENCE render as compact JSON. Any body access or
    decode failure -- the preview must never fail on one bad message -- renders as a fixed marker."""
    try:
        body_type = message.body_type
        kind = body_type.name if hasattr(body_type, "name") else str(body_type)
        if kind == "DATA":
            raw = b"".join(message.body)
            return raw.decode(charset_of(getattr(message, "content_type", None)), errors="replace")
        if kind == "SEQUENCE":
            return compact_json(list(message.body))
        return compact_json(message.body)  # VALUE
    except Exception:  # noqa: BLE001 -- display-only decode; any failure must not fail the whole preview.
        return "<unreadable body>"


def _preview_cell(text: str) -> str:
    """One markdown table cell: every line break (CRLF, LF or a lone CR) becomes one space and ``|``
    is escaped, so a sender-supplied value can never end the row or add a column."""
    return text.replace("\r\n", " ").replace("\r", " ").replace("\n", " ").replace("|", "\\|")


def render_preview(messages: Sequence[Any], body_chars: int = 120) -> str:
    """The ``previewMessages`` sync action's markdown table (spec §5.4): sequence number, enqueued
    time, message id, subject, state and the first ``body_chars`` characters of the text-decoded
    body. Nothing is settled here -- ``messages`` are peeked by the caller. The free-text cells
    (message id, subject, body) go through the same escaping; the body is cut to ``body_chars``
    *before* it, so escaping never splits a ``\\|`` pair across the cut."""
    rows = ["| Sequence Number | Enqueued (UTC) | Message ID | Subject | State | Body |", "|---|---|---|---|---|---|"]
    for message in messages:
        message_id = _preview_cell(_str(getattr(message, "message_id", None)))
        subject = _preview_cell(_str(getattr(message, "subject", None)))
        body = _preview_cell(_preview_body_text(message)[:body_chars])
        rows.append(
            f"| {message.sequence_number} | {format_timestamp(getattr(message, 'enqueued_time_utc', None))} "
            f"| {message_id} | {subject} | {message.state.name} | {body} |"
        )
    return "\n".join(rows)
