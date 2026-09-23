"""Body decoding, JSON flattening and the flatten-column registry (Task 7, spec §6.6, §6.10).

``encode_body`` turns a received message's body into either a single output cell (``text`` /
``base64``) or a leaf-path map (``json_flatten``), regardless of the AMQP body type (``DATA`` /
``VALUE`` / ``SEQUENCE``) the sender used. ``FlattenRegistry`` is the run-scoped, state-carried
mapping from a JSON path to its stable output column name (spec §6.10): every run materialises
exactly its *input* registry's columns plus the reserved ``body_unmapped`` column, so a job never
adds a column its saved state does not already know about (`split`); a key first seen in this run
goes to ``body_unmapped`` and is recorded for the *next* run (`register`). Over the column cap, a
path gets a provisional name that is never saved but is reused for the same path for the rest of
the run (Phase-4 amendment 4), so every row keys that path identically in ``body_unmapped``.

No message body content is ever logged here -- callers log sequence number / message id only.
"""

import base64
import codecs
import hashlib
import json
import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from configuration import BodyFormat
from state import FlattenColumn

CELL_LIMIT_BYTES = 16 * 1024 * 1024
MAX_FLATTEN_COLUMNS = 1000
MAX_COLUMN_NAME = 64
FLATTEN_PREFIX = "body_"
ROOT_VALUE_COLUMN = "body_value"
UNMAPPED_COLUMN = "body_unmapped"

_COLUMN_CHARS_RE = re.compile(r"[^A-Za-z0-9_]")


class BodyDecodeError(Exception):
    """Body access or decoding raised; wraps the cause (spec §6.6)."""


class NotJsonError(Exception):
    """``json_flatten`` and the body is not valid JSON in its charset (spec §6.6)."""


class BodyTooLargeError(Exception):
    """An encoded cell would exceed ``CELL_LIMIT_BYTES`` (spec §6.6)."""


@dataclass
class EncodedBody:
    """The result of ``encode_body``: either ``cell`` (``text`` / ``base64``) or ``fields``
    (``json_flatten``) is set, never both. ``size_bytes`` is the raw body size -- DATA sections
    joined, or the UTF-8 length of the VALUE/SEQUENCE body's compact JSON -- fed to the commit byte
    cap, independent of the chosen output format."""

    body_type: str
    size_bytes: int
    cell: str | None
    fields: dict[tuple[str, ...], str] | None


def charset_of(content_type: str | None) -> str:
    """The ``charset=`` parameter of ``content_type`` if Python has a *text* codec for it, else
    ``utf-8``. A sender-controlled ``content_type`` naming a non-text codec (``base64``, ``rot13``,
    ``zlib_codec``, ...) must not reach ``bytes.decode`` -- that raises ``LookupError`` /
    ``UnicodeDecodeError`` outside every unreadable-body policy -- so those are rejected too."""
    if not content_type:
        return "utf-8"
    for part in content_type.split(";")[1:]:
        key, _, value = part.partition("=")
        if key.strip().lower() != "charset":
            continue
        charset = value.strip().strip('"').strip("'")
        try:
            info = codecs.lookup(charset)
        except LookupError:
            return "utf-8"
        if not getattr(info, "_is_text_encoding", True):
            return "utf-8"
        return charset
    return "utf-8"


def to_jsonable(value: object) -> object:
    """Recursively make ``value`` safe for JSON encoding: ``bytes`` (and dict keys) become UTF-8
    strings with replacement, ``datetime`` becomes ISO-8601, ``Decimal`` is kept as-is (``compact_json``
    renders it as a bare, unquoted JSON number so ``1.10`` never loses its trailing zero or gets
    quoted), ``uuid.UUID`` becomes ``str()``, other non-JSON scalars fall back to ``str()``."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {to_jsonable(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [to_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return value
    if isinstance(value, UUID):
        return str(value)
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


def _json_key(key: object) -> str:
    """A dict key rendered the way ``json.dumps`` renders a non-string key (``int``/``float`` as
    their text, ``True``/``False``/``None`` as ``true``/``false``/``null``), since the manual
    encoder below no longer delegates key coercion to ``json.dumps``."""
    if isinstance(key, str):
        return key
    if key is True:
        return "true"
    if key is False:
        return "false"
    if key is None:
        return "null"
    return str(key)


def _encode_json(value: object) -> str:
    """A minimal recursive JSON encoder for an already-``to_jsonable`` value: dicts and lists are
    walked so a nested ``Decimal`` renders as ``str(d)`` -- a bare, unquoted JSON number, preserving
    its exact text (``1.10`` stays ``1.10``, never ``1.1`` or ``"1.10"``) -- while every other leaf
    goes through ``json.dumps`` (``default=str`` is a safety net; ``to_jsonable`` has already
    resolved every other non-native type)."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        items = ",".join(f"{json.dumps(_json_key(k), ensure_ascii=False)}:{_encode_json(v)}" for k, v in value.items())
        return "{" + items + "}"
    if isinstance(value, list):
        return "[" + ",".join(_encode_json(item) for item in value) + "]"
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def compact_json(value: object) -> str:
    return _encode_json(to_jsonable(value))


def _leaf_cell(value: object) -> str:
    """One flattened field's cell text (spec §6.10): booleans as ``true``/``false`` (checked before
    ``int``, since ``bool`` is its subclass), ``None`` as empty, arrays as compact JSON, everything
    else as its plain text."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    if isinstance(value, list):
        return compact_json(value)
    if isinstance(value, str):
        return value
    if isinstance(value, int | Decimal):
        return str(value)
    return compact_json(value)


def _flatten_leaf(value: object, path: tuple[str, ...]) -> dict[tuple[str, ...], str]:
    if isinstance(value, dict):
        if not value:
            return {path: "{}"}
        fields: dict[tuple[str, ...], str] = {}
        for key, item in value.items():
            fields.update(_flatten_leaf(item, path + (str(key),)))
        return fields
    return {path: _leaf_cell(value)}


def flatten_value(value: object) -> dict[tuple[str, ...], str]:
    """A JSON value flattened into ``path -> cell`` (spec §6.10). A non-empty root object expands
    recursively; an empty root object has no leaves at all; any other root (array or scalar) is not
    an object, so it becomes the single field ``{(): compact_json(value)}``."""
    if isinstance(value, dict):
        if not value:
            return {}
        fields: dict[tuple[str, ...], str] = {}
        for key, item in value.items():
            fields.update(_flatten_leaf(item, (str(key),)))
        return fields
    return {(): compact_json(value)}


def column_name_for(path: tuple[str, ...]) -> str:
    """The column name for ``path`` (spec §6.10), with no collision handling: ``()`` is the root
    value column; otherwise ``body_`` + the keys joined with ``_``, ASCII-folded (NFKD, combining
    marks dropped), every character outside ``[A-Za-z0-9_]`` replaced and trailing ``_`` stripped;
    names over 64 characters are truncated to 55 chars + ``_`` + an 8-hex-char path hash."""
    if not path:
        return ROOT_VALUE_COLUMN
    raw = FLATTEN_PREFIX + "_".join(path)
    folded = "".join(ch for ch in unicodedata.normalize("NFKD", raw) if not unicodedata.combining(ch))
    name = _COLUMN_CHARS_RE.sub("_", folded).rstrip("_")
    if len(name) > MAX_COLUMN_NAME:
        digest = hashlib.sha1(json.dumps(list(path)).encode()).hexdigest()[:8]
        name = name[:55] + "_" + digest
    return name


def path_hash(path: tuple[str, ...]) -> str:
    return hashlib.sha1(json.dumps(list(path), ensure_ascii=False).encode()).hexdigest()


class FlattenRegistry:
    """The run-scoped, state-carried JSON-path -> column-name registry (spec §6.10).

    ``existing`` seeds the registry from the row's *input* state (order preserved: it is the exact
    column set every mode must write, ``input_columns``). ``register`` assigns -- or looks up -- the
    stable name for a path; beyond ``MAX_FLATTEN_COLUMNS`` recorded entries a path instead gets a
    provisional name (returned, reserved for the run, never saved) that is reused for repeats of the
    same path within the run (amendment 4). ``reserved`` (plus ``UNMAPPED_COLUMN``) blocks flattened
    names from colliding with the fixed metadata columns.
    """

    def __init__(self, existing: list[FlattenColumn], reserved: Iterable[str]) -> None:
        self._by_hash: dict[str, str] = {}
        self._input_order: list[str] = []
        self._provisional: dict[str, str] = {}
        self._new_hashes: list[str] = []
        self._taken: set[str] = set(reserved) | {UNMAPPED_COLUMN}
        self.overflowed = False
        for entry in existing:
            self._by_hash[entry.path_sha1] = entry.column
            self._input_order.append(entry.path_sha1)
            self._taken.add(entry.column)
        self._input = set(self._input_order)

    def _unique_name(self, path: tuple[str, ...]) -> str:
        base = column_name_for(path)
        if base not in self._taken:
            return base
        n = 2
        while True:
            suffix = f"_{n}"
            candidate = base[: MAX_COLUMN_NAME - len(suffix)] + suffix
            if candidate not in self._taken:
                return candidate
            n += 1

    def register(self, path: tuple[str, ...]) -> str:
        h = path_hash(path)
        if h in self._by_hash:
            return self._by_hash[h]
        if h in self._provisional:
            return self._provisional[h]
        if len(self._by_hash) >= MAX_FLATTEN_COLUMNS:
            name = self._unique_name(path)
            self._provisional[h] = name
            self._taken.add(name)
            self.overflowed = True
            return name
        name = self._unique_name(path)
        self._by_hash[h] = name
        self._taken.add(name)
        self._new_hashes.append(h)
        return name

    @property
    def input_columns(self) -> list[str]:
        return [self._by_hash[h] for h in self._input_order]

    @property
    def new_columns(self) -> list[str]:
        return [self._by_hash[h] for h in self._new_hashes]

    @property
    def columns(self) -> list[str]:
        return self.input_columns + self.new_columns

    def to_state(self) -> list[FlattenColumn]:
        return [FlattenColumn(path_sha1=h, column=column) for h, column in self._by_hash.items()]

    def split(self, fields: dict[tuple[str, ...], str]) -> tuple[dict[str, str], str]:
        """Values for the input-registry columns, plus the ``body_unmapped`` cell for every other
        field (this run's new and provisional columns), keyed by the name ``register`` gave each
        path. Every path must already be registered -- by the caller, before calling ``split`` --
        a path this run never registered is a programming error. The per-field cap (checked by the
        caller before ``split``) bounds one field, not the combined ``body_unmapped`` cell -- several
        fields just under the cap can still add up past it, so that combined cell is checked here too
        (``CELL_LIMIT_BYTES`` read at call time, like every other cap check in this module)."""
        values: dict[str, str] = {}
        unmapped: dict[str, str] = {}
        for path, cell in fields.items():
            h = path_hash(path)
            if h in self._by_hash:
                name = self._by_hash[h]
                if h in self._input:
                    values[name] = cell
                else:
                    unmapped[name] = cell
            elif h in self._provisional:
                unmapped[self._provisional[h]] = cell
            else:
                raise KeyError(f"path {path!r} was never registered in this run")
        unmapped_json = compact_json(unmapped) if unmapped else ""
        if len(unmapped_json.encode("utf-8")) > CELL_LIMIT_BYTES:
            raise BodyTooLargeError(f"the body_unmapped cell exceeds the {CELL_LIMIT_BYTES}-byte cell limit")
        return values, unmapped_json


def _check_field_sizes(fields: dict[tuple[str, ...], str]) -> None:
    for cell in fields.values():
        if len(cell.encode("utf-8")) > CELL_LIMIT_BYTES:
            raise BodyTooLargeError(f"a flattened field exceeds the {CELL_LIMIT_BYTES}-byte cell limit")


def encode_body(message: Any, body_format: BodyFormat) -> EncodedBody:
    """Decode ``message``'s body per ``body_format`` (spec §6.10). Body access failures (including
    mid-decode ones the SDK raises as ``TypeError`` / ``BufferError``) become ``BodyDecodeError``;
    ``json_flatten`` on a non-JSON body becomes ``NotJsonError``; an over-limit cell or flattened
    field becomes ``BodyTooLargeError``."""
    raw_type = message.body_type
    body_type = raw_type.name if hasattr(raw_type, "name") else str(raw_type)
    content_type = getattr(message, "content_type", None)

    # Every bit of body *consumption* -- not just the ``.body`` property access -- lives inside this
    # try: a lazily-raising generator (a non-bytes section, a mid-iteration SDK failure) surfaces only
    # once its sections are actually joined / materialised, and must become BodyDecodeError just the
    # same as an eager failure at property-access time.
    try:
        sections = message.body
        if body_type == "DATA":
            raw: bytes | None = b"".join(sections)
            value: object = None
            size_bytes = len(raw)
        else:
            raw = None
            materialised = list(sections) if body_type == "SEQUENCE" else sections
            value = to_jsonable(materialised)
            size_bytes = len(compact_json(value).encode("utf-8"))
    except Exception as e:
        raise BodyDecodeError(f"{type(e).__name__}: {e}") from e

    if body_format is BodyFormat.JSON_FLATTEN:
        if raw is not None:
            charset = charset_of(content_type)
            try:
                text = raw.decode(charset, errors="strict")
            except UnicodeDecodeError as e:
                raise NotJsonError(str(e)) from e
            try:
                parsed = json.loads(text, parse_float=Decimal)
            except (ValueError, RecursionError) as e:
                raise NotJsonError(str(e)) from e
        else:
            parsed = value
        try:
            fields = flatten_value(parsed)
        except RecursionError as e:
            raise NotJsonError(str(e)) from e
        _check_field_sizes(fields)
        return EncodedBody(body_type=body_type, size_bytes=size_bytes, cell=None, fields=fields)

    if body_format is BodyFormat.TEXT:
        cell = raw.decode(charset_of(content_type), errors="replace") if raw is not None else compact_json(value)
    else:  # BASE64
        raw_bytes = raw if raw is not None else compact_json(value).encode("utf-8")
        cell = base64.b64encode(raw_bytes).decode("ascii")

    if len(cell.encode("utf-8")) > CELL_LIMIT_BYTES:
        raise BodyTooLargeError(f"the body cell exceeds the {CELL_LIMIT_BYTES}-byte cell limit")
    return EncodedBody(body_type=body_type, size_bytes=size_bytes, cell=cell, fields=None)
