"""Streaming output table and manifest (Task 9, spec §6.9, §6.10).

``OutputTable`` fixes its column set exactly once, at ``open()``: ``text`` / ``base64`` write the
fixed metadata columns plus ``body``; ``json_flatten`` writes the metadata columns plus the *input*
flatten registry's columns -- never columns first discovered during this run -- plus the reserved
``body_unmapped`` column. The manifest is written immediately at ``open()`` with
``write_always=False``; the caller (settlers, Tasks 11 / 14) arms it -- ``arm_write_always()`` -- at
its mode's first destructive point, rewriting the manifest with ``write_always=True``. Every row is
streamed straight into the CSV and flushed to disk (``f.flush()`` + ``os.fsync``) once per
``write_rows`` call -- no staging file, no rewrite at close, nothing else is ever written under the
table's output directory. This module is deliberately ignorant of settlement modes: it only exposes
``arm_write_always()`` for the caller to invoke at the right moment.
"""

import csv
import logging
import os
from collections import OrderedDict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Protocol, Self

from keboola.component.dao import BaseType, ColumnDefinition, TableDefinition

from body import UNMAPPED_COLUMN, FlattenRegistry
from columns import BODY_COLUMN, METADATA_COLUMNS, metadata_column_names
from configuration import BodyFormat, DestinationConfig

logger = logging.getLogger(__name__)

# The §6.9 native type name -> BaseType factory. Every column outside METADATA_COLUMNS (``body``,
# flattened ``body_*``, ``body_unmapped``) defaults to STRING.
_BASE_TYPE_FACTORIES: dict[str, Callable[[], BaseType]] = {
    "STRING": BaseType.string,
    "INTEGER": BaseType.integer,
    "FLOAT": BaseType.float,
    "BOOLEAN": BaseType.boolean,
    "TIMESTAMP": BaseType.timestamp,
}


@dataclass(frozen=True)
class OutputRow:
    """One row handed to a ``RowSink``: ``metadata`` is always the full ``message_metadata()`` map;
    exactly one of ``body`` (``text`` / ``base64``) or ``fields`` (``json_flatten``, the raw
    ``path -> cell`` map from ``encode_body``) is set, matching the sink's ``body_format``."""

    metadata: dict[str, str]
    body: str | None = None
    fields: dict[tuple[str, ...], str] | None = None


class RowSink(Protocol):
    def write_rows(self, rows: Sequence[OutputRow]) -> None: ...


def schema_for(columns: list[str]) -> OrderedDict[str, ColumnDefinition]:
    """``columns``' manifest schema, in order: a metadata column gets its §6.9 native type,
    everything else (``body``, flattened ``body_*``, ``body_unmapped``) is ``BaseType.string()``."""
    native_types = dict(METADATA_COLUMNS)
    schema: OrderedDict[str, ColumnDefinition] = OrderedDict()
    for name in columns:
        factory = _BASE_TYPE_FACTORIES[native_types.get(name, "STRING")]
        schema[name] = ColumnDefinition(data_types=factory())
    return schema


class OutputTable:
    """A single streamed output CSV plus its manifest (spec §6.9). Implements ``RowSink`` and the
    context-manager protocol: ``__enter__`` opens the table (creates the CSV, writes the header,
    writes the manifest), ``__exit__`` closes it and never lets a close failure mask an in-flight
    exception.
    """

    def __init__(
        self,
        *,
        table_name: str,
        destination: DestinationConfig,
        body_format: BodyFormat,
        registry: FlattenRegistry | None,
        create_definition: Callable[..., TableDefinition],
        write_manifest: Callable[[TableDefinition], None],
    ) -> None:
        self._table_name = table_name
        self._destination = destination
        self._body_format = body_format
        self._registry = registry
        self._create_definition = create_definition
        self._write_manifest = write_manifest
        self._columns: list[str] = []
        self._definition: TableDefinition | None = None
        self._file = None
        self._writer: csv.DictWriter | None = None
        self.write_always_armed = False
        self.rows_written = 0

    @property
    def columns(self) -> list[str]:
        return self._columns

    def _fixed_columns(self) -> list[str]:
        names = metadata_column_names()
        if self._body_format is BodyFormat.JSON_FLATTEN:
            if self._registry is None:
                raise ValueError("json_flatten body format requires a FlattenRegistry")
            return [*names, *self._registry.input_columns, UNMAPPED_COLUMN]
        return [*names, BODY_COLUMN]

    def open(self) -> None:
        """Fix the column set, create the CSV at the definition's ``full_path``, write its header
        and write the manifest -- with ``write_always=False`` unless ``arm_write_always()`` was
        already (unusually) called before ``open()``."""
        self._columns = self._fixed_columns()
        self._definition = self._create_definition(
            name=f"{self._table_name}.csv",
            schema=schema_for(self._columns),
            primary_key=self._destination.primary_key.columns,
            incremental=self._destination.incremental,
            write_always=self.write_always_armed,
            has_header=True,
        )
        self._write_manifest(self._definition)
        # The file deliberately stays open across `write_rows` calls until `close()` -- it cannot be
        # a `with` block scoped to this method.
        self._file = open(self._definition.full_path, "w", newline="", encoding="utf-8")  # noqa: SIM115
        self._writer = csv.DictWriter(self._file, fieldnames=self._columns, extrasaction="raise", restval="")
        self._writer.writeheader()
        self._file.flush()
        os.fsync(self._file.fileno())

    def arm_write_always(self) -> None:
        """Flip ``write_always`` on and rewrite the manifest. Idempotent: once armed, a further
        call does nothing -- not even a redundant manifest write."""
        if self.write_always_armed:
            return
        assert self._definition is not None, "arm_write_always() called before open()"
        self.write_always_armed = True
        self._definition.write_always = True
        self._write_manifest(self._definition)

    def write_rows(self, rows: Sequence[OutputRow]) -> None:
        """Write every row of ``rows`` completely (or raise -- a partially written batch never
        happens), then flush and fsync once for the whole call."""
        assert self._writer is not None and self._file is not None, "write_rows() called before open()"
        for row in rows:
            self._writer.writerow(self._row_record(row))
        self.rows_written += len(rows)
        self._file.flush()
        os.fsync(self._file.fileno())

    def _row_record(self, row: OutputRow) -> dict[str, str]:
        record = dict(row.metadata)
        if self._body_format is BodyFormat.JSON_FLATTEN:
            assert self._registry is not None, "json_flatten body format requires a FlattenRegistry"
            # `split` raises `BodyTooLargeError` only for an over-limit `body_unmapped` cell; the
            # processor already validated every row with `split` before handing it to us, so a raise
            # here is a bug in the caller, not something this sink should catch.
            values, unmapped = self._registry.split(row.fields or {})
            record.update(values)
            record[UNMAPPED_COLUMN] = unmapped
        else:
            record[BODY_COLUMN] = row.body if row.body is not None else ""
        return record

    def close(self) -> None:
        """Close the CSV file. Idempotent: a second call is a no-op. ``file.close()`` flushes any
        buffered data itself, so no explicit flush is needed here."""
        if self._file is None:
            return
        self._file.close()
        self._file = None
        self._writer = None

    def __enter__(self) -> Self:
        self.open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        try:
            self.close()
        except Exception as close_error:
            if exc is None:
                raise
            logger.error(
                "Failed to close output table %s while another exception was in flight: %s",
                self._table_name,
                close_error,
            )
        return False
