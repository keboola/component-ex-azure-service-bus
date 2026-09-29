"""``RecordingSink``: an in-memory ``RowSink`` for unit tests of the batch processor and the loops.

Each written ``OutputRow`` becomes one flat dict -- its metadata columns plus ``body`` and
``fields`` -- so a test reads ``row["body"]``, ``row["state"]`` or ``row["fields"]`` directly.
"""

from collections.abc import Sequence
from typing import Any

from output import OutputRow


class RecordingSink:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def write_rows(self, rows: Sequence[OutputRow]) -> None:
        self.rows.extend({**row.metadata, "body": row.body, "fields": row.fields} for row in rows)
