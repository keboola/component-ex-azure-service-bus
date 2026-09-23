"""Row-scoped state model, range encoding and the pending-commit-set builder (Task 6, spec §6.3, §6.8).

``ExtractorState`` is the exact §6.8 layout: a versioned envelope around the deferred-message
pending-commit set (H2/H5), the C4 peek cursor and the flatten-registry carry-over. Ranges keep a
non-partitioned single-consumer entity's pending set tiny -- consecutive deferrals collapse to one
``[start, end]`` pair instead of one entry per sequence number. ``PendingSetBuilder`` accumulates
deferrals (and, on H3, carried-forward groups from an earlier run's state) during C2/H3 and emits the
``PendingEntity`` list ``ExtractorState.pending_commit`` stores. A stored range can cover millions of
sequence numbers, so ranges are only ever expanded lazily (``iter_ranges``), never into a list.
"""

import json
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any

from keboola.component.exceptions import UserException
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from entity import EntityRef, partition_of

STATE_VERSION = 1
STATE_BUDGET_BYTES = 256 * 1024

_COMPACT_SEPARATORS = (",", ":")


def to_ranges(seqs: Iterable[int]) -> list[list[int]]:
    """Sorted, deduplicated sequence numbers collapsed into inclusive ``[start, end]`` ranges."""
    ordered = sorted(set(seqs))
    if not ordered:
        return []
    ranges: list[list[int]] = []
    start = end = ordered[0]
    for seq in ordered[1:]:
        if seq == end + 1:
            end = seq
        else:
            ranges.append([start, end])
            start = end = seq
    ranges.append([start, end])
    return ranges


def merge_ranges(ranges: Iterable[Sequence[int]]) -> list[list[int]]:
    """Inclusive ``[start, end]`` ranges sorted, with overlapping and adjacent ones joined and empty
    ones (``end < start``) dropped -- the ranges of a stored state may have been edited by hand."""
    merged: list[list[int]] = []
    for start, end in sorted((start, end) for start, end in ranges if start <= end):
        if merged and start <= merged[-1][1] + 1:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged


def iter_ranges(ranges: Iterable[Sequence[int]]) -> Iterator[int]:
    """The inverse of ``to_ranges``, lazily: every covered sequence number, ascending and deduplicated."""
    for start, end in merge_ranges(ranges):
        yield from range(start, end + 1)


def count_ranges(ranges: Iterable[Sequence[int]]) -> int:
    """How many distinct sequence numbers ``ranges`` cover, without expanding them."""
    return sum(end - start + 1 for start, end in merge_ranges(ranges))


class PendingGroup(BaseModel):
    """One (entity, session, partition) group of deferred sequence numbers (§6.3)."""

    model_config = ConfigDict(extra="ignore")

    session_id: str | None = None
    partition: int = 0
    max_body_bytes: int = 0
    ranges: list[list[int]] = Field(default_factory=list)

    def iter_sequence_numbers(self) -> Iterator[int]:
        return iter_ranges(self.ranges)

    @property
    def count(self) -> int:
        return count_ranges(self.ranges)

    def tail(self, first: int) -> PendingGroup:
        """This group restricted to its sequence numbers ``>= first`` -- what a commit interrupted
        after deleting everything below ``first`` leaves pending."""
        ranges = [[max(start, first), end] for start, end in merge_ranges(self.ranges) if end >= first]
        return self.model_copy(update={"ranges": ranges})


class PendingEntity(BaseModel):
    """The stored entity plus its groups deferred at ``deferred_at_utc`` (H2)."""

    model_config = ConfigDict(extra="ignore")

    entity: dict[str, Any] = Field(default_factory=dict)
    groups: list[PendingGroup] = Field(default_factory=list)
    deferred_at_utc: str = ""

    def entity_ref(self) -> EntityRef:
        return EntityRef.from_dict(self.entity)


class PeekCursor(BaseModel):
    """The C4 peek loop's resume point (§6.7)."""

    model_config = ConfigDict(extra="ignore")

    entity_path: str
    last_sequence_number: int


class FlattenColumn(BaseModel):
    """One registry entry mapping a JSON path hash to its assigned output column (§6.10)."""

    model_config = ConfigDict(extra="ignore")

    path_sha1: str
    column: str


class ExtractorState(BaseModel):
    """Row-scoped ``state.json`` (§6.8): version 1, merge rule applied by the caller (each mode
    updates only the keys it owns and writes the rest back unchanged)."""

    model_config = ConfigDict(extra="ignore")

    version: int = STATE_VERSION
    pending_commit: list[PendingEntity] = Field(default_factory=list)
    peek_cursor: PeekCursor | None = None
    flatten_columns: list[FlattenColumn] = Field(default_factory=list)

    @classmethod
    def load(cls, raw: dict[str, Any] | None) -> ExtractorState:
        """Missing / empty state (first run) loads as defaults; an unsupported ``version`` -- or a
        ``version: 1`` state whose shape pydantic rejects (a hand-edited or corrupted file) -- is a
        user-fixable problem (reset the row state), never a silent reinterpretation and never an
        unexpected (exit 2) failure."""
        if not raw:
            return cls()
        version = raw.get("version", STATE_VERSION)
        if version != STATE_VERSION:
            raise UserException(
                f"Unsupported state version {version}: this component understands state version 1. "
                "Reset the row state; the next run starts without a cursor, pending set or column registry."
            )
        try:
            return cls.model_validate(raw)
        except ValidationError as e:
            raise UserException(
                "The row state is malformed and cannot be read: reset the row state; the next run starts "
                f"without a cursor, pending set or column registry. (details: {e})"
            ) from e

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    def size_bytes(self) -> int:
        return len(json.dumps(self.to_dict(), separators=_COMPACT_SEPARATORS))


@dataclass
class _GroupDraft:
    """One group being built: the sequence numbers deferred in this run (added one by one) plus the
    carried-forward ranges, kept as ranges (a carried range may cover millions of sequence numbers)."""

    seqs: set[int] = field(default_factory=set)
    carried: list[list[int]] = field(default_factory=list)
    max_body_bytes: int = 0

    def ranges(self) -> list[list[int]]:
        return merge_ranges([*self.carried, *to_ranges(self.seqs)])


class PendingSetBuilder:
    """Accumulates deferred (and carried-forward) groups during C2/H3 and emits the
    ``pending_commit`` list ``ExtractorState`` stores. Keyed by ``(entity_key, session_id,
    partition)`` where ``entity_key`` is the sorted-key JSON of ``EntityRef.to_dict()`` -- stable and
    hashable without requiring ``EntityRef`` itself to be a dict key.
    """

    def __init__(self) -> None:
        self._entities: dict[str, dict[str, Any]] = {}
        self._groups: dict[tuple[str, str | None, int], _GroupDraft] = {}

    def _entity_key(self, entity: EntityRef) -> str:
        key = json.dumps(entity.to_dict(), sort_keys=True)
        self._entities.setdefault(key, entity.to_dict())
        return key

    def add(self, entity: EntityRef, sequence_number: int, session_id: str | None, body_bytes: int) -> None:
        entity_key = self._entity_key(entity)
        draft = self._groups.setdefault((entity_key, session_id, partition_of(sequence_number)), _GroupDraft())
        draft.seqs.add(sequence_number)
        draft.max_body_bytes = max(draft.max_body_bytes, body_bytes)

    def carry(self, entities: list[PendingEntity]) -> None:
        """Merge carried-forward groups (e.g. H5's session-locked carry, H3's scan resume) into the
        same ``(entity, session, partition)`` keys already being built, keeping the max of
        ``max_body_bytes``."""
        for pending_entity in entities:
            entity = pending_entity.entity_ref()
            entity_key = self._entity_key(entity)
            for group in pending_entity.groups:
                draft = self._groups.setdefault((entity_key, group.session_id, group.partition), _GroupDraft())
                draft.carried = merge_ranges([*draft.carried, *group.ranges])
                draft.max_body_bytes = max(draft.max_body_bytes, group.max_body_bytes)

    def build(self, deferred_at_utc: str) -> list[PendingEntity]:
        by_entity: dict[str, list[PendingGroup]] = {}
        for (entity_key, session_id, partition), draft in self._groups.items():
            by_entity.setdefault(entity_key, []).append(
                PendingGroup(
                    session_id=session_id,
                    partition=partition,
                    max_body_bytes=draft.max_body_bytes,
                    ranges=draft.ranges(),
                )
            )
        result: list[PendingEntity] = []
        for entity_key, groups in by_entity.items():
            groups.sort(key=lambda g: (g.session_id or "", g.partition))
            result.append(
                PendingEntity(
                    entity=self._entities[entity_key],
                    groups=groups,
                    deferred_at_utc=deferred_at_utc,
                )
            )
        return result

    @property
    def count(self) -> int:
        return sum(count_ranges(draft.ranges()) for draft in self._groups.values())
