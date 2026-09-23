import pytest
from keboola.component.exceptions import UserException

from configuration import EntityType, SubQueue
from entity import EntityRef
from state import ExtractorState, PendingSetBuilder, from_ranges, to_ranges

Q = EntityRef(EntityType.QUEUE, "orders", None, None, SubQueue.NONE)


def test_ranges_roundtrip():
    assert to_ranges([5, 1, 2, 3, 3, 9, 10]) == [[1, 3], [5, 5], [9, 10]]
    assert from_ranges([[1, 3], [5, 5]]) == [1, 2, 3, 5]
    assert to_ranges([]) == []


def test_load_defaults_and_roundtrip():
    s = ExtractorState.load(None)
    assert s.pending_commit == [] and s.peek_cursor is None and s.flatten_columns == []
    assert ExtractorState.load(s.to_dict()).to_dict() == s.to_dict()


def test_unknown_version_rejected():
    with pytest.raises(UserException, match="reset"):
        ExtractorState.load({"version": 99})


def test_unknown_keys_ignored():
    assert ExtractorState.load({"version": 1, "legacy": 1}).version == 1


def test_builder_groups_by_session_and_partition():
    b = PendingSetBuilder()
    b.add(Q, 1, None, 10)
    b.add(Q, 2, None, 30)
    b.add(Q, (52 << 48) | 1, None, 5)
    b.add(Q, 3, "s1", 7)
    built = b.build("2026-09-23 10:00:00.000000")
    assert len(built) == 1 and built[0].entity_ref() == Q
    groups = {(g.session_id, g.partition): g for g in built[0].groups}
    assert groups[(None, 0)].ranges == [[1, 2]] and groups[(None, 0)].max_body_bytes == 30
    assert groups[(None, 52)].sequence_numbers() == [(52 << 48) | 1]
    assert groups[("s1", 0)].sequence_numbers() == [3]
    assert b.count == 4


def test_builder_carry_forward_merges():
    b = PendingSetBuilder()
    b.add(Q, 10, None, 1)
    first = b.build("t")
    b2 = PendingSetBuilder()
    b2.carry(first)
    b2.add(Q, 11, None, 4)
    assert b2.build("t")[0].groups[0].ranges == [[10, 11]]


def test_size_is_small_for_contiguous_ranges():
    b = PendingSetBuilder()
    for seq in range(1, 100_001):
        b.add(Q, seq, None, 100)
    state = ExtractorState(pending_commit=b.build("t"))
    assert state.size_bytes() < 1_000
