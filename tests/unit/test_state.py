from itertools import islice

import pytest
from keboola.component.exceptions import UserException

from configuration import EntityType, SubQueue
from entity import EntityRef
from state import ExtractorState, PendingEntity, PendingGroup, PendingSetBuilder, iter_ranges, merge_ranges, to_ranges

Q = EntityRef(EntityType.QUEUE, "orders", None, None, SubQueue.NONE)
HUGE = 10**14  # far more sequence numbers than memory could hold (and still partition 0: < 2**48)


def test_ranges_roundtrip():
    assert to_ranges([5, 1, 2, 3, 3, 9, 10]) == [[1, 3], [5, 5], [9, 10]]
    assert list(iter_ranges([[1, 3], [5, 5]])) == [1, 2, 3, 5]
    assert to_ranges([]) == []


def test_merge_ranges_normalises_a_hand_edited_state():
    assert merge_ranges([[9, 10], [1, 3], [2, 5], [6, 6], [8, 7]]) == [[1, 6], [9, 10]]  # [8, 7] is empty
    assert list(iter_ranges([[3, 4], [1, 3]])) == [1, 2, 3, 4]  # ascending, no duplicates
    assert merge_ranges([]) == []


def test_pending_group_is_lazy_over_huge_ranges():
    group = PendingGroup(ranges=[[HUGE, 2 * HUGE], [1, 2]])
    assert group.count == HUGE + 3
    assert list(islice(group.iter_sequence_numbers(), 4)) == [1, 2, HUGE, HUGE + 1]
    assert group.tail(HUGE + 5).ranges == [[HUGE + 5, 2 * HUGE]] and group.tail(0).ranges == [[1, 2], [HUGE, 2 * HUGE]]
    assert group.tail(2 * HUGE + 1).count == 0


def test_builder_carry_keeps_ranges_without_expanding_them():
    carried = PendingEntity(entity=Q.to_dict(), groups=[PendingGroup(max_body_bytes=9, ranges=[[1, HUGE]])])
    b = PendingSetBuilder()
    b.carry([carried])
    b.add(Q, HUGE + 1, None, 4)
    b.add(Q, HUGE + 5, None, 4)
    (group,) = b.build("t")[0].groups
    assert group.ranges == [[1, HUGE + 1], [HUGE + 5, HUGE + 5]] and group.max_body_bytes == 9
    assert b.count == HUGE + 2


def test_load_defaults_and_roundtrip():
    s = ExtractorState.load(None)
    assert s.pending_commit == [] and s.peek_cursor is None and s.flatten_columns == []
    assert ExtractorState.load(s.to_dict()).to_dict() == s.to_dict()


def test_unknown_version_rejected():
    with pytest.raises(UserException, match="Reset the row state"):
        ExtractorState.load({"version": 99})


def test_unknown_keys_ignored():
    assert ExtractorState.load({"version": 1, "legacy": 1}).version == 1


def test_malformed_state_rejected():
    with pytest.raises(UserException, match="malformed"):
        ExtractorState.load({"version": 1, "pending_commit": "not-a-list"})


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
    assert list(groups[(None, 52)].iter_sequence_numbers()) == [(52 << 48) | 1]
    assert list(groups[("s1", 0)].iter_sequence_numbers()) == [3]
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
