"""GTID set arithmetic (evaluation issue P0-1 and P0-5)."""

from __future__ import annotations

import pytest

from mylox.gtid import GtidError, GtidSet, intersection, union

UUID = "3E11FA47-71CA-11E1-9E33-C80AA9429562"
OTHER = "11111111-2222-3333-4444-555555555555"


def test_parse_single_and_ranges():
    gtid = GtidSet("%s:1-5:7:9-10" % UUID)
    assert "3e11fa47-71ca-11e1-9e33-c80aa9429562:1" in gtid
    assert "3e11fa47-71ca-11e1-9e33-c80aa9429562:5" in gtid
    assert "3e11fa47-71ca-11e1-9e33-c80aa9429562:6" not in gtid
    assert "3e11fa47-71ca-11e1-9e33-c80aa9429562:10" in gtid
    assert "3e11fa47-71ca-11e1-9e33-c80aa9429562:11" not in gtid


def test_adjacent_ranges_are_merged():
    gtid = GtidSet()
    gtid.add("%s:1-5" % UUID)
    gtid.add("%s:6-9" % UUID)
    gtid.add("%s:4-12" % UUID)
    assert str(gtid) == "%s:1-12" % UUID.lower()


def test_multiple_source_ids():
    gtid = GtidSet()
    gtid.add("%s:1" % UUID)
    gtid.add("%s:1-2" % OTHER)
    assert set(str(gtid).split(",")) == {
        "%s:1" % UUID.lower(),
        "%s:1-2" % OTHER.lower(),
    }


def test_contains_gtidset():
    left = GtidSet("%s:1-10" % UUID)
    right = GtidSet("%s:3-5" % UUID)
    assert right in left
    assert left not in right
    assert GtidSet() in left


def test_union_and_intersection():
    left = GtidSet("%s:1-5" % UUID)
    right = GtidSet("%s:4-9" % UUID)
    assert str(union([left, right])) == "%s:1-9" % UUID.lower()
    assert str(intersection([left, right])) == "%s:4-5" % UUID.lower()
    assert str(intersection([left, GtidSet("%s:100" % OTHER)])) == ""


def test_single_gtid_is_not_a_set():
    """Writing a single GTID into ``gtid_set`` loses everything before it."""
    executed = GtidSet("%s:1-100" % UUID).copy()
    executed.add("%s:123" % UUID)
    single = GtidSet("%s:123" % UUID)

    assert "3e11fa47-71ca-11e1-9e33-c80aa9429562:1" not in single
    assert "3e11fa47-71ca-11e1-9e33-c80aa9429562:1" in executed
    assert str(executed) == "%s:1-100:123" % UUID.lower()


def test_merge_is_compact():
    executed = GtidSet()
    for number in range(1, 5000):
        executed.add("%s:%d" % (UUID, number))
    assert str(executed) == "%s:1-4999" % UUID.lower()


def test_invalid_input():
    with pytest.raises(GtidError):
        GtidSet("not-a-uuid:1")
    with pytest.raises(GtidError):
        GtidSet.split_gtid("12345")
    assert GtidSet.safe_parse("garbage").is_empty()
    assert GtidSet("%s:1" % UUID) == "%s:1" % UUID
    assert not GtidSet()
    assert bool(GtidSet("%s:1" % UUID))


def test_copy_is_deep():
    original = GtidSet("%s:1-3" % UUID)
    clone = original.copy()
    clone.add("%s:9" % UUID)
    assert original != clone
