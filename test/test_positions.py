"""Utility tests: binlog coordinates."""

from __future__ import annotations

import pytest

from mylox.utils.positions import (
    BinlogPosition,
    compare_positions,
    is_pos_greater_or_equal,
    split_binlog_name,
)


def test_numeric_suffix_beats_string_compare():
    # The original implementation compared raw strings, which sorts
    # mysql-bin.999999 *after* mysql-bin.1000000.
    assert "mysql-bin.999999" > "mysql-bin.1000000"
    assert compare_positions("mysql-bin.999999", 10, "mysql-bin.1000000", 1) == -1
    assert compare_positions("mysql-bin.1000000", 1, "mysql-bin.999999", 10) == 1


def test_position_ordering():
    assert BinlogPosition("mysql-bin.000002", 4) > BinlogPosition("mysql-bin.000001", 999999)
    assert BinlogPosition("mysql-bin.000001", 10) < BinlogPosition("mysql-bin.000001", 11)
    assert BinlogPosition("mysql-bin.000001", 10) == BinlogPosition("mysql-bin.000001", 10)


def test_empty_coordinates_are_smallest():
    assert compare_positions(None, None, "mysql-bin.000001", 1) == -1
    assert compare_positions("mysql-bin.000001", 1, None, None) == 1
    assert is_pos_greater_or_equal(None, None, "mysql-bin.000001", 1) is False
    assert is_pos_greater_or_equal("mysql-bin.000001", 5, "mysql-bin.000001", 5) is True


def test_split_binlog_name():
    assert split_binlog_name("mysql-bin.000042") == ("mysql-bin.", 42)
    assert split_binlog_name("binlog") == ("binlog", -1)


def test_parse_and_maybe():
    assert BinlogPosition.parse("mysql-bin.000001:4") == BinlogPosition("mysql-bin.000001", 4)
    assert BinlogPosition.maybe(None, 5) is None
    assert BinlogPosition.maybe("f", None) is None
    with pytest.raises(ValueError):
        BinlogPosition.parse("nonsense")


def test_str_roundtrip():
    position = BinlogPosition("mysql-bin.000123", 4567)
    assert str(position) == "mysql-bin.000123:4567"
