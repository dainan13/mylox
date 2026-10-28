"""Row image decoding (bitmaps, MINIMAL/FULL image, column naming)."""

from __future__ import annotations

from mylox.utils.rowimage import (
    bitmap_covers,
    bitmap_expected_columns,
    column_index_for_key,
    is_column_present_in_bitmap,
    remap_columns_with_bitmap,
)


def test_column_index_lookup():
    assert column_index_for_key("UNKNOWN_COL2", ["a", "b", "c"]) == 2
    assert column_index_for_key("b", ["a", "b", "c"]) == 1
    assert column_index_for_key("zz", ["a", "b"]) is None
    assert column_index_for_key(None, ["a"]) is None


def test_bitmap_bits_are_little_endian():
    # 0b0000_0101 -> columns 0 and 2 present
    bitmap = bytes([0b00000101])
    assert is_column_present_in_bitmap(bitmap, 0) is True
    assert is_column_present_in_bitmap(bitmap, 1) is False
    assert is_column_present_in_bitmap(bitmap, 2) is True
    assert is_column_present_in_bitmap(bitmap, 8) is False


def test_bitmap_none_means_everything_present():
    assert is_column_present_in_bitmap(None, 42) is True
    assert remap_columns_with_bitmap({"UNKNOWN_COL0": 1}, ["a"], None) == {"a": 1}


def test_remap_drops_absent_columns():
    # MINIMAL image: only column 0 and 2 are present.
    row = {"UNKNOWN_COL0": "x", "UNKNOWN_COL1": None, "UNKNOWN_COL2": "y"}
    bitmap = bytes([0b00000101])
    assert remap_columns_with_bitmap(row, ["a", "b", "c"], bitmap) == {"a": "x", "c": "y"}


def test_remap_accepts_real_names():
    row = {"a": 1, "b": None, "c": 3}
    bitmap = [True, False, True]
    assert remap_columns_with_bitmap(row, ["a", "b", "c"], bitmap) == {"a": 1, "c": 3}


def test_remap_keeps_unknown_keys_and_reports_drift():
    seen = []
    row = {"UNKNOWN_COL0": 1, "UNKNOWN_COL7": 9}
    result = remap_columns_with_bitmap(row, ["a"], None, on_unknown=lambda key, index: seen.append((key, index)))
    assert result == {"a": 1, "UNKNOWN_COL7": 9}
    assert seen == [("UNKNOWN_COL7", 7)]


def test_empty_inputs():
    assert remap_columns_with_bitmap({}, ["a"]) == {}
    assert remap_columns_with_bitmap({"a": 1}, []) == {}


def test_bitmap_coverage_helpers():
    assert bitmap_expected_columns(bytes([0b1])) == 8
    assert bitmap_expected_columns([True, False]) == 2
    assert bitmap_covers([True, False], 2) is True
    assert bitmap_covers([True, False], 3) is False
    assert bitmap_covers(None, 3) is True


def test_stringify_bytes_keeps_undecodable_values_as_bytes():
    """Binary data must not be silently turned into plausible looking text."""
    from mylox.utils.serialize import stringify_bytes

    # valid UTF-8 is decoded (the common case)
    assert stringify_bytes({"a": "héllo".encode("utf-8")}) == {"a": "héllo"}
    # invalid UTF-8 survives as bytes so a handler can still store/write it
    assert stringify_bytes({"raw": b"\x00\xff\x41"}) == {"raw": b"\x00\xff\x41"}
    assert stringify_bytes(b"\xff") == b"\xff"
    # mapping keys are always strings, even when mangled
    assert stringify_bytes({b"\xffkey": 1}) == {"\ufffdkey": 1}
    assert stringify_bytes([b"ok", b"\xfe"]) == ["ok", b"\xfe"]


def test_unsigned_integers_are_repaired_from_the_schema():
    """binlog_row_metadata=MINIMAL (and MySQL 5.7) loses column signedness."""
    from mylox.schema.model import ColumnSchema, TableSchema, fix_unsigned_values, unsigned_columns

    schema = TableSchema(
        db="d",
        table="t",
        columns=(
            ColumnSchema("big", "bigint(20) unsigned", False, None, "", 0),
            ColumnSchema("mid", "int(10) unsigned", False, None, "", 1),
            ColumnSchema("tiny", "tinyint(3) unsigned", False, None, "", 2),
            ColumnSchema("signed_col", "bigint(20)", False, None, "", 3),
        ),
        primary_keys=("big",),
    )
    assert unsigned_columns(schema) == (("big", 0xFFFFFFFFFFFFFFFF), ("mid", 0xFFFFFFFF), ("tiny", 0xFF))

    # the driver's signed interpretation is repaired, the signed column is kept
    assert fix_unsigned_values(schema, {"big": -1, "mid": -1, "tiny": -1, "signed_col": -1}) == {
        "big": 18446744073709551615,
        "mid": 4294967295,
        "tiny": 255,
        "signed_col": -1,
    }
    # already-correct values are untouched
    assert fix_unsigned_values(schema, {"big": 42}) == {"big": 42}
