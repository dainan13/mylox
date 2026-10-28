"""Helpers for decoding ``RowsEvent`` payloads.

``pymysqlreplication`` returns a mapping for *every* column of the table map,
including the ones that are absent from the current row image (their value is
``None`` or an empty placeholder).  The ``columns_present_bitmap`` carried by
the event is the only reliable way to know which columns are really present,
so it must always be applied before the payload reaches a handler.

Column keys are either:

* ``UNKNOWN_COL<i>`` - when the binlog carries no column names (the default,
  ``use_column_name_cache=False``), or
* the real column name - when MySQL was started with
  ``binlog_row_metadata=FULL``.

Both shapes, and both ``binlog_row_image`` modes (``FULL``/``MINIMAL``), are
handled here.
"""

from __future__ import annotations

import re
from typing import Callable, Dict, Optional, Sequence

_UNKNOWN_RE = re.compile(r"^UNKNOWN_COL(\d+)$")


def column_index_for_key(key: object, col_names: Sequence[str]) -> Optional[int]:
    """Map a payload key to a column index of ``col_names``."""
    if not isinstance(key, str):
        return None
    match = _UNKNOWN_RE.match(key)
    if match:
        return int(match.group(1))
    try:
        return list(col_names).index(key)
    except ValueError:
        return None


def is_column_present_in_bitmap(bitmap: object, col_idx: int) -> bool:
    """``True`` when column ``col_idx`` is set in a present-image bitmap.

    ``bitmap`` may be ``bytes`` (the wire format, little-endian bit order) or a
    sequence of booleans (handy in tests).  ``None`` means "no bitmap
    information", in which case every column is considered present.
    """
    if bitmap is None:
        return True
    if col_idx < 0:
        return False
    if isinstance(bitmap, (bytes, bytearray)):
        byte_index = col_idx // 8
        bit_index = col_idx % 8
        if byte_index < len(bitmap):
            return bool((bitmap[byte_index] >> bit_index) & 1)
        return False
    if isinstance(bitmap, (list, tuple)):
        if col_idx < len(bitmap):
            return bool(bitmap[col_idx])
        return False
    return True


def remap_columns_with_bitmap(
    row_dict: Optional[Dict[str, object]],
    col_names: Sequence[str],
    bitmap: object = None,
    on_unknown: Optional[Callable[[str, Optional[int]], None]] = None,
) -> Dict[str, object]:
    """Rename ``UNKNOWN_COL<i>`` keys and drop columns absent from the image.

    ``on_unknown(key, index)`` is invoked for every key that cannot be mapped
    onto ``col_names`` (schema drift); the value is preserved under its
    original key so no data is silently lost.
    """
    if not row_dict or not col_names:
        return {}

    names = list(col_names)
    result: Dict[str, object] = {}
    for key, value in row_dict.items():
        col_idx = column_index_for_key(key, names)
        if col_idx is not None and col_idx < len(names):
            col_name = names[col_idx]
        else:
            col_name = key
            if on_unknown is not None:
                on_unknown(key, col_idx)
        if col_idx is not None and bitmap is not None:
            if not is_column_present_in_bitmap(bitmap, col_idx):
                continue
        result[col_name] = value
    return result


def bitmap_expected_columns(bitmap: object) -> Optional[int]:
    """How many columns a ``bytes`` bitmap can describe (``None`` if unknown)."""
    if isinstance(bitmap, (bytes, bytearray)):
        return len(bitmap) * 8
    if isinstance(bitmap, (list, tuple)):
        return len(bitmap)
    return None


def bitmap_covers(bitmap: object, column_count: int) -> bool:
    """``True`` when ``bitmap`` is long enough for ``column_count`` columns."""
    expected = bitmap_expected_columns(bitmap)
    if expected is None:
        return True
    return expected >= column_count
