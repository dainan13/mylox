"""Binlog coordinate helpers.

MySQL binlog files are named ``<prefix>.<number>`` (e.g. ``mysql-bin.000042``).
The original implementation compared them as plain strings, which breaks the
moment the numeric suffix grows a digit (``mysql-bin.999999`` sorts *after*
``mysql-bin.1000000``).  :func:`compare_positions` compares the numeric suffix
first and uses the file name only for the prefix.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Tuple

_NAME_RE = re.compile(r"^(?P<prefix>.*?)(?P<number>\d+)$")


def split_binlog_name(name: str) -> Tuple[str, int]:
    """Split a binlog file name into ``(prefix, numeric_suffix)``.

    Names without a numeric suffix are compared as ``(name, -1)`` so that
    ``foo`` sorts before ``foo.000001``.
    """
    if not name:
        return "", -1
    match = _NAME_RE.match(name)
    if not match:
        return name, -1
    return match.group("prefix"), int(match.group("number"))


def compare_positions(
    file1: Optional[str],
    pos1: Optional[int],
    file2: Optional[str],
    pos2: Optional[int],
) -> int:
    """Return -1/0/1 comparing two binlog coordinates.

    ``None``/empty coordinates sort *before* any real coordinate (they mean
    "the very beginning of the binlog").
    """
    if not file1 and not file2:
        return _cmp(pos1 or 0, pos2 or 0)
    if not file1:
        return -1
    if not file2:
        return 1

    prefix1, number1 = split_binlog_name(file1)
    prefix2, number2 = split_binlog_name(file2)
    if number1 != number2:
        return -1 if number1 < number2 else 1
    if prefix1 != prefix2:
        return -1 if prefix1 < prefix2 else 1
    return _cmp(pos1 or 0, pos2 or 0)


def _cmp(left: int, right: int) -> int:
    if left == right:
        return 0
    return -1 if left < right else 1


def is_pos_greater_or_equal(
    file1: Optional[str], pos1: Optional[int], file2: Optional[str], pos2: Optional[int]
) -> bool:
    """``True`` when ``(file1, pos1) >= (file2, pos2)``.

    Unlike a plain string comparison it looks at the numeric suffix of the file
    name, so ``mysql-bin.999999`` is not considered "after"
    ``mysql-bin.1000000``.
    """
    if not file1 or not file2:
        return False
    return compare_positions(file1, pos1, file2, pos2) >= 0


@dataclass(frozen=True)
class BinlogPosition:
    """An immutable, orderable binlog coordinate."""

    file: str
    pos: int

    def __post_init__(self) -> None:
        if not self.file:
            raise ValueError("binlog file name must not be empty")
        if self.pos is None or self.pos < 0:
            raise ValueError("binlog position must be a non-negative integer")

    # -- ordering -----------------------------------------------------
    @property
    def sort_key(self) -> Tuple[str, int, int]:
        prefix, number = split_binlog_name(self.file)
        return prefix, number, self.pos

    def __lt__(self, other: "BinlogPosition") -> bool:
        if not isinstance(other, BinlogPosition):
            return NotImplemented
        return self.sort_key < other.sort_key

    def __le__(self, other: "BinlogPosition") -> bool:
        if not isinstance(other, BinlogPosition):
            return NotImplemented
        return self.sort_key <= other.sort_key

    def __gt__(self, other: "BinlogPosition") -> bool:
        if not isinstance(other, BinlogPosition):
            return NotImplemented
        return self.sort_key > other.sort_key

    def __ge__(self, other: "BinlogPosition") -> bool:
        if not isinstance(other, BinlogPosition):
            return NotImplemented
        return self.sort_key >= other.sort_key

    def __str__(self) -> str:
        return "%s:%d" % (self.file, self.pos)

    def __repr__(self) -> str:
        return "BinlogPosition(%r, %d)" % (self.file, self.pos)

    # -- helpers ------------------------------------------------------
    @classmethod
    def parse(cls, text: str) -> "BinlogPosition":
        """Parse ``mysql-bin.000001:1234``."""
        file_part, _, pos_part = text.rpartition(":")
        if not file_part:
            raise ValueError("not a binlog position: %r" % (text,))
        return cls(file_part, int(pos_part))

    @classmethod
    def maybe(cls, file: Optional[str], pos: Optional[int]) -> Optional["BinlogPosition"]:
        """Build a position, returning ``None`` when either part is missing."""
        if not file or pos is None:
            return None
        return cls(file, int(pos))
