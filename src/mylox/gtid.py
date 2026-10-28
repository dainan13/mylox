"""GTID set arithmetic.

MySQL represents the *executed* GTID set as a comma separated list of
``source_id:interval[:interval...]`` items, e.g.::

    3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:7:9-10,another-uuid:1-3

The naive implementation of a CDC checkpoint writes the GTID of the *current*
transaction (``uuid:123``) into a column named ``gtid_set`` and later feeds it
to ``auto_position``.  That tells MySQL "only transaction 123 was executed -
please resend 1..122", which either replays a huge amount of history or loses
its place entirely.

:class:`GtidSet` implements proper interval arithmetic so a checkpoint can
carry the real executed set (and grows in a compact form: a long lived stream
typically collapses into a handful of ranges per source id).
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from .errors import MyloxError

_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$")
_INTERVAL_RE = re.compile(r"^(\d+)(?:-(\d+))?$")

Interval = Tuple[int, int]


class GtidError(MyloxError, ValueError):
    """Raised for malformed GTID strings."""


def _add_interval(intervals: List[Interval], start: int, end: int) -> bool:
    """Merge ``[start, end]`` into a sorted, normalised interval list.

    Adjacent intervals (``1-5`` and ``6-9``) are merged as well, which is what
    MySQL itself does.  Returns ``True`` when the interval added new coverage.
    """
    if end < start:
        start, end = end, start

    added = True
    for low, high in intervals:
        if low <= start and end <= high:
            added = False
            break

    intervals.append((start, end))
    intervals.sort()
    merged: List[Interval] = []
    for low, high in intervals:
        if merged and low <= merged[-1][1] + 1:
            prev_low, prev_high = merged[-1]
            merged[-1] = (prev_low, max(prev_high, high))
        else:
            merged.append((low, high))
    intervals[:] = merged
    return added


class GtidSet:
    """A mutable, normalised set of GTIDs."""

    __slots__ = ("_intervals",)

    def __init__(self, value: Optional[object] = None) -> None:
        self._intervals: Dict[str, List[Interval]] = {}
        if value is None:
            return
        if isinstance(value, GtidSet):
            self._intervals = {sid: list(ivs) for sid, ivs in value._intervals.items()}
        elif isinstance(value, str):
            self.update(value)
        elif isinstance(value, Iterable):
            for item in value:
                self.add(str(item))
        else:  # pragma: no cover - defensive
            raise GtidError("cannot build a GtidSet from %r" % (value,))

    # -- construction -------------------------------------------------
    @classmethod
    def parse(cls, text: Optional[str]) -> "GtidSet":
        """Parse ``text``, returning an empty set for blank input."""
        return cls(text)

    def copy(self) -> "GtidSet":
        return GtidSet(self)

    # -- mutation -----------------------------------------------------
    def add(self, gtid: str) -> bool:
        """Add a single GTID (``uuid:number``). Returns ``True`` if it was new."""
        source_id, start, end = self.split_gtid(gtid)
        return self.add_interval(source_id, start, end)

    def add_interval(self, source_id: str, start: int, end: int) -> bool:
        if start < 0 or end < 0:
            raise GtidError("GTID intervals must be positive: %s:%s-%s" % (source_id, start, end))
        intervals = self._intervals.setdefault(source_id.strip().lower(), [])
        return _add_interval(intervals, start, end)

    def update(self, other: object) -> "GtidSet":
        """Merge ``other`` (a :class:`GtidSet`, a GTID string or an iterable)."""
        if isinstance(other, GtidSet):
            for source_id, intervals in other._intervals.items():
                target = self._intervals.setdefault(source_id, [])
                for start, end in intervals:
                    _add_interval(target, start, end)
            return self
        if isinstance(other, str):
            for item in self._split_items(other):
                source_id, intervals = self._parse_item(item)
                target = self._intervals.setdefault(source_id, [])
                for start, end in intervals:
                    _add_interval(target, start, end)
            return self
        if isinstance(other, Iterable):
            for item in other:
                self.add(str(item))
            return self
        raise GtidError("cannot merge %r into a GtidSet" % (other,))

    def clear(self) -> None:
        self._intervals.clear()

    # -- queries ------------------------------------------------------
    def __contains__(self, gtid: object) -> bool:
        if isinstance(gtid, GtidSet):
            return self.contains_all(gtid)
        try:
            source_id, start, end = self.split_gtid(str(gtid))
        except GtidError:
            return False
        return self.contains_interval(source_id, start, end)

    def contains_interval(self, source_id: str, start: int, end: int) -> bool:
        intervals = self._intervals.get(source_id.strip().lower())
        if not intervals:
            return False
        if end < start:
            start, end = end, start
        for low, high in intervals:
            if low <= start and end <= high:
                return True
        return False

    def contains_all(self, other: "GtidSet") -> bool:
        """``True`` when every GTID of ``other`` is present in ``self``."""
        for source_id, intervals in other._intervals.items():
            mine = self._intervals.get(source_id)
            if not mine:
                return False
            for start, end in intervals:
                if not self.contains_interval(source_id, start, end):
                    return False
        return True

    def is_empty(self) -> bool:
        return not self._intervals

    def __bool__(self) -> bool:
        return bool(self._intervals)

    def __iter__(self) -> Iterator[str]:
        for source_id, intervals in self._intervals.items():
            for start, end in intervals:
                if start == end:
                    yield "%s:%d" % (source_id, start)
                else:
                    yield "%s:%d-%d" % (source_id, start, end)

    def __len__(self) -> int:
        """Number of source ids in the set (not the number of transactions)."""
        return len(self._intervals)

    def __str__(self) -> str:
        return ",".join(
            "%s:%s" % (
                source_id,
                ":".join(
                    "%d-%d" % (start, end) if start != end else "%d" % start
                    for start, end in intervals
                ),
            )
            for source_id, intervals in self._intervals.items()
        )

    def __repr__(self) -> str:
        return "GtidSet(%r)" % (str(self),)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, GtidSet):
            return self._intervals == other._intervals
        if isinstance(other, str):
            return self._intervals == GtidSet(other)._intervals
        return NotImplemented

    def __hash__(self) -> int:  # pragma: no cover - sets are used readonly
        return hash(str(self))

    # -- helpers ------------------------------------------------------
    @staticmethod
    def _split_items(text: str) -> List[str]:
        items = [chunk.strip() for chunk in text.replace("\n", "").split(",")]
        return [item for item in items if item]

    @staticmethod
    def split_gtid(gtid: str) -> Tuple[str, int, int]:
        """Split ``uuid:number`` into its three components."""
        text = gtid.strip()
        if ":" not in text:
            raise GtidError("not a GTID: %r" % (gtid,))
        source_id, _, number = text.rpartition(":")
        source_id = source_id.strip().lower()
        if not _UUID_RE.match(source_id):
            raise GtidError("not a GTID source id: %r" % (source_id,))
        match = _INTERVAL_RE.match(number.strip())
        if not match:
            raise GtidError("not a GTID sequence number: %r" % (number,))
        start = int(match.group(1))
        end = int(match.group(2)) if match.group(2) else start
        return source_id, start, end

    @classmethod
    def _parse_item(cls, item: str) -> Tuple[str, List[Interval]]:
        parts = item.split(":")
        if len(parts) < 2:
            raise GtidError("malformed GTID set item: %r" % (item,))
        source_id = parts[0].strip().lower()
        if not _UUID_RE.match(source_id):
            raise GtidError("malformed GTID source id: %r" % (source_id,))
        intervals: List[Interval] = []
        for chunk in parts[1:]:
            chunk = chunk.strip()
            if not chunk:
                continue
            match = _INTERVAL_RE.match(chunk)
            if not match:
                raise GtidError("malformed GTID interval %r in %r" % (chunk, item))
            start = int(match.group(1))
            end = int(match.group(2)) if match.group(2) else start
            _add_interval(intervals, start, end)
        if not intervals:
            raise GtidError("GTID set item without intervals: %r" % (item,))
        return source_id, intervals

    @staticmethod
    def safe_parse(text: Optional[str]) -> "GtidSet":
        """Like :meth:`parse` but never raises: bad input yields an empty set."""
        try:
            return GtidSet(text)
        except GtidError:
            return GtidSet()


def intersection(sets: Iterable[GtidSet]) -> GtidSet:
    """Intersection of several GTID sets.

    Used when resuming in GTID mode: MySQL's ``auto_position`` replays every
    transaction *missing* from the supplied set, so the engine must supply the
    set consumed by **all** pipelines (the slowest consumer wins) and filter
    the already-consumed transactions per pipeline afterwards.
    """
    result: Optional[GtidSet] = None
    for item in sets:
        if result is None:
            result = item.copy()
            continue
        merged = GtidSet()
        for source_id, intervals in result._intervals.items():
            others = item._intervals.get(source_id)
            if not others:
                continue
            for start, end in intervals:
                for other_start, other_end in others:
                    low = max(start, other_start)
                    high = min(end, other_end)
                    if low <= high:
                        merged.add_interval(source_id, low, high)
        result = merged
        if result.is_empty():
            return result
    return result if result is not None else GtidSet()


def union(sets: Iterable[GtidSet]) -> GtidSet:
    """Union of several GTID sets."""
    result = GtidSet()
    for item in sets:
        result.update(item)
    return result
