"""Raw-text DDL salvage helpers.

sqlglot models most MySQL DDL, but a few very common statements still degrade
into an opaque ``exp.Command`` node (``RENAME TABLE a TO b, c TO d``,
``CREATE TABLE ... PARTITION BY ...``, ``ALTER TABLE ... CONVERT TO CHARACTER
SET ...``).  When that happens mylox falls back to ``information_schema`` - and
these helpers are what tell it *which* tables to re-read, or let it handle the
statement outright (``RENAME TABLE``).

Everything here is pure text processing: no sqlglot, no I/O, so it is trivial
to unit test.
"""

from __future__ import annotations

import re
from typing import List, Optional, Tuple

#: ``db.table`` or ``table``, with optional backticks.
TABLE_REF = r"(?:`[^`]+`|[A-Za-z_$][\w$]*)(?:\s*\.\s*(?:`[^`]+`|[A-Za-z_$][\w$]*))?"

_REF_PATTERNS: Tuple[Tuple[str, "re.Pattern[str]"], ...] = (
    (
        "create_table",
        re.compile(r"^\s*CREATE\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(" + TABLE_REF + r")", re.I),
    ),
    ("alter_table", re.compile(r"^\s*ALTER\s+TABLE\s+(" + TABLE_REF + r")", re.I)),
    (
        "drop_table",
        re.compile(
            r"^\s*DROP\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+EXISTS\s+)?((?:"
            + TABLE_REF
            + r")(?:\s*,\s*(?:"
            + TABLE_REF
            + r"))*)",
            re.I,
        ),
    ),
    ("truncate", re.compile(r"^\s*TRUNCATE\s+(?:TABLE\s+)?(" + TABLE_REF + r")", re.I)),
    (
        "drop_database",
        re.compile(r"^\s*DROP\s+(?:DATABASE|SCHEMA)\s+(?:IF\s+EXISTS\s+)?(`[^`]+`|[A-Za-z_$][\w$]*)", re.I),
    ),
    (
        "rename_table",
        re.compile(
            r"^\s*RENAME\s+TABLE\s+((?:"
            + TABLE_REF
            + r")\s+TO\s+(?:"
            + TABLE_REF
            + r")(?:\s*,\s*(?:"
            + TABLE_REF
            + r")\s+TO\s+(?:"
            + TABLE_REF
            + r"))*)",
            re.I,
        ),
    ),
    ("create_index", re.compile(r"^\s*CREATE\s+(?:UNIQUE\s+|FULLTEXT\s+|SPATIAL\s+)?INDEX\s+\S+\s+ON\s+(" + TABLE_REF + r")", re.I)),
    ("drop_index", re.compile(r"^\s*DROP\s+INDEX\s+\S+\s+ON\s+(" + TABLE_REF + r")", re.I)),
)

_RENAME_PAIR_RE = re.compile(r"(" + TABLE_REF + r")\s+TO\s+(" + TABLE_REF + r")", re.I)

#: Leading keywords that make a ``QueryEvent`` interesting for schema tracking.
_DDL_KEYWORDS = ("CREATE", "ALTER", "DROP", "RENAME", "TRUNCATE", "COMMENT")

_DML_KEYWORDS = ("INSERT", "UPDATE", "DELETE", "REPLACE", "LOAD DATA")


def is_ddl_statement(sql: str) -> bool:
    """Cheap check on the leading keyword (no parsing)."""
    stripped = (sql or "").lstrip()
    if not stripped:
        return False
    upper = stripped[:32].upper()
    return any(upper.startswith(keyword) for keyword in _DDL_KEYWORDS)


def looks_like_dml(sql: str) -> bool:
    upper = (sql or "").lstrip()[:16].upper()
    return any(upper.startswith(keyword) for keyword in _DML_KEYWORDS)


def split_ref(ref: str) -> Tuple[Optional[str], str]:
    """Split ``db.table`` / ``table`` (quotes removed)."""
    parts = [unquote(part) for part in ref.strip().split(".")]
    if len(parts) >= 2:
        return parts[-2], parts[-1] or ""
    return None, parts[0] or ""


def unquote(name: object) -> Optional[str]:
    """Normalise an identifier that may be a str or an object with ``.name``."""
    if name is None:
        return None
    identifier = getattr(name, "name", None)
    if isinstance(identifier, str) and identifier:
        return identifier
    text = str(name).strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in "`\"'":
        text = text[1:-1]
    return text.strip()


def salvage_table_refs(sql: str, default_db: Optional[str]) -> Tuple[Tuple[Optional[str], str], ...]:
    """Extract table references from raw DDL text (best effort)."""
    refs: List[Tuple[Optional[str], str]] = []
    for name, pattern in _REF_PATTERNS:
        match = pattern.match(sql or "")
        if not match:
            continue
        if name == "drop_database":
            # A database level statement does not name a table; the caller
            # expands it from the registry.
            break
        if name == "rename_table":
            for pair in _RENAME_PAIR_RE.finditer(match.group(1)):
                old_db, old_table = split_ref(pair.group(1))
                new_db, new_table = split_ref(pair.group(2))
                refs.append((old_db or default_db, old_table))
                refs.append((new_db or old_db or default_db, new_table))
        elif name == "drop_table":
            for item in match.group(1).split(","):
                db, table = split_ref(item)
                refs.append((db or default_db, table))
        else:
            db, table = split_ref(match.group(1))
            refs.append((db or default_db, table))
        break
    if not refs:
        # Last resort: every "db.table"-looking token.
        for token in re.findall(r"`([^`]+)`\.`([^`]+)`", sql or ""):
            refs.append((token[0], token[1]))
    return tuple(refs)


def parse_rename_table(sql: str, default_db: Optional[str]) -> Optional[List[Tuple[Optional[str], str, Optional[str], str]]]:
    """Parse ``RENAME TABLE a TO b [, c TO d]``.

    Returns ``(old_db, old_table, new_db, new_table)`` tuples, or ``None`` when
    the statement is not a ``RENAME TABLE``.
    """
    match = re.match(r"^\s*RENAME\s+TABLE\s+(.+)$", sql or "", re.I | re.S)
    if not match:
        return None
    body = match.group(1).strip().rstrip(";")
    pairs = list(_RENAME_PAIR_RE.finditer(body))
    if not pairs:
        return None
    renames = []
    for pair in pairs:
        old_db, old_table = split_ref(pair.group(1))
        new_db, new_table = split_ref(pair.group(2))
        renames.append((old_db or default_db, old_table, new_db or old_db or default_db, new_table))
    return renames
