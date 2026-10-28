"""Declarative table routing.

Two rule dialects are supported:

``FilterMode.GITIGNORE`` (default)
    Rules are *ignore* patterns, evaluated in order; the **last** matching rule
    wins and a leading ``!`` re-includes:

    .. code-block:: python

        TableFilter(["arch*", "!arch2018.keep_me"])
        # arch2018.keep_me -> allowed, every other arch* table -> denied

    With no matching rule the table is allowed.

``FilterMode.INCLUDE``
    Rules are *include* patterns and a leading ``!`` excludes - convenient when
    you would rather whitelist a few tables:

    .. code-block:: python

        TableFilter(["cinema_base.*", "!cinema_base.sys_log"], mode="include")
        # only cinema_base.* except sys_log

A rule matches when it matches either ``db.table`` or the bare ``table`` name
(so ``["*.log"]`` filters the table ``log`` in every schema).  Matching uses
:func:`fnmatch.fnmatchcase`, i.e. it is case sensitive and platform
independent.
"""

from __future__ import annotations

import fnmatch
from enum import Enum
from typing import Iterable, List, Optional, Sequence, Tuple


class FilterMode(str, Enum):
    GITIGNORE = "gitignore"
    INCLUDE = "include"


class TableFilter:
    """Decide whether a ``(schema, table)`` pair should be replicated."""

    def __init__(
        self,
        rules: Optional[Iterable[str]] = None,
        mode: object = FilterMode.GITIGNORE,
        default: Optional[bool] = None,
    ) -> None:
        self.mode = FilterMode(mode) if not isinstance(mode, FilterMode) else mode
        self._rules: List[Tuple[bool, str]] = []  # (negated, pattern)
        for raw in rules or []:
            pattern = (raw or "").strip()
            if not pattern or pattern.startswith("#"):
                continue
            negated = pattern.startswith("!")
            if negated:
                pattern = pattern[1:].strip()
            if pattern:
                self._rules.append((negated, pattern))

        if default is None:
            if self.mode is FilterMode.GITIGNORE:
                self.default = True
            else:
                self.default = not self._rules
        else:
            self.default = bool(default)

    # -- public API ---------------------------------------------------
    @property
    def rules(self) -> Tuple[str, ...]:
        """The parsed rules, with ``!`` preserved."""
        return tuple(("!" if negated else "") + pattern for negated, pattern in self._rules)

    def is_allowed(self, db: str, table: str) -> bool:
        return self.decision(db, table)[0]

    def decision(self, db: str, table: str) -> Tuple[bool, Optional[str]]:
        """Return ``(allowed, matching_rule)``."""
        allowed = self.default
        matched: Optional[str] = None
        full_name = "%s.%s" % (db, table)
        for negated, pattern in self._rules:
            if not (fnmatch.fnmatchcase(full_name, pattern) or fnmatch.fnmatchcase(table, pattern)):
                continue
            matched = ("!" if negated else "") + pattern
            # gitignore: a plain pattern ignores, ``!`` re-includes.
            # include:   a plain pattern includes, ``!`` excludes.
            allowed = negated if self.mode is FilterMode.GITIGNORE else not negated
        return allowed, matched

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return "TableFilter(mode=%r, rules=%r, default=%r)" % (
            self.mode.value,
            list(self.rules),
            self.default,
        )


def coerce_filter(
    value: object,
    mode: object = FilterMode.GITIGNORE,
    default: Optional[bool] = None,
) -> TableFilter:
    """Accept a :class:`TableFilter`, an iterable of rules or ``None``."""
    if isinstance(value, TableFilter):
        return value
    if value is None or isinstance(value, (str, bytes)):
        rules: Sequence[str] = [] if not value else [value]  # type: ignore[list-item]
        return TableFilter(rules, mode=mode, default=default)
    return TableFilter(value, mode=mode, default=default)  # type: ignore[arg-type]
