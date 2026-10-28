"""Interpret MySQL DDL statements with sqlglot.

This is the *main path* of schema evolution: no shadow tables, no round trip to
the source database - the DDL carried by the binlog ``QueryEvent`` is parsed
locally and turned into a :class:`~mylox.schema.plan.DDLPlan`.

The interpreter is deliberately conservative.  Whenever a statement (or a
single clause of it) cannot be interpreted with certainty, the affected table is
flagged with ``needs_fallback`` so :class:`~mylox.schema.manager.SchemaManager`
re-reads ``information_schema`` for exactly that table.  Safety beats
cleverness: a wrong in-memory schema silently corrupts every later payload.

Statements that never change the column layout (``CREATE INDEX``,
``TRUNCATE``, ``ALTER TABLE ... ENGINE=InnoDB``, view/trigger DDL, ...) are
reported as :data:`DDLKind.IGNORE`.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Sequence, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from ..logging_utils import get_logger
from .model import ColumnSchema, reindex
from .plan import AlterOp, ColumnPosition, DDLChange, DDLKind, DDLPlan
from .salvage import (
    is_ddl_statement,
    looks_like_dml,
    parse_rename_table,
    salvage_table_refs,
    split_ref,
    unquote,
)

logger = get_logger(__name__)

#: ``CREATE``/``DROP`` kinds that never touch a table's column layout.
_IGNORED_CREATE_KINDS = {
    "DATABASE",
    "SCHEMA",
    "VIEW",
    "INDEX",
    "FUNCTION",
    "PROCEDURE",
    "TRIGGER",
    "EVENT",
    "USER",
    "ROLE",
    "SEQUENCE",
    "TABLESPACE",
}
_IGNORED_DROP_KINDS = {
    "DATABASE",
    "SCHEMA",
    "VIEW",
    "INDEX",
    "FUNCTION",
    "PROCEDURE",
    "TRIGGER",
    "EVENT",
    "USER",
    "ROLE",
    "SEQUENCE",
    "TABLESPACE",
}

__all__ = [
    "SqlglotDDLParser",
    "is_ddl_statement",
    "looks_like_dml",
    "salvage_table_refs",
    "split_ref",
    "unquote",
]


class SqlglotDDLParser:
    """Parse a DDL statement into a :class:`DDLPlan`."""

    def __init__(self, dialect: str = "mysql") -> None:
        self.dialect = dialect
        #: Cache of parsed plans (DDL statements repeat after a restart).
        self._cache: Dict[str, DDLPlan] = {}
        self._cache_limit = 512

    # -- public API ---------------------------------------------------
    def is_ddl(self, sql: str) -> bool:
        return is_ddl_statement(sql)

    def parse(self, sql: str, default_db: Optional[str] = None) -> DDLPlan:
        if not sql or not sql.strip():
            return DDLPlan(statement=sql, default_db=default_db, is_ddl=False)

        statement = sql.strip()
        if not is_ddl_statement(statement):
            return DDLPlan(
                statement=statement,
                default_db=default_db,
                is_ddl=False,
                salvaged_refs=salvage_table_refs(statement, default_db),
            )

        cache_key = "%s\x00%s" % (default_db or "", statement)
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        plan = self._parse_uncached(statement, default_db)
        if len(self._cache) >= self._cache_limit:
            self._cache.clear()
        self._cache[cache_key] = plan
        return plan

    # -- internals ----------------------------------------------------
    def _parse_uncached(self, statement: str, default_db: Optional[str]) -> DDLPlan:
        try:
            expression = sqlglot.parse_one(statement, read=self.dialect)
        except SqlglotError as exc:
            logger.warning("sqlglot cannot parse DDL, falling back to information_schema: %s (%s)", statement, exc)
            return DDLPlan(
                statement=statement,
                default_db=default_db,
                error=str(exc),
                salvaged_refs=salvage_table_refs(statement, default_db),
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("unexpected sqlglot failure on %s: %r", statement, exc)
            return DDLPlan(
                statement=statement,
                default_db=default_db,
                error=repr(exc),
                salvaged_refs=salvage_table_refs(statement, default_db),
            )

        try:
            changes = self._dispatch(expression, statement, default_db)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("DDL interpretation failed for %s: %r", statement, exc)
            return DDLPlan(
                statement=statement,
                default_db=default_db,
                error=repr(exc),
                salvaged_refs=salvage_table_refs(statement, default_db),
            )

        return DDLPlan(
            statement=statement,
            default_db=default_db,
            changes=changes,
            salvaged_refs=salvage_table_refs(statement, default_db),
        )

    def _dispatch(self, expression: exp.Expression, statement: str, default_db: Optional[str]) -> List[DDLChange]:
        if isinstance(expression, exp.Create):
            return self._create(expression, default_db)
        if isinstance(expression, exp.Alter):
            return self._alter(expression, default_db)
        if isinstance(expression, exp.Drop):
            return self._drop(expression, default_db)
        if isinstance(expression, exp.TruncateTable):
            return [self._ignore(DDLKind.TRUNCATE, default_db)]
        if isinstance(expression, exp.Command):
            return self._command(expression, statement, default_db)
        if isinstance(expression, exp.RenameTable):  # future sqlglot support
            return self._alter_rename_node(expression, default_db)
        return [self._ignore(DDLKind.IGNORE, default_db, "unhandled statement type %s" % type(expression).__name__)]

    # -- CREATE -------------------------------------------------------
    def _create(self, expression: exp.Create, default_db: Optional[str]) -> List[DDLChange]:
        kind = str(expression.args.get("kind") or "").upper()
        target = expression.this

        if kind and kind not in ("TABLE",):
            if kind in _IGNORED_CREATE_KINDS:
                return [self._ignore(DDLKind.IGNORE, default_db, "CREATE %s" % kind)]
            return [self._ignore(DDLKind.IGNORE, default_db, "CREATE %s (unhandled kind)" % kind)]

        if isinstance(target, exp.Schema):
            table_node = target.this
            db, table = self._table_ref(table_node, default_db)
            columns, primary_keys, reason = self._columns_from_schema(target)
            if reason is not None:
                return [
                    DDLChange(kind=DDLKind.CREATE_TABLE, db=db, table=table, needs_fallback=True, reason=reason)
                ]
            return [
                DDLChange(
                    kind=DDLKind.CREATE_TABLE,
                    db=db,
                    table=table,
                    columns=columns,
                    primary_keys=primary_keys,
                )
            ]

        if not isinstance(target, exp.Table):
            return [self._ignore(DDLKind.IGNORE, default_db, "CREATE with unsupported target")]

        db, table = self._table_ref(target, default_db)
        like = self._like_source(expression, default_db)
        if like is not None:
            return [DDLChange(kind=DDLKind.CREATE_TABLE, db=db, table=table, like=like)]
        if expression.args.get("expression") is not None:
            return [
                DDLChange(
                    kind=DDLKind.CREATE_TABLE,
                    db=db,
                    table=table,
                    needs_fallback=True,
                    reason="CREATE TABLE ... AS SELECT",
                )
            ]
        return [
            DDLChange(
                kind=DDLKind.CREATE_TABLE,
                db=db,
                table=table,
                needs_fallback=True,
                reason="CREATE TABLE without column definitions",
            )
        ]

    @staticmethod
    def _like_source(expression: exp.Create, default_db: Optional[str]) -> Optional[Tuple[Optional[str], str]]:
        properties = expression.args.get("properties")
        if properties is None:
            return None
        candidates: Sequence[exp.Expression] = (
            properties.expressions if isinstance(properties, exp.Properties) else [properties]
        )
        for prop in candidates:
            if isinstance(prop, exp.LikeProperty):
                table = prop.args.get("this")
                if isinstance(table, exp.Table):
                    return (unquote(table.args.get("db")) or default_db, table.name)
        return None

    def _columns_from_schema(
        self, schema: exp.Schema
    ) -> Tuple[Tuple[ColumnSchema, ...], Tuple[str, ...], Optional[str]]:
        columns: List[ColumnSchema] = []
        primary_keys: List[str] = []
        for item in schema.expressions:
            if isinstance(item, exp.ColumnDef):
                columns.append(self._column_from_def(item, len(columns)))
                if self._has_inline_primary_key(item):
                    primary_keys.append(self._identifier_name(item.this))
            elif isinstance(item, exp.PrimaryKey):
                for key in item.expressions:
                    name = self._identifier_name(key)
                    if name:
                        primary_keys.append(name)
            # Indexes, unique keys, foreign keys and checks do not affect the
            # column layout.
        if not columns:
            return (), (), "CREATE TABLE without column definitions"
        return reindex(columns), tuple(primary_keys), None

    def _column_from_def(self, column_def: exp.ColumnDef, ordinal: int) -> ColumnSchema:
        name = self._identifier_name(column_def.this) or "?"
        data_type = column_def.args.get("kind")
        column_type = data_type.sql(dialect=self.dialect) if data_type is not None else ""

        nullable = True
        default: Optional[str] = None
        extra_parts: List[str] = []
        for constraint in column_def.args.get("constraints") or []:
            kind = constraint.args.get("kind")
            if kind is None:
                continue
            if isinstance(kind, exp.NotNullColumnConstraint):
                # ``NULL`` is modelled as NotNullColumnConstraint(allow_null=True).
                nullable = bool(kind.args.get("allow_null"))
            elif isinstance(kind, exp.DefaultColumnConstraint):
                default = self._normalize_default(kind.this)
            elif isinstance(kind, exp.AutoIncrementColumnConstraint):
                extra_parts.append("auto_increment")
            elif isinstance(kind, exp.OnUpdateColumnConstraint):
                extra_parts.append(kind.sql(dialect=self.dialect))
            elif isinstance(
                kind,
                (
                    exp.PrimaryKeyColumnConstraint,
                    exp.CommentColumnConstraint,
                    exp.CharacterSetColumnConstraint,
                    exp.CollateColumnConstraint,
                    exp.UniqueColumnConstraint,
                ),
            ):
                continue
            elif type(kind).__name__ in ("GeneratedAsIdentityColumnConstraint", "ComputedColumnConstraint"):
                extra_parts.append("generated")
        return ColumnSchema(
            name=name,
            column_type=column_type,
            nullable=nullable,
            default=default,
            extra=" ".join(extra_parts),
            ordinal=ordinal,
        )

    def _normalize_default(self, node: Optional[exp.Expression]) -> Optional[str]:
        """Render a ``DEFAULT`` the way ``information_schema`` reports it.

        ``DEFAULT 'x'`` -> ``x``, ``DEFAULT NULL`` -> ``None``,
        ``DEFAULT CURRENT_TIMESTAMP`` -> ``CURRENT_TIMESTAMP``.  Without this
        normalisation the DDL-derived schema and the ``information_schema``
        snapshot would always differ and every restart would record a redundant
        history row.
        """
        if node is None or isinstance(node, exp.Null):
            return None
        if isinstance(node, exp.Literal):
            return str(node.this)
        if isinstance(node, (exp.CurrentTimestamp, exp.CurrentDate, exp.CurrentTime, exp.CurrentDatetime)):
            return node.sql(dialect=self.dialect).replace("()", "")
        return node.sql(dialect=self.dialect)

    @staticmethod
    def _has_inline_primary_key(column_def: exp.ColumnDef) -> bool:
        for constraint in column_def.args.get("constraints") or []:
            kind = constraint.args.get("kind")
            if isinstance(kind, exp.PrimaryKeyColumnConstraint):
                return True
            if type(kind).__name__ == "PrimaryKeyColumnConstraint":
                return True
            if isinstance(constraint, exp.PrimaryKeyColumnConstraint):
                return True
        return False

    # -- ALTER --------------------------------------------------------
    def _alter(self, expression: exp.Alter, default_db: Optional[str]) -> List[DDLChange]:
        table_node = expression.this
        if not isinstance(table_node, exp.Table):
            return [self._ignore(DDLKind.IGNORE, default_db, "ALTER without table target")]
        db, table = self._table_ref(table_node, default_db)

        ops: List[AlterOp] = []
        renames: List[DDLChange] = []
        fallback_reason: Optional[str] = None

        for action in expression.args.get("actions") or []:
            if isinstance(action, exp.ColumnDef):
                ops.append(
                    AlterOp.add_column(
                        self._column_from_def(action, 0),
                        position=self._position(action),
                        ignore_if_exists=bool(action.args.get("exists")),
                    )
                )
            elif isinstance(action, exp.DropPrimaryKey) or (
                isinstance(action, exp.Drop) and str(action.args.get("kind") or "").upper() == "PRIMARY KEY"
            ):
                ops.append(AlterOp.drop_primary_key())
            elif isinstance(action, exp.Drop):
                drop_kind = str(action.args.get("kind") or "").upper()
                if drop_kind == "COLUMN":
                    for column in action.args.get("tables") or []:
                        name = self._identifier_name(column)
                        if name:
                            ops.append(AlterOp.drop_column(name, ignore_if_missing=bool(action.args.get("exists"))))
                elif drop_kind in ("INDEX", "KEY", "FOREIGN KEY", "CONSTRAINT", "PARTITION", "CHECK"):
                    continue
                elif drop_kind == "":
                    # ``ALTER TABLE t DROP col`` without the COLUMN keyword.
                    for column in action.args.get("tables") or []:
                        name = self._identifier_name(column)
                        if name:
                            ops.append(AlterOp.drop_column(name, ignore_if_missing=bool(action.args.get("exists"))))
                else:
                    fallback_reason = fallback_reason or "unhandled ALTER action DROP %s" % drop_kind
            elif isinstance(action, exp.ModifyColumn):
                column_def = action.args.get("this")
                if not isinstance(column_def, exp.ColumnDef):
                    fallback_reason = fallback_reason or "MODIFY COLUMN without a column definition"
                    continue
                previous = self._identifier_name(action.args.get("rename_from"))
                ops.append(
                    AlterOp.modify_column(
                        self._column_from_def(column_def, 0),
                        previous_name=previous,
                        position=self._position(column_def),
                    )
                )
            elif isinstance(action, exp.RenameColumn):
                old_name = self._identifier_name(action.this)
                new_name = self._identifier_name(action.args.get("to"))
                if not old_name or not new_name:
                    fallback_reason = fallback_reason or "RENAME COLUMN without names"
                    continue
                ops.append(AlterOp.rename_column(old_name, new_name, ignore_if_missing=bool(action.args.get("exists"))))
            elif isinstance(action, exp.AddConstraint):
                primary = self._primary_key_from_constraint(action)
                if primary is None:
                    # Index/unique/foreign key/check: layout unchanged.
                    continue
                ops.append(AlterOp.add_primary_key(primary))
            elif isinstance(action, exp.AlterRename):
                new_node = action.args.get("this")
                if not isinstance(new_node, exp.Table):
                    fallback_reason = fallback_reason or "RENAME TO without a table"
                    continue
                new_db, new_table = self._table_ref(new_node, db or default_db)
                renames.append(
                    DDLChange(
                        kind=DDLKind.RENAME_TABLE,
                        db=db,
                        table=table,
                        new_db=new_db,
                        new_table=new_table,
                    )
                )
            elif isinstance(action, (exp.AlterColumn, exp.SetProperty, exp.AddPartition, exp.DropPartition)):
                continue
            else:
                fallback_reason = fallback_reason or "unhandled ALTER action %s" % type(action).__name__

        changes: List[DDLChange] = []
        if ops:
            changes.append(
                DDLChange(
                    kind=DDLKind.ALTER_TABLE,
                    db=db,
                    table=table,
                    ops=tuple(ops),
                    needs_fallback=fallback_reason is not None,
                    reason=fallback_reason,
                )
            )
        changes.extend(renames)
        if not changes:
            reason = fallback_reason or "no column-affecting clause"
            changes.append(self._ignore(DDLKind.IGNORE, default_db, "ALTER TABLE: %s" % reason))
        return changes

    def _primary_key_from_constraint(self, action: exp.AddConstraint) -> Optional[Tuple[str, ...]]:
        expressions = list(action.args.get("expressions") or [])
        if not expressions and action.args.get("expression") is not None:
            expressions = [action.args["expression"]]
        for item in expressions:
            node = item
            if isinstance(node, exp.Constraint):
                inner = node.args.get("expressions") or []
                if len(inner) == 1:
                    node = inner[0]
                else:
                    continue
            if isinstance(node, exp.PrimaryKey):
                names = [self._identifier_name(expr) for expr in node.expressions]
                return tuple(name for name in names if name)
            if isinstance(node, exp.ForeignKey):
                continue
        return None

    def _position(self, column_def: exp.ColumnDef) -> Optional[ColumnPosition]:
        position = column_def.args.get("position")
        if position is None:
            return None
        if isinstance(position, exp.ColumnPosition):
            if str(position.args.get("position") or "").upper() == "FIRST":
                return ColumnPosition.first_position()
            name = self._identifier_name(position.args.get("this"))
            return ColumnPosition.after_column(name) if name else None
        text = position.sql(dialect=self.dialect).upper() if isinstance(position, exp.Expression) else str(position).upper()
        if text.startswith("FIRST"):
            return ColumnPosition.first_position()
        match = re.match(r"AFTER\s+`?([\w$]+)`?", text)
        if match:
            return ColumnPosition.after_column(match.group(1))
        return None

    # -- DROP ---------------------------------------------------------
    def _drop(self, expression: exp.Drop, default_db: Optional[str]) -> List[DDLChange]:
        kind = str(expression.args.get("kind") or "").upper()
        nodes = list(expression.args.get("tables") or expression.args.get("expressions") or [])

        if kind == "DATABASE" or kind == "SCHEMA":
            databases = []
            for node in nodes:
                name = self._identifier_name(node) or self._identifier_name(getattr(node, "this", None))
                if name:
                    databases.append(name)
            if not databases:
                return [self._ignore(DDLKind.IGNORE, default_db, "DROP DATABASE")]
            return [DDLChange(kind=DDLKind.DROP_DATABASE, database=name) for name in databases]

        if kind and kind not in ("TABLE", "TEMPORARY TABLE"):
            if kind in _IGNORED_DROP_KINDS:
                return [self._ignore(DDLKind.IGNORE, default_db, "DROP %s" % kind)]
            return [self._ignore(DDLKind.IGNORE, default_db, "DROP %s (unhandled kind)" % kind)]

        changes = []
        for node in nodes:
            db, table = self._table_ref(node, default_db)
            if table:
                changes.append(DDLChange(kind=DDLKind.DROP_TABLE, db=db, table=table))
        if not changes:
            return [self._ignore(DDLKind.IGNORE, default_db, "DROP TABLE without tables")]
        return changes

    # -- Command (sqlglot could not model the statement) --------------
    def _command(self, expression: exp.Command, statement: str, default_db: Optional[str]) -> List[DDLChange]:
        renames = parse_rename_table(statement, default_db)
        if renames:
            return [
                DDLChange(
                    kind=DDLKind.RENAME_TABLE,
                    db=old_db,
                    table=old_table,
                    new_db=new_db,
                    new_table=new_table,
                )
                for old_db, old_table, new_db, new_table in renames
            ]

        table = expression.args.get("this")
        body = expression.args.get("expression")
        body_text = str(body) if body is not None else ""
        text = "%s %s" % (table or "", body_text)

        if re.match(r"^\s*ALTER\s+TABLE\b", statement, re.I) and re.search(
            r"\bCONVERT\s+TO\s+CHARACTER\s+SET\b", statement, re.I
        ):
            refs = salvage_table_refs(statement, default_db)
            return [
                DDLChange(
                    kind=DDLKind.IGNORE,
                    db=refs[0][0] if refs else default_db,
                    table=refs[0][1] if refs else None,
                    reason="ALTER TABLE ... CONVERT TO CHARACTER SET does not change column layout",
                )
            ]

        refs = salvage_table_refs(statement, default_db)
        if not refs:
            return [self._ignore(DDLKind.IGNORE, default_db, "unsupported statement: %s" % text)]
        return [
            DDLChange(
                kind=self._salvaged_kind(statement),
                db=db,
                table=table_name,
                needs_fallback=True,
                reason="sqlglot could not model %s" % text.strip(),
            )
            for db, table_name in refs
        ]

    @staticmethod
    def _salvaged_kind(statement: str) -> DDLKind:
        upper = statement.lstrip()[:24].upper()
        if upper.startswith("CREATE"):
            return DDLKind.CREATE_TABLE
        if upper.startswith("DROP"):
            return DDLKind.DROP_TABLE
        if upper.startswith("ALTER"):
            return DDLKind.ALTER_TABLE
        return DDLKind.UNKNOWN

    # -- helpers ------------------------------------------------------
    def _alter_rename_node(self, expression: exp.Expression, default_db: Optional[str]) -> List[DDLChange]:  # pragma: no cover
        return [self._ignore(DDLKind.IGNORE, default_db, "RENAME TABLE node")]

    @staticmethod
    def _ignore(kind: DDLKind, default_db: Optional[str], reason: Optional[str] = None) -> DDLChange:
        return DDLChange(kind=kind, db=default_db, table=None, reason=reason)

    @staticmethod
    def _identifier_name(node: Optional[exp.Expression]) -> Optional[str]:
        if node is None:
            return None
        if isinstance(node, exp.Identifier):
            return node.name
        if isinstance(node, exp.Column):
            return node.name
        if isinstance(node, exp.Ordered):
            return SqlglotDDLParser._identifier_name(node.this)
        if isinstance(node, exp.Table):
            return node.name
        name = getattr(node, "name", None)
        if name:
            return str(name)
        return unquote(str(node)) or None

    @staticmethod
    def _table_ref(node: exp.Expression, default_db: Optional[str]) -> Tuple[Optional[str], str]:
        if isinstance(node, exp.Table):
            return (unquote(node.args.get("db")) or default_db, node.name or "")
        if isinstance(node, exp.Column):
            return (unquote(node.args.get("table")) or default_db, node.name or "")
        return (default_db, unquote(str(node)) or "")
