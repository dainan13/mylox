"""Schema acquisition, evolution and lookup.

No shadow tables: the schema comes from a hybrid of ``information_schema``
snapshots and local sqlglot DDL interpretation.
"""

from __future__ import annotations

from .ddl import SqlglotDDLParser
from .evolve import apply_alter_ops, build_created_table
from .loader import InformationSchemaLoader
from .manager import DDLReport, SchemaManager
from .model import ColumnSchema, TableSchema, coerce_column, schemas_equal
from .plan import AlterOp, ColumnPosition, DDLChange, DDLKind, DDLPlan, OpKind
from .registry import PutOutcome, SchemaRegistry, SchemaVersion, TableTimeline
from .salvage import is_ddl_statement, looks_like_dml, parse_rename_table, salvage_table_refs, unquote

__all__ = [
    "AlterOp",
    "ColumnPosition",
    "ColumnSchema",
    "DDLChange",
    "DDLKind",
    "DDLPlan",
    "DDLReport",
    "InformationSchemaLoader",
    "OpKind",
    "PutOutcome",
    "SchemaManager",
    "SchemaRegistry",
    "SchemaVersion",
    "SqlglotDDLParser",
    "TableSchema",
    "TableTimeline",
    "apply_alter_ops",
    "build_created_table",
    "coerce_column",
    "is_ddl_statement",
    "looks_like_dml",
    "parse_rename_table",
    "salvage_table_refs",
    "unquote",
    "schemas_equal",
]
