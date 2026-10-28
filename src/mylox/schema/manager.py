"""Schema acquisition and evolution - the heart of mylox.

There are no shadow tables any more.  The schema is obtained in a hybrid way:

1. **Initial snapshot** - one ``information_schema`` query against the replica
   at the initialization anchor; every table is written to
   ``mylox_schema_history`` (source ``bootstrap``).
2. **DDL evolution, main path** - the DDL statement carried by the binlog
   ``QueryEvent`` is parsed locally with sqlglot and applied to the in-memory
   timeline (source ``ddl_sqlglot``).
3. **DDL evolution, fallback** - when sqlglot cannot parse the statement, or
   the parsed result looks incomplete (``CREATE TABLE ... AS SELECT``, an
   unknown ``ALTER`` clause, a column that is not in the model, ...), a single
   ``information_schema`` query is issued for the affected tables only
   (source ``ddl_fallback``).

Both DDL paths write ``mylox_schema_history``, which is what lets a lagging
pipeline resolve the schema of a position older than the current snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple

from ..config import EngineConfig
from ..constants import (
    SOURCE_BOOTSTRAP,
    SOURCE_DDL_FALLBACK,
    SOURCE_DDL_FALLBACK_APPROX,
    SOURCE_DDL_SQLGLOT,
)
from ..errors import IncompleteDDL, SchemaNotFound
from ..filtering import TableFilter
from ..logging_utils import get_logger
from ..state.models import SchemaHistoryRecord
from ..utils.positions import BinlogPosition
from .ddl import SqlglotDDLParser
from .evolve import apply_alter_ops, build_created_table
from .loader import InformationSchemaLoader
from .model import TableSchema, schemas_equal
from .plan import DDLChange, DDLKind, DDLPlan
from .registry import PutOutcome, SchemaRegistry

logger = get_logger(__name__)

Ref = Tuple[Optional[str], str]


@dataclass
class DDLReport:
    """What happened while applying one DDL statement."""

    statement: str
    mode: str  # sqlglot | fallback | ignore | unhandled | error
    tables: List[str]
    reason: Optional[str] = None
    position: Optional[BinlogPosition] = None
    changed: bool = False
    recorded: int = 0

    @property
    def recorded_history(self) -> bool:
        return self.recorded > 0


class SchemaManager:
    """Owns the schema registry and every source of schema information."""

    def __init__(
        self,
        registry: SchemaRegistry,
        loader: InformationSchemaLoader,
        parser: SqlglotDDLParser,
        store: object,
        table_filter: TableFilter,
        config: Optional[EngineConfig] = None,
    ) -> None:
        self.registry = registry
        self.loader = loader
        self.parser = parser
        self.store = store
        self.filter = table_filter
        self.config = config or EngineConfig()
        self.namespace = self.config.namespace or "default"
        self._history_loaded: Set[Tuple[str, str]] = set()

    # -- public API ----------------------------------------------------
    def bind_namespace(self, namespace: str) -> None:
        self.namespace = namespace

    def bootstrap(self, position: BinlogPosition, gtid: Optional[str] = None) -> int:
        """Pull the whole filtered schema from ``information_schema``.

        Returns the number of history rows written.  Redundant rows are skipped
        by comparing against the newest history record of each table, so
        restarting the engine frequently does not bloat the history table.
        """
        schemas = self.loader.load_all()
        self.registry.mark_baseline(position)

        known: Dict[Tuple[str, str], TableSchema] = {}
        if self._history_enabled():
            try:
                known = {
                    key: record.schema for key, record in self._store().load_latest_schemas().items()
                }
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("could not read the latest schema history, recording the full snapshot: %s", exc)

        records = []
        for schema in schemas:
            if not self.filter.is_allowed(schema.db, schema.table):
                continue
            _, outcome = self.registry.put(schema, position, source=SOURCE_BOOTSTRAP, gtid=gtid)
            logger.debug("bootstrap %s (%s)", schema.name, outcome.value)
            if self._history_enabled() and not schemas_equal(known.get((schema.db, schema.table)), schema):
                records.append(self._record(schema, position, SOURCE_BOOTSTRAP, None, gtid))
        written = self._write(records)
        logger.info(
            "Schema bootstrap at %s: %d tables loaded, %d history rows written",
            position,
            len(schemas),
            written,
        )
        if not schemas:
            total = 0
            try:
                total = self.loader.count_base_tables()
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("could not count the source tables: %s", exc)
            if total:
                # Tables exist but the rules excluded all of them: by far the
                # most common cause is a rule written in the other dialect
                # (gitignore mode *ignores* plain patterns, include mode includes
                # them), and the symptom is otherwise "nothing is replicated".
                logger.warning(
                    "no table matched the filter rules, although the source has %d user table(s): "
                    "mode=%s rules=%s - if that is unexpected, check the dialect (gitignore mode "
                    "ignores plain patterns and uses '!' to re-include); tables created later are "
                    "still picked up through their DDL",
                    total,
                    self.filter.mode.value,
                    list(self.filter.rules),
                )
            else:
                logger.info(
                    "the source has no user tables yet (filter mode=%s rules=%s); tables created later "
                    "are still picked up through their DDL",
                    self.filter.mode.value,
                    list(self.filter.rules) or "<all tables>",
                )
        return written

    def handle_ddl(
        self,
        statement: str,
        position: BinlogPosition,
        default_db: Optional[str] = None,
        gtid: Optional[str] = None,
    ) -> DDLReport:
        """Apply one DDL statement to the schema model."""
        plan = self.parser.parse(statement, default_db=default_db)

        if not plan.is_ddl:
            return DDLReport(statement=statement, mode="ignore", tables=[], position=position)
        if plan.error or plan.needs_fallback:
            reason = plan.error or next(
                (change.reason for change in plan.changes if change.needs_fallback), "incomplete DDL"
            )
            return self._fallback(plan, position, gtid, reason)
        if plan.ignored:
            reason = plan.changes[0].reason if plan.changes else None
            return DDLReport(
                statement=statement, mode="ignore", tables=[], reason=reason, position=position
            )

        records: List[object] = []
        tables: List[str] = []
        changed = False
        for change in plan.changes:
            try:
                applied = self._apply_change(change, plan, position, gtid)
            except IncompleteDDL as exc:
                logger.info(
                    "DDL on %s could not be applied locally (%s); falling back to information_schema",
                    change.target,
                    exc,
                )
                return self._fallback(plan, position, gtid, str(exc))
            if applied is None:
                continue
            applied_records, applied_changed = applied
            tables.append(change.target)
            changed = changed or applied_changed
            records.extend(applied_records)

        recorded = self._write(records)
        logger.info(
            "DDL applied via sqlglot at %s [%s]: %s (%s)",
            position,
            SOURCE_DDL_SQLGLOT,
            statement.strip()[:200],
            ", ".join(tables) or "no schema change",
        )
        return DDLReport(
            statement=statement,
            mode="sqlglot",
            tables=tables,
            position=position,
            changed=changed,
            recorded=recorded,
        )

    def schema_for(
        self,
        db: str,
        table: str,
        position: Optional[BinlogPosition] = None,
        required: bool = True,
    ) -> Optional[TableSchema]:
        """Schema to use for a row event at ``position``.

        Resolution order:

        1. the in-memory timeline (fast path, no I/O);
        2. if the position predates the bootstrap snapshot, the persisted
           history is loaded **once per table** into the timeline.
        """
        schema = self.registry.schema_at(db, table, position)

        if schema is None and self._needs_history(db, table, position):
            self._load_history(db, table)
            schema = self.registry.schema_at(db, table, position)

        if schema is None and position is None:
            schema = self.registry.latest_schema(db, table)

        if schema is None and required:
            raise SchemaNotFound(
                "no schema known for %s.%s at %s" % (db, table, position or "<latest>")
            )
        return schema

    def refresh_tables(
        self,
        refs: Sequence[Ref],
        position: BinlogPosition,
        gtid: Optional[str] = None,
        allow_pre_baseline: bool = False,
    ) -> List[object]:
        """Force a re-read of specific tables from ``information_schema``.

        ``information_schema`` always describes *now*.  Stamping that state at a
        position older than the bootstrap snapshot would decode later-replayed
        row events with the wrong column layout (values landing in the wrong
        column) - so it is refused unless the caller explicitly accepts the
        approximation (the DDL fallback path, which labels it
        ``ddl_fallback_approx``).
        """
        if not allow_pre_baseline and self.is_pre_baseline(position):
            logger.error(
                "refusing to apply the live schema of %s at %s: the position is older than the "
                "bootstrap snapshot %s and no usable history exists. Reset the affected pipeline "
                "checkpoint so that it is re-initialized at the current anchor.",
                ", ".join("%s.%s" % (db, table) for db, table in refs),
                position,
                self.registry.baseline,
            )
            return []
        source = self._fallback_source(position)
        records: List[object] = []
        wanted = [(db, table) for db, table in refs if db and table]
        if not wanted:
            return records

        schemas = {schema.name: schema for schema in self.loader.load_tables(wanted)}
        for db, table in wanted:
            schema = schemas.get("%s.%s" % (db, table))
            if schema is None:
                # The table does not exist (anymore) - the DDL probably dropped
                # or renamed it.
                if self.registry.drop(db, table, position):
                    logger.info("table %s.%s no longer exists, marked dropped at %s", db, table, position)
                continue
            if not self.filter.is_allowed(schema.db, schema.table):
                logger.debug("refreshed table %s is filtered out", schema.name)
                continue
            version, outcome = self.registry.put(
                schema, position, source=source, gtid=gtid, ddl=None
            )
            if outcome is not PutOutcome.UNCHANGED:
                records.append(self._record(schema, position, source, None, gtid))
        return records

    def is_pre_baseline(self, position: Optional[BinlogPosition]) -> bool:
        """``True`` when ``position`` predates the bootstrap snapshot."""
        baseline = self.registry.baseline
        return baseline is not None and position is not None and position < baseline

    def refresh_tables_and_record(
        self,
        refs: Sequence[Ref],
        position: BinlogPosition,
        gtid: Optional[str] = None,
        allow_pre_baseline: bool = False,
    ) -> int:
        """Refresh tables and persist whatever changed (used by the engine).

        Called from the row path when a table is missing from the model or the
        event column count does not match.  Bounded to one round trip per table
        per process thanks to the registry lookup.
        """
        records = self.refresh_tables(refs, position, gtid, allow_pre_baseline=allow_pre_baseline)
        return self._write(records)

    # -- change application --------------------------------------------
    def _apply_change(
        self,
        change: DDLChange,
        plan: DDLPlan,
        position: BinlogPosition,
        gtid: Optional[str],
    ) -> Optional[Tuple[List[object], bool]]:
        """Apply one change.

        Returns ``(history_records, changed)``, or ``None`` when the change is a
        no-op / filtered out.
        """
        if change.kind is DDLKind.IGNORE or change.kind is DDLKind.TRUNCATE:
            return None

        if change.kind is DDLKind.DROP_DATABASE:
            database = change.database
            if not database:
                return None
            dropped = self.registry.drop_database(database, position)
            logger.info("database %s dropped at %s (%d tables)", database, position, len(dropped))
            return [], bool(dropped)

        db, table = change.db, change.table
        if not db or not table:
            raise IncompleteDDL("DDL change without a table reference")

        if change.kind is DDLKind.DROP_TABLE:
            return [], self.registry.drop(db, table, position)

        if change.kind is DDLKind.RENAME_TABLE:
            new_db = change.new_db or db
            new_table = change.new_table
            if not new_table:
                raise IncompleteDDL("RENAME TABLE without a target name")
            if self.registry.has_version_at(new_db, new_table, position):
                # The same RENAME was already applied at this exact position
                # (restart / reconnect replay): nothing to do.
                return [], False
            result = self.registry.rename(
                db, table, new_db, new_table, position, source=SOURCE_DDL_SQLGLOT, ddl=plan.statement, gtid=gtid
            )
            if result is None:
                raise IncompleteDDL("cannot rename %s.%s: unknown schema" % (db, table))
            version, _ = result
            return [self._record(version.schema, position, SOURCE_DDL_SQLGLOT, plan.statement, gtid)], True

        if change.kind is DDLKind.CREATE_TABLE:
            if change.columns is None:
                if change.like is not None:
                    source_db, source_table = change.like
                    source = self.registry.latest_schema(source_db or db, source_table)
                    if source is None:
                        raise IncompleteDDL(
                            "CREATE TABLE ... LIKE %s.%s: the source schema is unknown"
                            % (source_db or db, source_table)
                        )
                    schema = source.renamed(db, table)
                else:
                    raise IncompleteDDL("CREATE TABLE without columns")
            else:
                schema = build_created_table(db, table, change.columns, change.primary_keys)
            if not self.filter.is_allowed(db, table):
                return None
            _, outcome = self.registry.put(
                schema, position, source=SOURCE_DDL_SQLGLOT, ddl=plan.statement, gtid=gtid
            )
            if outcome is PutOutcome.UNCHANGED:
                return [], False
            return [self._record(schema, position, SOURCE_DDL_SQLGLOT, plan.statement, gtid)], True

        if change.kind is DDLKind.ALTER_TABLE:
            base = self.registry.schema_before(db, table, position)
            if base is None:
                # Might be a table that existed before the bootstrap snapshot
                # and was never recorded.
                base = self._history_schema_before(db, table, position)
            if base is None:
                raise IncompleteDDL("no base schema for %s.%s" % (db, table))
            evolved = apply_alter_ops(base, change.ops)
            if not self.filter.is_allowed(db, table):
                return None
            if schemas_equal(evolved, base):
                # Nothing column-related changed (e.g. ALGORITHM/ENGINE only).
                return [], False
            _, outcome = self.registry.put(
                evolved, position, source=SOURCE_DDL_SQLGLOT, ddl=plan.statement, gtid=gtid
            )
            if outcome is PutOutcome.UNCHANGED:
                return [], False
            return [self._record(evolved, position, SOURCE_DDL_SQLGLOT, plan.statement, gtid)], True

        raise IncompleteDDL("unsupported DDL kind %r" % (change.kind,))

    # -- fallback ------------------------------------------------------
    def _fallback(
        self,
        plan: DDLPlan,
        position: BinlogPosition,
        gtid: Optional[str],
        reason: str,
    ) -> DDLReport:
        if not self.config.schema_fallback_enabled:
            logger.error(
                "DDL at %s could not be interpreted (%s) and the fallback is disabled: %s",
                position,
                reason,
                plan.statement,
            )
            return DDLReport(
                statement=plan.statement, mode="error", tables=[], reason=reason, position=position
            )

        refs = self._fallback_refs(plan)
        if refs:
            logger.warning(
                "DDL at %s fell back to information_schema (%s): %s", position, reason, plan.statement.strip()[:200]
            )
            records = self.refresh_tables(refs, position, gtid, allow_pre_baseline=True)
        else:
            logger.warning(
                "DDL at %s could not be interpreted and no table could be extracted (%s): %s",
                position,
                reason,
                plan.statement.strip()[:200],
            )
            records = self._full_refresh(position, gtid)

        recorded = self._write(records)
        return DDLReport(
            statement=plan.statement,
            mode="fallback" if records else "unhandled",
            tables=[change.target for change in plan.changes],
            reason=reason,
            position=position,
            changed=bool(records),
            recorded=recorded,
        )

    def _full_refresh(self, position: BinlogPosition, gtid: Optional[str]) -> List[object]:
        """Re-read every filtered table and reconcile the registry."""
        baseline = self.registry.baseline
        if baseline is not None and position < baseline:
            # While replaying history the live information_schema does not
            # describe this position, so a full refresh would be wrong.
            logger.error(
                "refusing a full schema refresh at %s: the position predates the bootstrap snapshot %s",
                position,
                baseline,
            )
            return []
        source = self._fallback_source(position)
        schemas = self.loader.load_all()
        present = {schema.name for schema in schemas}
        records: List[object] = []
        for schema in schemas:
            if not self.filter.is_allowed(schema.db, schema.table):
                continue
            _, outcome = self.registry.put(schema, position, source=source, gtid=gtid)
            if outcome is not PutOutcome.UNCHANGED:
                records.append(self._record(schema, position, source, None, gtid))
        for db, table in list(self.registry.tables()):
            if "%s.%s" % (db, table) in present:
                continue
            if self.registry.drop(db, table, position):
                logger.info("table %s.%s disappeared from information_schema", db, table)
        return records

    def _fallback_refs(self, plan: DDLPlan) -> List[Ref]:
        refs: List[Ref] = []
        for change in plan.changes:
            if change.kind is DDLKind.DROP_DATABASE and change.database:
                refs.extend((change.database, table) for _, table in self._tables_of(change.database))
                continue
            if change.table:
                refs.append((change.db or plan.default_db, change.table))
            if change.new_table:
                refs.append((change.new_db or change.db or plan.default_db, change.new_table))
        if not refs:
            refs.extend(plan.salvaged_refs)
        if not refs and plan.default_db:
            refs.extend((plan.default_db, table) for _, table in self._tables_of(plan.default_db))
        deduped: List[Ref] = []
        seen: Set[Ref] = set()
        for ref in refs:
            if not ref[0] or not ref[1] or ref in seen:
                continue
            seen.add(ref)
            deduped.append(ref)
        return deduped

    def _tables_of(self, db: str) -> List[Tuple[str, str]]:
        return [(candidate_db, table) for candidate_db, table in self.registry.tables() if candidate_db == db]

    def _fallback_source(self, position: BinlogPosition) -> str:
        baseline = self.registry.baseline
        if baseline is not None and position < baseline:
            return SOURCE_DDL_FALLBACK_APPROX
        return SOURCE_DDL_FALLBACK

    # -- history -------------------------------------------------------
    def _needs_history(self, db: str, table: str, position: Optional[BinlogPosition]) -> bool:
        if position is None:
            return False
        if not self._history_enabled():
            return False
        baseline = self.registry.baseline
        if baseline is None or position >= baseline:
            return False
        return (db, table) not in self._history_loaded

    def _load_history(self, db: str, table: str) -> int:
        """Load every recorded version of a table older than the snapshot."""
        baseline = self.registry.baseline
        try:
            records = self._store().load_schema_history(db, table, before=baseline)
        except Exception as exc:
            # Deliberately *not* marking the table as loaded: a transient
            # PostgreSQL failure must not permanently hide its history.
            logger.error("could not load the schema history of %s.%s: %s", db, table, exc)
            return 0
        self._history_loaded.add((db, table))

        loaded = 0
        for record in records:
            if record.position is None:
                continue
            _, outcome = self.registry.put(
                record.schema,
                record.position,
                source=record.source,
                ddl=record.ddl,
                gtid=record.gtid,
                history_id=record.record_id,
            )
            if outcome is not PutOutcome.UNCHANGED:
                loaded += 1
        if loaded:
            logger.info("loaded %d historical schema versions for %s.%s", loaded, db, table)
        return loaded

    def _history_schema_before(self, db: str, table: str, position: BinlogPosition) -> Optional[TableSchema]:
        if not self._history_enabled():
            return None
        if (db, table) not in self._history_loaded:
            self._load_history(db, table)
        return self.registry.schema_before(db, table, position)

    # -- plumbing ------------------------------------------------------
    def _history_enabled(self) -> bool:
        return bool(self.config.record_schema_history) and self.store is not None

    def _store(self):
        return self.store

    def _record(
        self,
        schema: TableSchema,
        position: Optional[BinlogPosition],
        source: str,
        ddl: Optional[str],
        gtid: Optional[str],
    ) -> SchemaHistoryRecord:
        return SchemaHistoryRecord(
            db_name=schema.db,
            table_name=schema.table,
            schema=schema,
            position=position,
            gtid=gtid,
            source=source,
            ddl=ddl,
            namespace=self.namespace,
        )

    def _write(self, records: Sequence[object]) -> int:
        if not records or not self._history_enabled():
            return 0
        try:
            return self._store().record_schema_versions(list(records))
        except Exception as exc:
            logger.error("could not persist schema history (%d rows): %s", len(records), exc)
            return 0
