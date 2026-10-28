"""The CDC engine: one binlog stream fanned out to many pipelines.

The properties this loop is built around:

* no shadow tables and no master connection - the schema is kept in memory and
  evolved from the DDL statements themselves;
* a DDL advances the checkpoint, so a restart no longer re-applies it;
* a row event never queries PostgreSQL for the schema;
* one failing pipeline is isolated with a savepoint and dropped instead of
  killing the process;
* ``on_init`` always resumes replication, even when it raises;
* the checkpoint carries a real executed GTID set (not a single GTID);
* transient connection errors are retried with exponential backoff.
"""

from __future__ import annotations

import re
import signal
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from pymysqlreplication.event import GtidEvent, HeartbeatLogEvent, QueryEvent, XidEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent

from ..config import EngineConfig, MySQLSettings, PostgresSettings, coerce_mysql_settings, coerce_postgres_settings
from ..constants import SAVEPOINT_TEMPLATE
from ..errors import ConfigurationError, FatalEngineError, ReaderError, SchemaNotFound
from ..filtering import TableFilter
from ..gtid import GtidError, GtidSet
from ..logging_utils import get_logger
from ..schema.ddl import SqlglotDDLParser, is_ddl_statement, looks_like_dml
from ..schema.loader import InformationSchemaLoader
from ..schema.manager import SchemaManager
from ..schema.model import fix_unsigned_values
from ..schema.registry import SchemaRegistry
from ..state.models import Checkpoint
from ..state.store import PgStateStore
from ..utils.positions import BinlogPosition, is_pos_greater_or_equal
from ..utils.rowimage import bitmap_covers, bitmap_expected_columns, remap_columns_with_bitmap
from ..utils.serialize import stringify_bytes
from .context import InitContext
from .handler import CDCHandler, ChangeAction, RowChange
from .pipeline import PipelineSet
from .reader import BinlogReaderFactory

logger = get_logger(__name__)

_SAVEPOINT_RE = re.compile(r"^\s*SAVEPOINT\s+([`\"']?[\w$]+[`\"']?)\s*;?\s*$", re.IGNORECASE)
_ROLLBACK_TO_RE = re.compile(
    r"^\s*ROLLBACK\s+TO\s+(?:SAVEPOINT\s+)?([`\"']?[\w$]+[`\"']?)\s*;?\s*$", re.IGNORECASE
)
_RELEASE_RE = re.compile(r"^\s*RELEASE\s+SAVEPOINT\s+([`\"']?[\w$]+[`\"']?)\s*;?\s*$", re.IGNORECASE)


class CDCEngine:
    """Consume one replica's binlog and feed every pipeline."""

    def __init__(
        self,
        slave_settings: object,
        pg_settings: object,
        pipelines: Iterable[str],
        handler: CDCHandler,
        config: Optional[EngineConfig] = None,
        *,
        registry: Optional[SchemaRegistry] = None,
        store: Optional[PgStateStore] = None,
        loader: Optional[InformationSchemaLoader] = None,
        parser: Optional[SqlglotDDLParser] = None,
        stream_factory: Optional[Any] = None,
    ) -> None:
        names = list(dict.fromkeys(pipelines or []))
        if not names:
            raise ConfigurationError("pipelines must not be empty")
        if handler is None:
            raise ConfigurationError("a CDCHandler instance is required")

        self.mysql_settings: MySQLSettings = coerce_mysql_settings(slave_settings)
        self.pg_settings: PostgresSettings = coerce_postgres_settings(pg_settings)
        self.pipelines = names
        self.handler = handler
        self.config = config or EngineConfig()
        self.namespace = self.config.resolve_namespace(self.pg_settings)

        self.filter = TableFilter(self.config.filter_rules, mode=self.config.filter_mode)
        self.registry = registry if registry is not None else SchemaRegistry()
        self.loader = loader if loader is not None else InformationSchemaLoader(self.mysql_settings, self.filter)
        self.parser = parser if parser is not None else SqlglotDDLParser()
        self.store = (
            store
            if store is not None
            else PgStateStore(self.pg_settings, self.namespace)
        )
        self.schema_manager = SchemaManager(
            self.registry, self.loader, self.parser, self.store, self.filter, self.config
        )
        self.schema_manager.bind_namespace(self.namespace)

        self._server_id = self.config.resolve_server_id()
        if stream_factory is None:
            reader = BinlogReaderFactory(
                self.mysql_settings,
                server_id=self._server_id,
                heartbeat=self.config.slave_heartbeat_seconds,
                extra_options=self.config.stream_options,
                ignore_decode_errors=self.config.ignore_decode_errors,
            )
            self._stream_factory = reader.create
            self._reader = reader
        else:
            self._stream_factory = stream_factory
            self._reader = None

        # runtime state
        self.stop_requested = False
        self.pipeline_set: Optional[PipelineSet] = None
        self._current_uuid: Optional[str] = None
        self._current_server_id: Optional[int] = None
        self._anchor: Optional[BinlogPosition] = None
        self._anchor_gtid: str = ""
        self._paused = False
        self._gtid_mode = False
        self._session_gtids = GtidSet()
        self._session_gtid: Optional[str] = None
        self._txn_active: Set[str] = set()
        #: Pipelines that must NOT advance in the current transaction (a row
        #: could not be decoded).  Kept separate from ``_txn_active`` because a
        #: later event of the same transaction would otherwise re-add them.
        self._txn_blocked: Set[str] = set()
        self._drift_checked: Set[Tuple[str, str, int]] = set()
        self._bitmap_warned: Set[Tuple[str, str, int, str]] = set()
        self._binary_warned: Set[str] = set()
        self._warned_statement_dml = False
        self._warned_gtid_format = False
        self._signals_installed = False

    # -- lifecycle ------------------------------------------------------
    def install_signal_handlers(self) -> bool:
        """Best effort SIGINT/SIGTERM handling (no-op off the main thread)."""
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
            self._signals_installed = True
        except (ValueError, AttributeError):  # pragma: no cover - non main thread
            logger.debug("signal handlers could not be installed in this context")
        return self._signals_installed

    def _signal_handler(self, signum, frame) -> None:  # pragma: no cover - signal path
        if not self.stop_requested:
            logger.info(
                "received signal %s: stopping after the current transaction / next heartbeat", signum
            )
            self.stop_requested = True
        else:
            logger.warning("second signal received, forcing exit")
            self.request_stop(force=True)

    def request_stop(self, force: bool = False) -> None:
        self.stop_requested = True
        if force:
            raise SystemExit(1)

    def run(self) -> None:
        """Run until stopped (or until a fatal error is raised)."""
        self.install_signal_handlers()
        backoff = max(self.config.reconnect_initial_backoff, 0.1)
        attempts = 0
        try:
            # ``_prepare`` may have paused the replica already, so it must live
            # inside the try: the finally block is what guarantees START REPLICA.
            self._prepare()
            while not self.stop_requested:
                try:
                    self._consume()
                    break
                except FatalEngineError:
                    raise
                except Exception as exc:
                    if not self._should_reconnect(exc):
                        raise
                    attempts += 1
                    if self.config.reconnect_max_attempts is not None and attempts > self.config.reconnect_max_attempts:
                        logger.error("giving up after %d reconnect attempts", attempts - 1)
                        raise
                    logger.error("binlog stream failed (%s: %s), reconnecting in %.1fs", type(exc).__name__, exc, backoff)
                    if self._sleep(backoff):
                        break
                    backoff = min(backoff * 2, max(self.config.reconnect_max_backoff, backoff))
                    self._recover()
        finally:
            self._shutdown()

    def _sleep(self, seconds: float) -> bool:
        """Sleep in small slices so a signal is honoured quickly."""
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if self.stop_requested:
                return True
            time.sleep(min(0.2, max(0.0, deadline - time.monotonic())))
        return self.stop_requested

    def _should_reconnect(self, exc: BaseException) -> bool:
        if self.stop_requested:
            return False
        if not self.config.reconnect:
            return False
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            return False
        return True

    def _shutdown(self) -> None:
        self._resume_replication()
        if self.store is not None:
            try:
                self.store.rollback_if_needed()
            except Exception:  # pragma: no cover - best effort
                pass
            self.store.close()
        logger.info("CDC engine stopped")

    # -- preparation ----------------------------------------------------
    def _prepare(self) -> None:
        info = self.loader.server_info()
        self._current_uuid = info.get("server_uuid")
        self._current_server_id = info.get("server_id")
        logger.info(
            "starting mylox engine (namespace=%s, server_id=%s, source_uuid=%s, pipelines=%s)",
            self.namespace,
            self._server_id,
            self._current_uuid,
            self.pipelines,
        )
        logger.info(
            "table filter: mode=%s default_allowed=%s rules=%s",
            self.filter.mode.value,
            self.filter.default,
            list(self.filter.rules) or "<all tables>",
        )

        checkpoints = self.store.get_checkpoints(self.pipelines)
        for name in self.pipelines:
            checkpoints.setdefault(name, Checkpoint(pipeline=name))
        self.pipeline_set = PipelineSet([checkpoints[name] for name in self.pipelines])

        pending = self.pipeline_set.pending_init
        if pending and self.config.pause_slave_on_init:
            self._paused = self.loader.pause_replication()
            if not self._paused and self.config.strict_pause:
                raise FatalEngineError(
                    "replica replication could not be paused and strict_pause is enabled: "
                    "the initialization anchor would be unreliable"
                )

        status = self.loader.master_status()
        anchor = BinlogPosition.maybe(status.get("log_file"), status.get("log_pos"))
        if anchor is None:
            raise ReaderError(
                "could not read the replica binlog coordinates (SHOW MASTER STATUS); "
                "is log_bin enabled and log_slave_updates=ON?"
            )
        self._anchor = anchor
        self._anchor_gtid = status.get("gtid_set") or ""

        self._gtid_mode = self.pipeline_set.validate_gtid_resume(self._current_uuid)
        if self._gtid_mode:
            logger.warning("GTID mode enabled: source server_uuid changed since the last checkpoint")

        if self.config.bootstrap_schema:
            self.schema_manager.bootstrap(anchor, self._anchor_gtid)
            self._warn_binary_columns()

        for state in self.pipeline_set.ready:
            logger.info(
                "pipeline [%s] resumes at %s (gtid_set=%s)",
                state.name,
                state.position,
                str(state.gtid_set)[:120] or "<empty>",
            )

        pending = self.pipeline_set.pending_init
        if pending:
            start = self.pipeline_set.start_position()
            if not self.pipeline_set.has_ready():
                self._cold_start()
            elif start is not None and is_pos_greater_or_equal(start.file, start.pos, anchor.file, anchor.pos):
                # The ready pipelines are already at (or past) the anchor: there
                # is nothing to replay, so waiting for the stream to "reach" the
                # anchor would never happen and would leave the replica paused.
                self._initialize_now([state.name for state in pending])
            else:
                logger.info(
                    "pipelines %s will be initialized when the stream reaches the anchor %s",
                    [state.name for state in pending],
                    anchor,
                )

    def _initialize_now(self, names: List[str]) -> None:
        """Initialize pipelines at the current anchor (no catch-up needed)."""
        if not names:
            return
        logger.info("initializing pipelines %s at the anchor %s", names, self._anchor)
        if not self._run_on_init(names):
            if not self.pipeline_set.has_ready():
                raise FatalEngineError("no pipeline could be initialized")
            return
        self.pipeline_set.mark_initialized(
            names, self._anchor, GtidSet.safe_parse(self._anchor_gtid), self._current_uuid, self._current_server_id
        )
        self._store_initialization(names)

    def _cold_start(self) -> None:
        pending = self.pipeline_set.pending_init
        names = [state.name for state in pending]
        logger.info("cold start: initializing pipelines %s at anchor %s", names, self._anchor)
        succeeded = self._run_on_init(names)
        if succeeded:
            self.pipeline_set.mark_initialized(
                names, self._anchor, GtidSet.safe_parse(self._anchor_gtid), self._current_uuid, self._current_server_id
            )
            self._store_initialization(names)
        elif not self.pipeline_set.has_ready():
            raise FatalEngineError("no pipeline could be initialized")

    def _store_initialization(self, names: Sequence[str]) -> None:
        try:
            self.store.mark_initialized(
                names,
                self._anchor,
                GtidSet.safe_parse(self._anchor_gtid),
                self._current_uuid,
                self._current_server_id,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("could not persist the initialization anchor: %s", exc)

    def _run_on_init(self, names: List[str]) -> bool:
        """Call ``handler.on_init`` and make sure replication is resumed."""
        ctx = InitContext(
            pipelines=frozenset(names),
            anchor=self._anchor,
            server_uuid=self._current_uuid,
            server_id=self._current_server_id,
            mysql_settings=self.mysql_settings,
            pg_settings=self.pg_settings,
            schema_manager=self.schema_manager,
            filter=self.filter,
            engine=self,
        )
        try:
            self.handler.on_init(names, ctx)
            return True
        except Exception as exc:
            logger.exception("on_init failed for pipelines %s: %s", names, exc)
            for name in names:
                self.pipeline_set.drop(name, "on_init failed: %s" % (exc,))
                self._notify_failure(name, exc, None)
            return False
        finally:
            self._resume_replication()

    def _resume_replication(self) -> None:
        if not self._paused:
            return
        self._paused = False
        self.loader.resume_replication()

    def _recover(self) -> None:
        """Rebuild connections and reload checkpoints from PostgreSQL.

        Reloading is essential: if a commit failed, the in-memory position is
        ahead of the durable one and resuming from memory would lose data.
        """
        if self.store is not None:
            try:
                self.store.reconnect()
            except Exception as exc:  # pragma: no cover - defensive
                logger.error("could not reconnect to PostgreSQL: %s", exc)
        if self.pipeline_set is None:
            return
        try:
            checkpoints = self.store.get_checkpoints(self.pipelines)
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("could not reload the checkpoints: %s", exc)
            return
        for state in self.pipeline_set.states():
            fresh = checkpoints.get(state.name)
            if fresh is None:
                continue
            if state.initialized and not fresh.is_initialized:
                # on_init already ran in this process; do not run it again.
                fresh.is_initialized = True
            state.checkpoint = fresh
            state.initialized = fresh.is_initialized
        self._session_gtids = GtidSet()
        self._session_gtid = None

    # -- streaming ------------------------------------------------------
    def _refresh_source_identity(self) -> None:
        """Re-read the source identity before (re)opening the stream.

        A failover may have happened while we were disconnected; resuming a
        new server from the old server's ``log_file``/``log_pos`` is exactly
        the bug GTID mode exists to prevent.
        """
        assert self.pipeline_set is not None
        try:
            info = self.loader.server_info()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("could not re-read the source identity: %s", exc)
            return
        uuid = info.get("server_uuid")
        server_id = info.get("server_id")
        if uuid == self._current_uuid and server_id == self._current_server_id:
            return
        logger.warning(
            "source server changed (%s/%s -> %s/%s), re-evaluating the resume strategy",
            self._current_uuid,
            self._current_server_id,
            uuid,
            server_id,
        )
        self._current_uuid = uuid
        self._current_server_id = server_id
        self._session_gtids = GtidSet()
        self._session_gtid = None
        self._gtid_mode = self.pipeline_set.validate_gtid_resume(uuid)
        if self._gtid_mode:
            logger.warning("GTID mode enabled: the source server_uuid changed")

    def _consume(self) -> None:
        assert self.pipeline_set is not None
        self._refresh_source_identity()
        auto_position: Optional[str] = None
        start: Optional[BinlogPosition] = None

        if self._gtid_mode:
            consumed = self.pipeline_set.gtid_position()
            if consumed.is_empty():
                raise FatalEngineError(
                    "GTID mode is required but no pipeline has an executed GTID set to resume from"
                )
            auto_position = str(consumed)
            logger.info("resuming with auto_position=%s", auto_position[:200])
        else:
            start = self.pipeline_set.start_position()
            if start is None:
                raise FatalEngineError("no ready pipeline has a usable position")
            logger.info("resuming at %s", start)

        stream = self._stream_factory(start=start, auto_position=auto_position, blocking=True)
        in_transaction = False
        has_business_events = False
        self._reset_transaction()

        #: Position of the last event that has been *fully handled*.  The
        #: initialization anchor is only ever compared against this, so the
        #: pending pipelines can never be initialized at a position that
        #: includes an event they have not consumed yet.
        handled: Optional[BinlogPosition] = None

        try:
            for event in stream:
                position = BinlogPosition.maybe(stream.log_file, stream.log_pos)

                if self.stop_requested and not in_transaction:
                    logger.info("stop requested outside a transaction, exiting the stream")
                    break

                if not in_transaction:
                    # Checked on every event (heartbeats and ROTATE included) so
                    # a pending pipeline is initialized even when the anchor is
                    # crossed by a DDL, a log rotation, or nothing at all.
                    self._maybe_initialize_at_anchor(handled)

                if isinstance(event, HeartbeatLogEvent):
                    continue

                if isinstance(event, GtidEvent):
                    self._session_gtid = event.gtid
                    if self._session_gtid:
                        try:
                            self._session_gtids.add(self._session_gtid)
                        except GtidError:
                            # e.g. a non MySQL GTID flavour (MariaDB uses
                            # domain-server-seq); keep running, position based
                            # decisions still work.
                            if not self._warned_gtid_format:
                                self._warned_gtid_format = True
                                logger.warning(
                                    "cannot parse GTID %r, GTID bookkeeping is disabled", self._session_gtid
                                )
                    handled = position
                    continue

                if isinstance(event, QueryEvent):
                    in_transaction, has_business_events = self._handle_query_event(
                        event, position, in_transaction, has_business_events
                    )
                    handled = position
                    if not in_transaction:
                        # a DDL / COMMIT / plain statement ends a transaction:
                        # give the pending pipelines a chance to start here
                        self._maybe_initialize_at_anchor(handled)
                    if self.stop_requested and not in_transaction:
                        break
                    continue

                if isinstance(event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
                    if self._handle_rows_event(event, position):
                        has_business_events = True
                        if not in_transaction:
                            # Non transactional engine (MyISAM) or an implicit
                            # commit: MySQL will not send an XidEvent, so the
                            # checkpoint must be advanced here.
                            self._commit_transaction(position)
                            self._reset_transaction()
                            self._session_gtid = None
                            has_business_events = False
                            handled = position
                            self._maybe_initialize_at_anchor(handled)
                    handled = position
                    continue

                if isinstance(event, XidEvent):
                    if has_business_events:
                        self._commit_transaction(position)
                    else:
                        self._discard_transaction()
                    in_transaction = False
                    has_business_events = False
                    self._reset_transaction()
                    self._session_gtid = None
                    handled = position
                    self._maybe_initialize_at_anchor(handled)
                    if self.stop_requested:
                        logger.info("stop requested at a transaction boundary, exiting the stream")
                        break
                    continue

                handled = position
        except KeyboardInterrupt:  # pragma: no cover - belt and braces
            logger.info("keyboard interrupt caught in the stream loop")
        finally:
            if self._reader is not None:
                self._reader.close(stream)
            else:
                try:
                    stream.close()
                except Exception:  # pragma: no cover - best effort
                    pass
            self._discard_transaction()

    # -- event handlers -------------------------------------------------
    def _handle_query_event(
        self, event: QueryEvent, position: Optional[BinlogPosition], in_transaction: bool, has_business: bool
    ) -> Tuple[bool, bool]:
        raw_schema = event.schema
        schema = raw_schema.decode("utf-8", "replace") if isinstance(raw_schema, bytes) else raw_schema
        query = (event.query or "").strip()
        if not query:
            return in_transaction, has_business
        upper = query.upper()

        if upper == "BEGIN":
            self._discard_transaction()
            self._reset_transaction()
            return True, False
        if upper == "ROLLBACK":
            self._discard_transaction()
            self._reset_transaction()
            return False, False
        if upper == "COMMIT":
            if has_business:
                self._commit_transaction(position)
            else:
                self._discard_transaction()
            self._reset_transaction()
            self._session_gtid = None
            self._maybe_initialize_at_anchor(position)
            return False, False

        if in_transaction and self._mirror_savepoint(query):
            return in_transaction, has_business

        if is_ddl_statement(query):
            self._apply_ddl(query, schema, position)
            if in_transaction:
                # MySQL implicitly commits DDL; if it ever shows up inside a
                # transaction the following Xid must advance the checkpoint.
                self._txn_active |= self._active_pipelines(position, self._session_gtid)
                return in_transaction, True
            # Never leave the DDL un-checkpointed: that was the restart loop
            # that re-applied the same DDL forever.
            active = self._active_pipelines(position, self._session_gtid)
            if active:
                self._txn_active = set(active)
                self._commit_transaction(position)
                self._reset_transaction()
            self._session_gtid = None
            return in_transaction, has_business

        if looks_like_dml(query):
            self._handle_statement_dml(query)
        return in_transaction, has_business

    def _handle_statement_dml(self, query: str) -> None:
        """A DML statement in the binlog means statement based replication.

        Row based replication never puts DML in a ``QueryEvent``.  Silently
        ignoring it would drop data, so the default policy is to fail loudly.
        """
        policy = self.config.on_statement_dml
        if policy != "ignore" and not self._warned_statement_dml:
            self._warned_statement_dml = True
            logger.warning(
                "a binlog QueryEvent carries DML (%s): the source is logging statements "
                "(binlog_format is not ROW); mylox can only capture row events",
                query[:160],
            )
        if policy == "fail":
            raise FatalEngineError(
                "the source logged DML in statement format (%s); set binlog_format=ROW or "
                "relax EngineConfig.on_statement_dml" % query[:120]
            )

    def _apply_ddl(self, query: str, default_db: Optional[str], position: Optional[BinlogPosition]) -> None:
        try:
            report = self.schema_manager.handle_ddl(
                query, position=position, default_db=default_db, gtid=self._session_gtid
            )
            self._warn_binary_columns()
            if report.mode in ("unhandled", "error"):
                self._schema_warning(
                    "DDL not applied to the schema model",
                    {"statement": query, "position": str(position), "reason": report.reason, "mode": report.mode},
                )
        except Exception as exc:
            logger.exception("failed to apply DDL at %s: %s", position, query[:200])
            self._schema_warning(
                "DDL failed", {"statement": query, "position": str(position), "error": repr(exc)}
            )

    def _handle_rows_event(self, event: Any, position: Optional[BinlogPosition]) -> bool:
        assert self.pipeline_set is not None
        db = event.schema
        table = event.table
        active = self._active_pipelines(position, self._session_gtid)
        if not active:
            return False

        if not db or not table or not self.filter.is_allowed(db, table):
            # Nothing to apply, but the position *was* consumed: letting the
            # checkpoint move past filtered-only traffic keeps the resume point
            # close to the head instead of replaying the whole tail.
            self._txn_active |= active
            return True

        schema = self._resolve_schema(db, table, position, event)
        if schema is None:
            # Never decode a row without a trustworthy schema, and never let the
            # checkpoints move past it: ``_txn_blocked`` covers the rest of this
            # transaction, ``drop`` (default) parks the affected pipelines so
            # the row can still be delivered by a later catch-up run.
            self._txn_blocked |= active
            reason = "no trustworthy schema for %s at %s" % (table, position)
            logger.error("%s, event not delivered", reason)
            self._schema_warning(
                "row event without schema",
                {"table": "%s.%s" % (db, table), "position": str(position), "policy": self.config.on_schema_error},
            )
            if self.config.on_schema_error == "drop":
                error = SchemaNotFound(reason)
                for name in sorted(active):
                    self.pipeline_set.drop(name, reason)
                    self._notify_failure(name, error, None)
                if not self.pipeline_set.has_ready() and not self.pipeline_set.pending_init:
                    raise FatalEngineError(
                        "every pipeline was parked because no schema could be resolved (%s); "
                        "fix mylox_schema_history or reset the affected checkpoints" % reason
                    )
            return False

        col_names = list(schema.column_names)
        action = (
            ChangeAction.INSERT
            if isinstance(event, WriteRowsEvent)
            else ChangeAction.UPDATE
            if isinstance(event, UpdateRowsEvent)
            else ChangeAction.DELETE
        )

        before_bitmap = getattr(event, "columns_present_bitmap", None)
        after_bitmap = getattr(event, "columns_present_bitmap2", before_bitmap)
        self._warn_on_short_bitmap(db, table, position, col_names, before_bitmap, after_bitmap)
        self._txn_active |= active

        try:
            with self.store.cursor() as cur:
                for row in event.rows:
                    change = self._build_change(
                        event, row, action, db, table, schema, before_bitmap, after_bitmap, position
                    )
                    if change is None:
                        continue
                    self._dispatch(change, active, cur)
        except FatalEngineError:
            raise
        except Exception as exc:
            # The event was received but cannot be decoded (a charset the driver
            # cannot handle, an unsupported column type, a truncated packet...).
            # Retrying would fail identically, so the pipelines are parked
            # instead of reconnecting into the same error forever.
            self._handle_undecodable_event(db, table, position, active, exc)
            return False
        return True

    def _handle_undecodable_event(
        self,
        db: str,
        table: str,
        position: Optional[BinlogPosition],
        active: Set[str],
        error: BaseException,
    ) -> None:
        reason = "cannot decode the binlog event for %s.%s at %s (%s: %s)" % (
            db,
            table,
            position,
            type(error).__name__,
            error,
        )
        logger.error("%s", reason)
        self._txn_blocked |= active
        self._schema_warning(
            "undecodable row event",
            {
                "table": "%s.%s" % (db, table),
                "position": str(position),
                "error": "%s: %s" % (type(error).__name__, error),
                "policy": self.config.on_schema_error,
            },
        )
        if self.config.on_schema_error == "drop":
            failure = ReaderError(reason)
            for name in sorted(active):
                self.pipeline_set.drop(name, reason)
                self._notify_failure(name, failure, None)
            if not self.pipeline_set.has_ready() and not self.pipeline_set.pending_init:
                raise FatalEngineError(
                    "every pipeline was parked because a binlog event could not be decoded (%s)" % reason
                )

    def _build_change(
        self,
        event: Any,
        row: Dict[str, Any],
        action: ChangeAction,
        db: str,
        table: str,
        schema: Any,
        before_bitmap: Any,
        after_bitmap: Any,
        position: Optional[BinlogPosition],
    ) -> Optional[RowChange]:
        col_names = list(schema.column_names)
        primary_keys = list(schema.primary_keys)

        def remap(values: Any, bitmap: Any) -> Dict[str, Any]:
            return fix_unsigned_values(
                schema,
                remap_columns_with_bitmap(values, col_names, bitmap, on_unknown=self._on_unknown_column),
            )

        base = {
            "db": db,
            "table": table,
            "action": action,
            "timestamp": getattr(event, "timestamp", None),
            "primary_keys": tuple(primary_keys),
            "log_file": position.file if position else None,
            "log_pos": position.pos if position else None,
            "gtid": self._session_gtid,
        }

        if action is ChangeAction.INSERT:
            after = stringify_bytes(remap(row.get("values"), after_bitmap))
            return RowChange(before=None, after=after, modify=None, **base)  # type: ignore[arg-type]

        if action is ChangeAction.DELETE:
            before = stringify_bytes(remap(row.get("values") or row.get("before_values"), before_bitmap))
            return RowChange(before=before, after=None, modify=None, **base)  # type: ignore[arg-type]

        before = stringify_bytes(remap(row.get("before_values"), before_bitmap))
        after = stringify_bytes(remap(row.get("after_values"), after_bitmap))
        modify: Dict[str, Any] = {}
        for column, new_value in after.items():
            if column in primary_keys and before.get(column) == new_value:
                continue
            if column not in before or before[column] != new_value:
                modify[column] = new_value
        return RowChange(before=before, after=None, modify=modify, **base)  # type: ignore[arg-type]

    def _dispatch(self, change: RowChange, active: Set[str], cur: Any) -> None:
        """Run the handler for every active pipeline, isolated by savepoints."""
        policy = self.config.on_pipeline_error
        for index, name in enumerate(sorted(active)):
            savepoint = SAVEPOINT_TEMPLATE.format(index=index)
            cur.execute("SAVEPOINT %s" % savepoint)
            try:
                self.handler.handle_row(change, name, cur)
                cur.execute("RELEASE SAVEPOINT %s" % savepoint)
            except Exception as exc:
                try:
                    cur.execute("ROLLBACK TO SAVEPOINT %s" % savepoint)
                except Exception:  # pragma: no cover - the transaction is broken
                    logger.error("could not roll back to %s", savepoint)
                logger.exception("pipeline [%s] failed on %s: %s", name, change.full_name, exc)
                if policy == "raise":
                    raise FatalEngineError(
                        "pipeline [%s] failed while handling %s: %s" % (name, change.full_name, exc)
                    ) from exc
                self.pipeline_set.drop(name, "%s: %s" % (type(exc).__name__, exc))
                self._txn_active.discard(name)
                self._txn_blocked.add(name)
                self._notify_failure(name, exc, change)
        if not self.pipeline_set.has_ready() and not self.pipeline_set.pending_init:
            raise FatalEngineError("every pipeline has failed; stopping the engine")

    def _notify_failure(self, name: str, error: BaseException, change: Optional[RowChange]) -> None:
        try:
            self.handler.on_pipeline_failed(name, error, change)
        except Exception:  # pragma: no cover - user callback must not kill the engine
            logger.exception("on_pipeline_failed raised for pipeline [%s]", name)

    def _schema_warning(self, message: str, detail: Dict[str, Any]) -> None:
        try:
            self.handler.on_schema_warning(message, detail)
        except Exception:  # pragma: no cover - user callback
            logger.debug("on_schema_warning raised", exc_info=True)

    def _mirror_savepoint(self, query: str) -> bool:
        """Mirror MySQL's savepoints onto the PostgreSQL transaction.

        Without this, rows written between ``SAVEPOINT`` and
        ``ROLLBACK TO SAVEPOINT`` would stay applied even though MySQL rolled
        them back.  Returns ``True`` when the statement was a savepoint one.
        """
        match = _SAVEPOINT_RE.match(query)
        if match:
            statement = "SAVEPOINT %s" % self._pg_savepoint(match.group(1))
        else:
            match = _ROLLBACK_TO_RE.match(query)
            if match:
                statement = "ROLLBACK TO SAVEPOINT %s" % self._pg_savepoint(match.group(1))
            else:
                match = _RELEASE_RE.match(query)
                if not match:
                    return False
                statement = "RELEASE SAVEPOINT %s" % self._pg_savepoint(match.group(1))
        try:
            with self.store.cursor() as cur:
                cur.execute(statement)
        except Exception as exc:
            logger.warning("could not mirror %r on PostgreSQL: %s", query.strip()[:80], exc)
        return True

    @staticmethod
    def _pg_savepoint(name: str) -> str:
        safe = re.sub(r"[^0-9A-Za-z_$]", "", name) or "unnamed"
        return '"mysql_%s"' % safe[:48]

    # -- helpers --------------------------------------------------------
    def _active_pipelines(self, position: Optional[BinlogPosition], gtid: Optional[str]) -> Set[str]:
        assert self.pipeline_set is not None
        return self.pipeline_set.active_pipelines(position, gtid, self._gtid_mode)

    def _resolve_schema(self, db: str, table: str, position: Optional[BinlogPosition], event: Any):
        """Pick the schema used to decode one row event.

        Returns ``None`` when no schema can be trusted.  The caller then skips
        the event *and* keeps the pipeline from advancing: decoding a row with a
        schema whose column order does not match the event silently writes
        values into the wrong columns, which is far worse than a stalled
        pipeline.
        """
        try:
            schema = self.schema_manager.schema_for(db, table, position)
        except SchemaNotFound:
            schema = None

        expected = getattr(event, "number_of_columns", None)
        mismatch = schema is not None and expected is not None and len(schema.columns) != expected
        if mismatch:
            self._warn_schema_drift(db, table, position, expected, len(schema.columns))

        if schema is None or mismatch:
            refreshed = None
            if position is not None and not self.schema_manager.is_pre_baseline(position):
                # At or after the snapshot the live information_schema is a
                # valid source (the table may simply have been created while a
                # pipeline was behind, or the creating DDL was filtered out).
                self.schema_manager.refresh_tables_and_record([(db, table)], position, self._session_gtid)
                refreshed = self.schema_manager.registry.schema_at(db, table, position)
            elif position is not None:
                logger.error(
                    "no schema for %s.%s at %s: the position predates the bootstrap snapshot %s and "
                    "mylox_schema_history has no usable version; the event is skipped and the pipeline "
                    "is not advanced",
                    db,
                    table,
                    position,
                    self.registry.baseline,
                )
                self._schema_warning(
                    "no historical schema",
                    {
                        "table": "%s.%s" % (db, table),
                        "position": str(position),
                        "baseline": str(self.registry.baseline),
                    },
                )

            if refreshed is not None and (expected is None or len(refreshed.columns) == expected):
                return refreshed
            return None
        return schema

    def _warn_schema_drift(
        self, db: str, table: str, position: Optional[BinlogPosition], expected: int, modelled: int
    ) -> None:
        key = (db, table, int(expected))
        if key in self._drift_checked:
            return
        self._drift_checked.add(key)
        logger.warning(
            "schema drift on %s.%s at %s: the binlog event has %d columns but the model has %d",
            db,
            table,
            position,
            expected,
            modelled,
        )
        self._schema_warning(
            "schema drift detected",
            {
                "table": "%s.%s" % (db, table),
                "position": str(position),
                "event_columns": expected,
                "model_columns": modelled,
            },
        )

    #: MySQL types whose values cannot survive the driver's text decoding.
    _BINARY_TYPE_PREFIXES = ("blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary", "geometry")

    def _warn_binary_columns(self) -> None:
        """Tell the operator, once per table, that binary columns are lossy."""
        for version in self.registry.snapshot():
            schema = version.schema
            if schema.name in self._binary_warned:
                continue
            binary = [
                column.name
                for column in schema.columns
                if (column.column_type or "").lower().startswith(self._BINARY_TYPE_PREFIXES)
            ]
            if not binary:
                continue
            self._binary_warned.add(schema.name)
            logger.warning(
                "%s has binary column(s) %s: the binlog reader decodes them as text, so their "
                "values are lossy (use a text/hex representation upstream if byte fidelity matters)",
                schema.name,
                ", ".join(binary),
            )

    def _on_unknown_column(self, key: Any, index: Optional[int]) -> None:
        logger.debug("row image carries an unmapped column %r (index=%s)", key, index)

    def _warn_on_short_bitmap(
        self,
        db: str,
        table: str,
        position: Optional[BinlogPosition],
        col_names: Sequence[str],
        before_bitmap: Any,
        after_bitmap: Any,
    ) -> None:
        """A bitmap shorter than the column list silently truncates payloads."""
        for label, bitmap in (("before", before_bitmap), ("after", after_bitmap)):
            if bitmap is None or bitmap_covers(bitmap, len(col_names)):
                continue
            key = (db, table, len(col_names), label)
            if key in self._bitmap_warned:
                continue
            self._bitmap_warned.add(key)
            logger.warning(
                "%s image bitmap of %s.%s at %s covers %s bits but the schema has %d columns; "
                "the payload may be truncated",
                label,
                db,
                table,
                position,
                bitmap_expected_columns(bitmap),
                len(col_names),
            )

    def _commit_transaction(self, position: Optional[BinlogPosition]) -> None:
        """Advance the checkpoints and commit the business transaction.

        The executed GTID set comes from ``self._session_gtids`` (everything this
        process has seen), which is a superset of what the transaction itself
        carried - and therefore always safe to persist.
        """
        assert self.pipeline_set is not None
        if position is None:
            logger.warning("cannot commit checkpoint without a binlog position")
            self._discard_transaction()
            return

        advancing = [
            state
            for state in (self.pipeline_set.get(name) for name in sorted(self._txn_active - self._txn_blocked))
            if state is not None and not state.failed
        ]

        # The in-memory checkpoints are only moved *after* the commit succeeds.
        # If the commit fails, the process keeps the durable position and is
        # therefore free to re-deliver the transaction instead of skipping it.
        updates: List[Tuple[str, Optional[str], Optional[int], Optional[str]]] = []
        for state in advancing:
            merged = state.gtid_set.copy()
            merged.update(self._session_gtids)
            updates.append((state.name, position.file, position.pos, str(merged) if merged else None))

        try:
            if updates:
                self.store.advance_checkpoints(updates, self._current_uuid, self._current_server_id)
            self.store.commit()
        except Exception as exc:
            logger.error("checkpoint commit failed at %s: %s", position, exc)
            self.store.rollback_if_needed()
            raise
        for state in advancing:
            state.advance(position, self._session_gtids, self._current_uuid, self._current_server_id)

    def _reset_transaction(self) -> None:
        self._txn_active = set()
        self._txn_blocked = set()

    def _discard_transaction(self) -> None:
        try:
            self.store.rollback_if_needed()
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("rollback failed: %s", exc)

    def _maybe_initialize_at_anchor(self, position: Optional[BinlogPosition]) -> None:
        assert self.pipeline_set is not None
        pending = self.pipeline_set.pending_init
        if not pending or self._anchor is None or position is None:
            return
        if not is_pos_greater_or_equal(position.file, position.pos, self._anchor.file, self._anchor.pos):
            return

        names = [state.name for state in pending]
        logger.info("reached the initialization anchor %s, initializing pipelines %s", self._anchor, names)
        self._discard_transaction()
        if self._run_on_init(names):
            consumed = self._session_gtids.copy()
            consumed.update(GtidSet.safe_parse(self._anchor_gtid))
            self.pipeline_set.mark_initialized(
                names, position, consumed, self._current_uuid, self._current_server_id
            )
            try:
                self.store.mark_initialized(
                    names, position, consumed, self._current_uuid, self._current_server_id
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.error("could not persist the initialization anchor: %s", exc)
        elif not self.pipeline_set.has_ready():
            raise FatalEngineError("no pipeline could be initialized at the anchor")
