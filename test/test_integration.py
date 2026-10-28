"""Integration tests against the real MySQL master/replica pair and PostgreSQL.

Topology (see ``conftest.py`` for the environment variables):

* the **master** is where the tests create the scratch schema and generate
  traffic (a real replica is ``super_read_only`` and rejects writes);
* the **replica** is the mylox source: the binlog it streams and the
  ``information_schema`` it reads;
* **PostgreSQL** holds ``mylox_checkpoint`` / ``mylox_schema_history``.

Everything created is namespaced (``mylox_it_<random>``) and dropped afterwards.
``MYLOX_TEST_ALLOW_REPLICA_CONTROL=1`` additionally enables the test that issues
``STOP REPLICA`` / ``START REPLICA``.
"""

from __future__ import annotations

import re
import time
import uuid

import pytest

from conftest import replica_control_allowed

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def mysql_conn(settings):
    import pymysql
    import pymysql.cursors

    return pymysql.connect(**settings.cursor_kwargs(pymysql.cursors.DictCursor))


def pg_conn(settings):
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(**settings.connect_kwargs(autocommit=True), row_factory=dict_row)


def binlog_ready(replica_settings) -> bool:
    """The replica must log row events (``log_bin`` + ``log_slave_updates``)."""
    try:
        with mysql_conn(replica_settings) as conn, conn.cursor() as cur:
            cur.execute(
                "SHOW VARIABLES WHERE Variable_name IN "
                "('log_bin', 'binlog_format', 'log_slave_updates', 'log_replica_updates', 'gtid_mode')"
            )
            values = {row["Variable_name"]: row["Value"] for row in cur.fetchall()}
    except Exception:
        return False
    updates = values.get("log_slave_updates") or values.get("log_replica_updates") or "OFF"
    return (
        values.get("log_bin", "OFF").upper() == "ON"
        and values.get("binlog_format", "").upper() == "ROW"
        and updates.upper() == "ON"
        and values.get("gtid_mode", "OFF").upper() == "ON"
    )


def replication_healthy(replica_settings) -> bool:
    """``SHOW REPLICA STATUS`` on 8.0.22+, ``SHOW SLAVE STATUS`` on 5.7."""
    for statement in ("SHOW REPLICA STATUS", "SHOW SLAVE STATUS"):
        try:
            with mysql_conn(replica_settings) as conn, conn.cursor() as cur:
                cur.execute(statement)
                row = cur.fetchone()
        except Exception:
            continue
        if not row:
            return False
        io = row.get("Replica_IO_Running") or row.get("Slave_IO_Running")
        sql = row.get("Replica_SQL_Running") or row.get("Slave_SQL_Running")
        errors = (row.get("Last_Error") or "") + (row.get("Last_IO_Error") or "") + (row.get("Last_SQL_Error") or "")
        return io == "Yes" and sql == "Yes" and not errors
    return False


def mysql_version(settings):
    """``(major, minor, patch)`` of the server behind ``settings``."""
    with mysql_conn(settings) as conn, conn.cursor() as cur:
        cur.execute("SELECT VERSION() AS v")
        raw = cur.fetchone()["v"] or ""
    match = re.match(r"(\d+)\.(\d+)\.(\d+)", str(raw))
    return tuple(int(part) for part in match.groups()) if match else (0, 0, 0)


def sync_replica(master_settings, replica_settings, timeout: float = 30.0) -> str:
    """Block until the replica has executed everything the master has."""
    with mysql_conn(master_settings) as conn, conn.cursor() as cur:
        cur.execute("SELECT @@GLOBAL.gtid_executed AS gtid")
        target = (cur.fetchone()["gtid"] or "").replace("\n", "")
    if not target:
        return target
    with mysql_conn(replica_settings) as conn, conn.cursor() as cur:
        cur.execute("SELECT WAIT_FOR_EXECUTED_GTID_SET(%s, %s) AS waited", (target, timeout))
        waited = cur.fetchone()["waited"]
    if waited is None:
        raise AssertionError("the replica did not catch up to %s within %.0fs" % (target, timeout))
    return target


def only_db(db: str):
    """Rules that let exactly one schema through, in the default filter mode.

    ``TableFilter`` defaults to gitignore semantics: a plain pattern *ignores*.
    So "only this schema" is expressed as "ignore everything, re-include it".
    """
    return ["*", "!%s.*" % db]


def execute_on_master(master_settings, statements) -> None:
    with mysql_conn(master_settings) as conn, conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)


@pytest.fixture(scope="module")
def scratch_db(mysql_master_settings, mysql_settings):
    """A scratch schema created on the master and replicated to the replica."""
    if not binlog_ready(mysql_settings):
        pytest.skip("the replica needs log_bin=ON, binlog_format=ROW, log_slave_updates=ON, gtid_mode=ON")
    if not replication_healthy(mysql_settings):
        pytest.skip("the replica's replication threads are not healthy")
    name = "mylox_it_%s" % uuid.uuid4().hex[:8]
    execute_on_master(mysql_master_settings, ["CREATE DATABASE `%s`" % name])
    sync_replica(mysql_master_settings, mysql_settings)
    try:
        yield name
    finally:
        execute_on_master(mysql_master_settings, ["DROP DATABASE IF EXISTS `%s`" % name])
        try:
            sync_replica(mysql_master_settings, mysql_settings)
        except AssertionError:  # pragma: no cover - cleanup is best effort
            pass


@pytest.fixture(scope="module")
def pg_namespace(pg_settings):
    from mylox.state.store import PgStateStore

    namespace = "mylox_it_%s" % uuid.uuid4().hex[:8]
    store = PgStateStore(pg_settings, namespace=namespace)
    try:
        yield namespace, store
    finally:
        with store.metadata_connection.cursor() as cur:
            cur.execute("DELETE FROM mylox_schema_history WHERE namespace = %s", (namespace,))
            cur.execute("DELETE FROM mylox_checkpoint WHERE pipeline LIKE %s", ("mylox_it_%",))
        store.close()


def make_manager(replica_settings, rules, store=None):
    from mylox.filtering import TableFilter
    from mylox.schema.ddl import SqlglotDDLParser
    from mylox.schema.loader import InformationSchemaLoader
    from mylox.schema.manager import SchemaManager
    from mylox.schema.registry import SchemaRegistry

    if store is None:
        from fakes import FakeStateStore

        store = FakeStateStore()
    loader = InformationSchemaLoader(replica_settings, TableFilter(rules))
    manager = SchemaManager(
        SchemaRegistry(), loader, SqlglotDDLParser(), store, TableFilter(rules)
    )
    manager.bind_namespace("ns")
    return manager, loader


# ---------------------------------------------------------------------------
# PostgreSQL state store
# ---------------------------------------------------------------------------
def test_state_store_roundtrip(pg_settings, pg_namespace):
    from fakes import make_schema
    from mylox.gtid import GtidSet
    from mylox.state.models import SchemaHistoryRecord
    from mylox.utils.positions import BinlogPosition

    namespace, store = pg_namespace
    pipeline = "mylox_it_pipeline"
    position = BinlogPosition("mysql-bin.000001", 1234)
    gtid = GtidSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")

    store.mark_initialized([pipeline], position, gtid, "uuid-x", 42)
    checkpoint = store.get_checkpoints([pipeline])[pipeline]
    assert checkpoint.is_initialized is True
    assert checkpoint.position == position
    assert checkpoint.gtid_set == gtid

    store.advance_checkpoints([(pipeline, "mysql-bin.000002", 55, str(gtid))], "uuid-x", 42)
    store.commit()
    assert store.get_checkpoints([pipeline])[pipeline].log_file == "mysql-bin.000002"

    schema = make_schema("itdb", "ittable", ["id", "a"], ["id"])
    record = SchemaHistoryRecord(
        db_name="itdb",
        table_name="ittable",
        schema=schema,
        position=position,
        source="bootstrap",
        namespace=namespace,
    )
    assert store.record_schema_versions([record]) == 1
    # the unique index makes a replayed version a no-op
    assert store.record_schema_versions([record]) == 0

    records = store.load_schema_history("itdb", "ittable")
    assert records and records[-1].schema.column_names == ("id", "a")
    assert store.load_latest_schemas()[("itdb", "ittable")].schema.primary_keys == ("id",)

    with store.transaction_connection.cursor() as cur:
        cur.execute("SELECT 1")
    store.rollback()


# ---------------------------------------------------------------------------
# information_schema loader
# ---------------------------------------------------------------------------
def test_loader_reads_columns_and_primary_keys(mysql_master_settings, mysql_settings, scratch_db):
    from mylox.filtering import TableFilter
    from mylox.schema.loader import InformationSchemaLoader

    execute_on_master(
        mysql_master_settings,
        [
            "CREATE TABLE `%s`.`t_loader` ("
            " id INT NOT NULL AUTO_INCREMENT,"
            " name VARCHAR(64) DEFAULT NULL,"
            " amount DECIMAL(10,2) NOT NULL DEFAULT 0,"
            " created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            " PRIMARY KEY (id), KEY idx_name (name))" % scratch_db
        ],
    )
    sync_replica(mysql_master_settings, mysql_settings)

    loader = InformationSchemaLoader(mysql_settings, TableFilter(only_db(scratch_db)))
    schemas = {schema.name: schema for schema in loader.load_all()}
    schema = schemas["%s.t_loader" % scratch_db]
    assert schema.column_names == ("id", "name", "amount", "created")
    assert schema.primary_keys == ("id",)
    assert schema.column("amount").nullable is False
    # MySQL 8.4 removed SHOW MASTER STATUS: the loader must fall back
    assert loader.master_status().get("log_file")
    assert loader.server_info()["server_uuid"]


def test_loader_reads_composite_primary_key_order(mysql_master_settings, mysql_settings, scratch_db):
    from mylox.filtering import TableFilter
    from mylox.schema.loader import InformationSchemaLoader

    execute_on_master(
        mysql_master_settings,
        [
            "CREATE TABLE `%s`.`t_pk` (a INT NOT NULL, b INT NOT NULL, c INT, "
            "PRIMARY KEY (b, a))" % scratch_db
        ],
    )
    sync_replica(mysql_master_settings, mysql_settings)

    loader = InformationSchemaLoader(mysql_settings, TableFilter(only_db(scratch_db)))
    schema = loader.load_tables([(scratch_db, "t_pk")])[0]
    # the key order (b, a) differs from the column order (a, b, c)
    assert schema.column_names == ("a", "b", "c")
    assert schema.primary_keys == ("b", "a")


# ---------------------------------------------------------------------------
# sqlglot evolution vs information_schema
# ---------------------------------------------------------------------------
def test_sqlglot_evolution_matches_information_schema(
    mysql_master_settings, mysql_settings, scratch_db
):
    """Every supported DDL must leave the model identical to MySQL's own view."""
    from mylox.utils.positions import BinlogPosition

    table = "t_evolution"
    parent = "t_evo_parent"
    # the referenced column must have exactly the type of the referencing one,
    # and `b` ends up BIGINT, so the parent key is BIGINT too
    execute_on_master(
        mysql_master_settings,
        ["CREATE TABLE `%s`.`%s` (id BIGINT NOT NULL, PRIMARY KEY (id))" % (scratch_db, parent)],
    )
    sync_replica(mysql_master_settings, mysql_settings)
    # MySQL 5.7 has no ``RENAME COLUMN``; ``CHANGE COLUMN old new`` is the
    # equivalent and exercises the same schema mutation.
    version = mysql_version(mysql_settings)
    rename_statement = (
        "ALTER TABLE `{db}`.`{t}` RENAME COLUMN a2 TO a3".format(db=scratch_db, t=table)
        if version >= (8, 0, 0)
        else "ALTER TABLE `{db}`.`{t}` CHANGE COLUMN a2 a3 VARCHAR(20) NULL".format(
            db=scratch_db, t=table
        )
    )

    # (statement, how the parser is expected to classify it)
    statements = [
        (
            "CREATE TABLE `{db}`.`{t}` (id INT NOT NULL AUTO_INCREMENT, a VARCHAR(10), "
            "PRIMARY KEY (id))".format(db=scratch_db, t=table),
            "sqlglot",
        ),
        (
            "ALTER TABLE `{db}`.`{t}` ADD COLUMN b INT NOT NULL DEFAULT 0 AFTER id".format(
                db=scratch_db, t=table
            ),
            "sqlglot",
        ),
        (
            "ALTER TABLE `{db}`.`{t}` ADD COLUMN c VARCHAR(5) FIRST".format(db=scratch_db, t=table),
            "sqlglot",
        ),
        (
            "ALTER TABLE `{db}`.`{t}` CHANGE COLUMN a a2 VARCHAR(20) NULL".format(db=scratch_db, t=table),
            "sqlglot",
        ),
        (
            "ALTER TABLE `{db}`.`{t}` MODIFY COLUMN b BIGINT NULL".format(db=scratch_db, t=table),
            "sqlglot",
        ),
        ("ALTER TABLE `{db}`.`{t}` DROP COLUMN c".format(db=scratch_db, t=table), "sqlglot"),
        (rename_statement, "sqlglot"),
        # index-only DDL: acknowledged but ignored, the column layout is unchanged
        (
            "ALTER TABLE `{db}`.`{t}` ADD CONSTRAINT fk_evo FOREIGN KEY (b) "
            "REFERENCES `{db}`.`{p}` (id)".format(db=scratch_db, t=table, p=parent),
            "ignore",
        ),
    ]
    manager, loader = make_manager(mysql_settings, only_db(scratch_db))
    position = BinlogPosition("mysql-bin.000001", 1)

    for statement, expected_mode in statements:
        execute_on_master(mysql_master_settings, [statement])
        sync_replica(mysql_master_settings, mysql_settings)
        position = BinlogPosition(position.file, position.pos + 1)

        report = manager.handle_ddl(statement, position, default_db=scratch_db)
        assert report.mode == expected_mode, (statement, report.mode, report.reason)

        live = loader.load_tables([(scratch_db, table)])[0]
        modelled = manager.registry.latest_schema(scratch_db, table)
        assert modelled is not None, statement
        assert modelled.column_names == live.column_names, statement
        assert modelled.primary_keys == live.primary_keys, statement
        for name in live.column_names:
            assert modelled.column(name).nullable == live.column(name).nullable, (statement, name)


def test_fallback_recovers_from_an_unparsable_ddl(
    mysql_master_settings, mysql_settings, scratch_db
):
    """``CREATE TABLE ... PARTITION BY`` is valid MySQL but opaque to sqlglot."""
    from mylox.utils.positions import BinlogPosition

    table = "t_fallback"
    create = (
        "CREATE TABLE `%s`.`%s` (id INT NOT NULL, a INT, PRIMARY KEY (id)) "
        "PARTITION BY HASH(id) PARTITIONS 4" % (scratch_db, table)
    )
    execute_on_master(mysql_master_settings, [create])
    sync_replica(mysql_master_settings, mysql_settings)

    manager, loader = make_manager(mysql_settings, only_db(scratch_db))
    report = manager.handle_ddl(create, BinlogPosition("mysql-bin.000001", 10), default_db=scratch_db)
    assert report.mode == "fallback"
    assert report.recorded_history is True
    schema = manager.registry.latest_schema(scratch_db, table)
    assert schema is not None and schema.column_names == ("id", "a")
    # and the model agrees with MySQL's own view of the partitioned table
    live = loader.load_tables([(scratch_db, table)])[0]
    assert schema.column_names == live.column_names
    assert schema.primary_keys == live.primary_keys


# ---------------------------------------------------------------------------
# full engine replay
# ---------------------------------------------------------------------------
def test_engine_replays_live_binlog(
    mysql_master_settings, mysql_settings, pg_settings, scratch_db, pg_namespace
):
    """Drive the real engine over real binlog events.

    ``_prepare()`` runs before the traffic is generated on purpose: the
    bootstrap snapshot is then taken *before* the DDL, so the replayed
    ``ALTER TABLE`` really exercises the sqlglot path.
    """
    from mylox.config import EngineConfig
    from mylox.engine import CDCEngine, CDCHandler, RowChange
    from mylox.filtering import TableFilter
    from mylox.gtid import GtidSet
    from mylox.schema.loader import InformationSchemaLoader
    from mylox.state.store import PgStateStore

    namespace, _ = pg_namespace
    table = "t_engine"
    pipeline = "mylox_it_engine"
    rules = only_db(scratch_db)

    execute_on_master(
        mysql_master_settings,
        [
            "CREATE TABLE `%s`.`%s` (id INT NOT NULL, a VARCHAR(20), b INT, "
            "PRIMARY KEY (id))" % (scratch_db, table)
        ],
    )
    sync_replica(mysql_master_settings, mysql_settings)

    loader = InformationSchemaLoader(mysql_settings, TableFilter(rules))
    start = loader.position()
    info = loader.server_info()
    store = PgStateStore(pg_settings, namespace=namespace)
    store.mark_initialized([pipeline], start, GtidSet(), info["server_uuid"], info["server_id"])

    received: list = []

    class Handler(CDCHandler):
        def handle_row(self, change: RowChange, pl: str, cur) -> None:
            received.append(
                (
                    change.action.value,
                    change.table,
                    dict(change.after or change.before or {}),
                    dict(change.modify or {}),
                )
            )

        def on_init(self, pipelines, ctx):  # pragma: no cover - the pipeline is pre-initialized
            raise AssertionError("the pipeline is already initialized")

    engine = CDCEngine(
        mysql_settings,
        pg_settings,
        [pipeline],
        Handler(),
        EngineConfig(
            namespace=namespace,
            filter_rules=rules,
            pause_slave_on_init=False,
            stream_options={"blocking": False},
        ),
        store=store,
    )
    try:
        engine._prepare()  # bootstrap snapshot at `start`

        execute_on_master(
            mysql_master_settings,
            [
                "INSERT INTO `%s`.`%s` VALUES (1, 'one', 10), (2, 'two', 20)" % (scratch_db, table),
                "UPDATE `%s`.`%s` SET a = 'uno' WHERE id = 1" % (scratch_db, table),
                "DELETE FROM `%s`.`%s` WHERE id = 2" % (scratch_db, table),
                "ALTER TABLE `%s`.`%s` ADD COLUMN c VARCHAR(10) NULL" % (scratch_db, table),
            ],
        )
        sync_replica(mysql_master_settings, mysql_settings)

        engine._consume()  # non blocking: stops at the end of the replica binlog

        # binlog_row_metadata is MINIMAL, so the payload is mapped by ordinal
        # position: the real column names asserted here prove that mapping.
        inserts = [payload for action, _, payload, _ in received if action == "INSERT"]
        assert {"id": 1, "a": "one", "b": 10} in inserts
        assert {"id": 2, "a": "two", "b": 20} in inserts

        updates = [(payload, modify) for action, _, payload, modify in received if action == "UPDATE"]
        assert updates and updates[0][0] == {"id": 1, "a": "one", "b": 10}
        assert updates[0][1] == {"a": "uno"}

        deletes = [payload for action, _, payload, _ in received if action == "DELETE"]
        assert deletes == [{"id": 2, "a": "two", "b": 20}]

        schema = engine.registry.latest_schema(scratch_db, table)
        assert schema is not None and schema.column_names == ("id", "a", "b", "c")

        checkpoint = store.get_checkpoints([pipeline])[pipeline]
        assert checkpoint.position is not None and checkpoint.position > start

        history = store.load_schema_history(scratch_db, table)
        assert any(record.source == "bootstrap" for record in history)
        assert any(record.source == "ddl_sqlglot" for record in history)
    finally:
        engine._shutdown()


# ---------------------------------------------------------------------------
# replica control (opt-in)
# ---------------------------------------------------------------------------
@pytest.mark.skipif(
    not replica_control_allowed(), reason="set MYLOX_TEST_ALLOW_REPLICA_CONTROL=1 to enable"
)
def test_replica_control_commands(mysql_settings):
    """``STOP REPLICA`` / ``START REPLICA`` on MySQL 8.4 (the anchor freeze)."""
    from mylox.schema.loader import InformationSchemaLoader

    loader = InformationSchemaLoader(mysql_settings)
    assert replication_healthy(mysql_settings), "the replica must be healthy before the test"
    try:
        assert loader.pause_replication() is True
        assert not replication_healthy(mysql_settings)
        first = loader.position()
        time.sleep(1.0)
        second = loader.position()
        assert first == second, "the anchor must be stable while the replica is stopped"
    finally:
        assert loader.resume_replication() is True
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and not replication_healthy(mysql_settings):
        time.sleep(0.5)
    assert replication_healthy(mysql_settings), "START REPLICA did not restore replication"


# ---------------------------------------------------------------------------
# full cold start against a real paused replica (opt-in)
# ---------------------------------------------------------------------------
def _wait_for(predicate, what: str, timeout: float = 30.0, interval: float = 0.2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    raise AssertionError("timed out after %.0fs waiting for %s" % (timeout, what))


@pytest.mark.skipif(
    not replica_control_allowed(), reason="set MYLOX_TEST_ALLOW_REPLICA_CONTROL=1 to enable"
)
def test_engine_cold_start_with_a_real_anchor(
    mysql_master_settings, mysql_settings, pg_settings, scratch_db, pg_namespace
):
    """The production first run: STOP REPLICA, anchor, bootstrap, on_init, START REPLICA, stream.

    The engine runs in a background thread (``blocking=True``, as in production);
    ``request_stop`` is honoured at the next heartbeat.
    """
    import threading

    from mylox.config import EngineConfig
    from mylox.engine import CDCEngine, CDCHandler, RowChange
    from mylox.schema.loader import InformationSchemaLoader
    from mylox.state.store import PgStateStore

    namespace, _ = pg_namespace
    table = "t_cold"
    pipeline = "mylox_it_cold"
    rules = only_db(scratch_db)

    execute_on_master(
        mysql_master_settings,
        [
            "CREATE TABLE `%s`.`%s` (id INT NOT NULL, a VARCHAR(20), PRIMARY KEY (id))"
            % (scratch_db, table),
            "INSERT INTO `%s`.`%s` VALUES (1, 'before-init'), (2, 'snapshot')" % (scratch_db, table),
        ],
    )
    sync_replica(mysql_master_settings, mysql_settings)

    store = PgStateStore(pg_settings, namespace=namespace)
    state = {"init_done": False, "init_schemas": [], "init_anchor": None, "rows": []}

    class Handler(CDCHandler):
        def on_init(self, pipelines, ctx) -> None:
            state["init_anchor"] = ctx.anchor
            state["init_schemas"] = sorted(schema.name for schema in ctx.all_schemas())
            # a real handler would bulk load here; the engine resumes replication
            # only after this method returns
            state["init_done"] = True

        def handle_row(self, change: RowChange, pl: str, cur) -> None:
            state["rows"].append((change.action.value, dict(change.after or change.before or {})))

    engine = CDCEngine(
        mysql_settings,
        pg_settings,
        [pipeline],
        Handler(),
        EngineConfig(namespace=namespace, filter_rules=rules),
        store=store,
    )
    thread = threading.Thread(target=engine.run, name="mylox-cold-start", daemon=True)
    thread.start()
    try:
        _wait_for(lambda: state["init_done"], "the cold-start on_init")
        # on_init sees every filtered table of the scratch schema (the other
        # tests in this module created some of them)
        assert "%s.%s" % (scratch_db, table) in state["init_schemas"]
        assert all(name.startswith(scratch_db + ".") for name in state["init_schemas"])
        assert state["init_anchor"] is not None

        # only now generate traffic: the anchor was frozen before it
        execute_on_master(
            mysql_master_settings,
            [
                "INSERT INTO `%s`.`%s` VALUES (3, 'after-init')" % (scratch_db, table),
                "UPDATE `%s`.`%s` SET a = 'updated' WHERE id = 3" % (scratch_db, table),
                "DELETE FROM `%s`.`%s` WHERE id = 1" % (scratch_db, table),
            ],
        )
        sync_replica(mysql_master_settings, mysql_settings)

        _wait_for(lambda: len(state["rows"]) >= 3, "three row events", timeout=45)
        assert ("INSERT", {"id": 3, "a": "after-init"}) in state["rows"]
        assert ("UPDATE", {"id": 3, "a": "after-init"}) in state["rows"]
        assert ("DELETE", {"id": 1, "a": "before-init"}) in state["rows"]
    finally:
        engine.request_stop()
        thread.join(timeout=30)
        engine._shutdown()
        if not replication_healthy(mysql_settings):  # never leave the replica stopped
            InformationSchemaLoader(mysql_settings).resume_replication()

    assert not thread.is_alive(), "the engine did not stop on request"
    assert replication_healthy(mysql_settings), "replication was not resumed"

    checkpoint = store.get_checkpoints([pipeline])[pipeline]
    assert checkpoint.is_initialized is True
    assert checkpoint.position is not None and checkpoint.position >= state["init_anchor"]
    history = store.load_schema_history(scratch_db, table)
    assert any(record.source == "bootstrap" for record in history)


# ---------------------------------------------------------------------------
# failover to a second replica (GTID mode)
# ---------------------------------------------------------------------------
def test_failover_to_a_second_replica(
    mysql_master_settings,
    mysql_settings,
    mysql_second_settings,
    pg_settings,
    scratch_db,
    pg_namespace,
):
    """Move the engine from replica A to replica B, which has a different uuid
    *and* different binlog coordinates: only GTID resume can be correct.

    Three phases:
      1. consume from replica A and persist a checkpoint;
      2. re-run against A - nothing may be re-delivered;
      3. switch to replica B - it must resume with ``auto_position`` (no
         ``log_file``), deliver only the new transaction, and keep growing the
         executed GTID set.
    """
    from mylox.config import EngineConfig
    from mylox.engine import CDCEngine, CDCHandler, RowChange
    from mylox.engine.reader import BinlogReaderFactory
    from mylox.filtering import TableFilter
    from mylox.gtid import GtidSet
    from mylox.schema.loader import InformationSchemaLoader
    from mylox.state.store import PgStateStore

    if not binlog_ready(mysql_second_settings):
        pytest.skip("the second replica needs log_bin / ROW / log_slave_updates / gtid_mode")
    if not replication_healthy(mysql_second_settings):
        pytest.skip("the second replica's replication threads are not healthy")

    namespace, _ = pg_namespace
    table = "t_failover"
    pipeline = "mylox_it_failover"
    rules = only_db(scratch_db)

    execute_on_master(
        mysql_master_settings,
        [
            "CREATE TABLE `%s`.`%s` (id INT NOT NULL, a VARCHAR(20), PRIMARY KEY (id))" % (scratch_db, table),
            "INSERT INTO `%s`.`%s` VALUES (1, 'snapshot'), (2, 'snapshot')" % (scratch_db, table),
        ],
    )
    sync_replica(mysql_master_settings, mysql_settings)
    sync_replica(mysql_master_settings, mysql_second_settings)

    class Recorder(CDCHandler):
        def __init__(self) -> None:
            self.rows = []

        def handle_row(self, change: RowChange, pl: str, cur) -> None:
            if change.table == table:
                self.rows.append((change.action.value, dict(change.after or change.before or {})))

    store = PgStateStore(pg_settings, namespace=namespace)

    def build(replica_settings, recorder, factory=None):
        return CDCEngine(
            replica_settings,
            pg_settings,
            [pipeline],
            recorder,
            EngineConfig(
                namespace=namespace,
                filter_rules=rules,
                pause_slave_on_init=False,
                stream_options={"blocking": False},
            ),
            store=store,
            stream_factory=factory,
        )

    # ---- phase 1: replica A ------------------------------------------------
    loader_a = InformationSchemaLoader(mysql_settings, TableFilter(rules))
    status_a = loader_a.master_status()
    info_a = loader_a.server_info()
    position_a = loader_a.position()
    # production records the *whole* executed set at the anchor, not a single GTID
    store.mark_initialized(
        [pipeline],
        position_a,
        GtidSet.safe_parse(status_a["gtid_set"]),
        info_a["server_uuid"],
        info_a["server_id"],
    )

    first = Recorder()
    engine = build(mysql_settings, first)
    try:
        engine._prepare()
        execute_on_master(
            mysql_master_settings,
            ["INSERT INTO `%s`.`%s` VALUES (3, 'from-replica-a')" % (scratch_db, table)],
        )
        sync_replica(mysql_master_settings, mysql_settings)
        engine._consume()
    finally:
        engine._shutdown()

    assert [row["id"] for _, row in first.rows] == [3]
    checkpoint_a = store.get_checkpoints([pipeline])[pipeline]
    assert checkpoint_a.server_uuid == info_a["server_uuid"]
    assert checkpoint_a.position is not None
    # the checkpoint carries the *whole* executed set recorded at the anchor
    # (compact intervals, not a pile of single GTIDs)
    anchor_set = GtidSet.safe_parse(status_a["gtid_set"])
    assert not anchor_set.is_empty()
    assert checkpoint_a.gtid_set.contains_all(anchor_set)
    assert len(str(checkpoint_a.gtid_set)) < 200

    # ---- phase 2: same replica, nothing new --------------------------------
    replay = Recorder()
    engine = build(mysql_settings, replay)
    try:
        engine._prepare()
        engine._consume()
    finally:
        engine._shutdown()
    assert replay.rows == [], "a restart must not re-deliver already committed transactions"

    # ---- phase 3: fail over to replica B -----------------------------------
    loader_b = InformationSchemaLoader(mysql_second_settings)
    info_b = loader_b.server_info()
    position_b = loader_b.position()
    assert info_b["server_uuid"] != info_a["server_uuid"], "the failover needs two distinct source uuids"
    assert (position_b.file, position_b.pos) != (position_a.file, position_a.pos), (
        "the two replicas must sit at different binlog coordinates for this test to be meaningful"
    )

    calls = []
    real = BinlogReaderFactory(mysql_second_settings, server_id=910099, heartbeat=1.0)

    def recording_factory(start=None, auto_position=None, blocking=True, **kwargs):
        # The engine always asks for a blocking stream (production behaviour);
        # the test wants a bounded one, exactly like `stream_options={"blocking": False}`.
        calls.append({"start": start, "auto_position": auto_position, "blocking": blocking})
        return real.create(start=start, auto_position=auto_position, blocking=False)

    second = Recorder()
    engine = build(mysql_second_settings, second, factory=recording_factory)
    try:
        engine._prepare()
        assert engine._gtid_mode is True, "a changed server_uuid must switch the engine to GTID mode"

        execute_on_master(
            mysql_master_settings,
            ["INSERT INTO `%s`.`%s` VALUES (4, 'from-replica-b')" % (scratch_db, table)],
        )
        sync_replica(mysql_master_settings, mysql_second_settings)
        engine._consume()
    finally:
        engine._shutdown()

    assert calls, "the stream factory was never used"
    assert calls[0]["start"] is None, "GTID mode must not resume from replica A's log file"
    assert calls[0]["auto_position"] == str(checkpoint_a.gtid_set)

    # only the transaction generated after the failover is delivered: the
    # snapshot rows and row 3 are already part of the executed set
    assert [row["id"] for _, row in second.rows] == [4]

    checkpoint_b = store.get_checkpoints([pipeline])[pipeline]
    assert checkpoint_b.server_uuid == info_b["server_uuid"]
    assert checkpoint_b.gtid_set.contains_all(checkpoint_a.gtid_set)
    assert checkpoint_b.gtid_set != checkpoint_a.gtid_set
