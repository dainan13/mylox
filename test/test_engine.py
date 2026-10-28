"""End-to-end engine behaviour, driven by fake binlog events.

These tests cover the concrete defects listed in the code review:

* P0-1/P0-5 the checkpoint carries a real executed GTID set and active-set
  decisions use the GTID, not the binlog file name;
* P0-2  a DDL advances the checkpoint;
* P0-3  a row event never reads the schema history from PostgreSQL;
* P0-4  a failing pipeline is isolated and ``on_init`` always resumes
  replication;
* P0-6  schema history is written outside the business transaction.
"""

from __future__ import annotations

import pytest

from fakes import (
    FakeDeleteRows,
    FakeGtid,
    FakeHeartbeat,
    FakeLoader,
    FakeQuery,
    FakeStateStore,
    FakeStream,
    FakeUpdateRows,
    FakeWriteRows,
    FakeXid,
    make_handler,
    make_schema,
)
from mylox.config import EngineConfig, MySQLSettings, PostgresSettings
from mylox.engine import CDCEngine
from mylox.errors import FatalEngineError, ReaderError
from mylox.gtid import GtidSet
from mylox.state.models import Checkpoint
from mylox.utils.positions import BinlogPosition

UUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
UUID2 = "11111111-2222-3333-4444-555555555555"
ANCHOR = BinlogPosition("mysql-bin.000001", 1000)
#: Default bootstrap snapshot position: behind the checkpoints, i.e. the
#: "schema already known in memory" fast path.
BASELINE = BinlogPosition("mysql-bin.000001", 50)


def mysql_settings():
    return MySQLSettings(host="replica", user="cdc", password="x")


def pg_settings():
    return PostgresSettings(dbname="dw", user="cdc", password="x")


def build_engine(
    events,
    handler=None,
    checkpoints=(),
    schemas=None,
    config=None,
    loader=None,
    store=None,
    event_log_file="mysql-bin.000001",
    event_log_pos_start=0,
    anchor=BASELINE,
):
    handler = handler if handler is not None else make_handler()
    store = store if store is not None else FakeStateStore(checkpoints=checkpoints)
    schemas = schemas if schemas is not None else [make_schema("d", "t", ["id", "a", "b"], ["id"])]
    loader = (
        loader
        if loader is not None
        else FakeLoader(schemas=schemas, position=anchor, server_uuid=UUID, server_id=7)
    )
    config = config or EngineConfig(namespace="ns")
    streams = []

    def stream_factory(start=None, auto_position=None, blocking=True, **kwargs):
        stream = FakeStream(events, log_file=event_log_file, log_pos=event_log_pos_start)
        stream.start = start
        stream.auto_position = auto_position
        streams.append(stream)
        return stream

    engine = CDCEngine(
        mysql_settings(),
        pg_settings(),
        ["p1"],
        handler,
        config,
        store=store,
        loader=loader,
        stream_factory=stream_factory,
    )
    engine.streams = streams
    return engine, handler, store, loader


def initialized_checkpoint(name="p1", file="mysql-bin.000001", pos=100, gtid="", server_uuid=UUID):
    return Checkpoint(
        pipeline=name,
        log_file=file,
        log_pos=pos,
        gtid_set=GtidSet(gtid) if gtid else GtidSet(),
        server_uuid=server_uuid,
        server_id=7,
        is_initialized=True,
    )


# ---------------------------------------------------------------------------
# row events
# ---------------------------------------------------------------------------
def test_insert_update_delete_payloads_and_checkpoint():
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (FakeGtid("%s:101" % UUID), "mysql-bin.000001", 120),
        (
            FakeWriteRows(
                "d",
                "t",
                [{"values": {"UNKNOWN_COL0": 1, "UNKNOWN_COL1": "x", "UNKNOWN_COL2": "y"}}],
                columns=3,
                bitmap=bytes([0b00000111]),
            ),
            "mysql-bin.000001",
            130,
        ),
        (
            FakeUpdateRows(
                "d",
                "t",
                [
                    {
                        "before_values": {"UNKNOWN_COL0": 1, "UNKNOWN_COL1": "x", "UNKNOWN_COL2": "y"},
                        "after_values": {"UNKNOWN_COL0": 1, "UNKNOWN_COL1": "x2", "UNKNOWN_COL2": "y"},
                    }
                ],
                columns=3,
                bitmap=bytes([0b00000111]),
                bitmap2=bytes([0b00000111]),
            ),
            "mysql-bin.000001",
            140,
        ),
        (FakeXid(), "mysql-bin.000001", 150),
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 160),
        (
            FakeDeleteRows(
                "d", "t", [{"values": {"UNKNOWN_COL0": 1, "UNKNOWN_COL1": None, "UNKNOWN_COL2": None}}], columns=3, bitmap=bytes([0b00000001])
            ),
            "mysql-bin.000001",
            170,
        ),
        (FakeXid(), "mysql-bin.000001", 180),
    ]
    engine, handler, store, _ = build_engine(events, checkpoints=[initialized_checkpoint()])
    engine.run()

    assert [change.action.value for change, _ in handler.calls] == ["INSERT", "UPDATE", "DELETE"]
    insert = handler.calls[0][0]
    assert insert.after == {"id": 1, "a": "x", "b": "y"}
    assert insert.before is None

    update = handler.calls[1][0]
    assert update.before == {"id": 1, "a": "x", "b": "y"}
    assert update.modify == {"a": "x2"}  # unchanged columns stay out
    assert update.after is None

    delete = handler.calls[2][0]
    assert delete.before == {"id": 1}  # bitmap trimmed the absent columns
    assert [pipeline for _, pipeline in handler.calls] == ["p1"] * 3

    checkpoint = store.checkpoints["p1"]
    assert (checkpoint.log_file, checkpoint.log_pos) == ("mysql-bin.000001", 180)
    assert store.commits >= 2


def test_savepoints_isolate_every_pipeline():
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (FakeWriteRows("d", "t", [{"values": {"UNKNOWN_COL0": 1}}], columns=3, bitmap=bytes([0b00000001])), "mysql-bin.000001", 120),
        (FakeXid(), "mysql-bin.000001", 130),
    ]
    handler = make_handler()
    engine, handler, store, _ = build_engine(
        events,
        handler=handler,
        checkpoints=[initialized_checkpoint("p1"), initialized_checkpoint("p2")],
    )
    engine.pipelines = ["p1", "p2"]
    engine.pipeline_set = None
    engine.run()

    statements = [sql for sql, _ in store.statements]
    assert any(sql.startswith("SAVEPOINT") for sql in statements)
    assert any(sql.startswith("RELEASE SAVEPOINT") for sql in statements)
    assert sorted(pipeline for _, pipeline in handler.calls) == ["p1", "p2"]


def test_failing_pipeline_is_dropped_and_the_others_keep_running():
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (FakeWriteRows("d", "t", [{"values": {"UNKNOWN_COL0": 1}}], columns=3, bitmap=bytes([0b00000001])), "mysql-bin.000001", 120),
        (FakeGtid("%s:102" % UUID), "mysql-bin.000001", 125),
        (FakeXid(), "mysql-bin.000001", 130),
    ]
    handler = make_handler(fail_for={"p1": 1})
    engine, handler, store, _ = build_engine(
        events,
        handler=handler,
        checkpoints=[initialized_checkpoint("p1"), initialized_checkpoint("p2")],
    )
    engine.pipelines = ["p1", "p2"]
    engine.run()

    assert handler.failed and handler.failed[0][0] == "p1"
    # p2 still received the change
    assert [pipeline for _, pipeline in handler.calls] == ["p2"]
    # p1 must not advance its checkpoint any more
    assert store.checkpoints["p1"].log_pos == 100
    assert store.checkpoints["p2"].log_pos == 130
    statements = [sql for sql, _ in store.statements]
    assert any(sql.startswith("ROLLBACK TO SAVEPOINT") for sql in statements)


def test_all_pipelines_failing_is_fatal():
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (FakeWriteRows("d", "t", [{"values": {"UNKNOWN_COL0": 1}}], columns=3, bitmap=bytes([0b00000001])), "mysql-bin.000001", 120),
        (FakeXid(), "mysql-bin.000001", 130),
    ]
    handler = make_handler(fail_for={"p1": 1})
    engine, handler, store, _ = build_engine(events, handler=handler, checkpoints=[initialized_checkpoint()])
    with pytest.raises(FatalEngineError):
        engine.run()


def test_row_events_do_not_read_pg_schema_history():
    """P0-3: the schema comes from memory, not from a per-row SELECT."""
    events = [(FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110)]
    for index in range(20):
        events.append(
            (
                FakeWriteRows(
                    "d", "t", [{"values": {"UNKNOWN_COL0": index}}], columns=3, bitmap=bytes([0b00000001])
                ),
                "mysql-bin.000001",
                120 + index,
            )
        )
    events.append((FakeXid(), "mysql-bin.000001", 200))

    engine, handler, store, _ = build_engine(events, checkpoints=[initialized_checkpoint()])
    engine.run()

    assert len(handler.calls) == 20
    # 20 row events and one table: the history was never touched.
    assert store.history_reads == 0


def _pre_baseline_events():
    events = [(FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110)]
    for index in range(20):
        events.append(
            (
                FakeWriteRows(
                    "d", "t", [{"values": {"UNKNOWN_COL0": index}}], columns=3, bitmap=bytes([0b00000001])
                ),
                "mysql-bin.000001",
                120 + index,
            )
        )
    events.append((FakeXid(), "mysql-bin.000001", 200))
    return events


def _history_record(position, columns=("id", "a", "b"), namespace="ns"):
    from mylox.state.models import SchemaHistoryRecord

    return SchemaHistoryRecord(
        db_name="d",
        table_name="t",
        schema=make_schema("d", "t", list(columns), ["id"]),
        position=position,
        source="bootstrap",
        namespace=namespace,
    )


def test_replaying_before_the_snapshot_uses_history_once_per_table():
    """The only case that needs the persisted timeline, and only once."""
    store = FakeStateStore(
        checkpoints=[initialized_checkpoint()],
        history=[_history_record(BinlogPosition("mysql-bin.000001", 60))],
    )
    engine, handler, store, _ = build_engine(
        _pre_baseline_events(), checkpoints=[initialized_checkpoint()], store=store, anchor=ANCHOR
    )
    engine.run()

    assert len(handler.calls) == 20
    assert store.history_reads == 1
    assert store.checkpoints["p1"].log_pos == 200


def test_pre_baseline_rows_without_history_are_not_decoded_with_a_guess():
    """No history -> refuse, rather than decode columns in the wrong order."""
    engine, handler, store, _ = build_engine(
        _pre_baseline_events(), checkpoints=[initialized_checkpoint()], anchor=ANCHOR
    )
    with pytest.raises(FatalEngineError):
        engine.run()

    assert handler.calls == []
    assert store.checkpoints["p1"].log_pos == 100  # never advanced
    assert any(warning[0] == "no historical schema" for warning in handler.warnings)
    assert handler.failed and handler.failed[0][0] == "p1"


# ---------------------------------------------------------------------------
# DDL handling
# ---------------------------------------------------------------------------
def test_ddl_is_applied_and_advances_the_checkpoint():
    """P0-2: a DDL outside a transaction must advance the position."""
    events = [
        (FakeGtid("%s:201" % UUID), "mysql-bin.000001", 210),
        (FakeQuery("d", "ALTER TABLE t ADD COLUMN c INT"), "mysql-bin.000001", 220),
    ]
    engine, handler, store, loader = build_engine(events, checkpoints=[initialized_checkpoint()])
    engine.run()

    assert engine.registry.latest_schema("d", "t").column_names == ("id", "a", "b", "c")
    checkpoint = store.checkpoints["p1"]
    assert checkpoint.log_pos == 220
    assert "%s:201" % UUID in checkpoint.gtid_set
    assert [record.source for record in store.history][-1] == "ddl_sqlglot"


def test_ddl_inside_a_transaction_is_committed_by_the_xid():
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (FakeGtid("%s:202" % UUID), "mysql-bin.000001", 115),
        (FakeQuery("d", "ALTER TABLE t ADD COLUMN c INT"), "mysql-bin.000001", 120),
        (FakeXid(), "mysql-bin.000001", 130),
    ]
    engine, handler, store, _ = build_engine(events, checkpoints=[initialized_checkpoint()])
    engine.run()
    assert engine.registry.latest_schema("d", "t").column_names == ("id", "a", "b", "c")
    assert store.checkpoints["p1"].log_pos == 130


def test_replayed_ddl_is_not_recorded_twice():
    events = [
        (FakeGtid("%s:203" % UUID), "mysql-bin.000001", 210),
        (FakeQuery("d", "ALTER TABLE t ADD COLUMN c INT"), "mysql-bin.000001", 220),
    ]
    engine, handler, store, _ = build_engine(events, checkpoints=[initialized_checkpoint()])
    engine.run()
    first = len(store.history)

    engine2, _, store2, _ = build_engine(events, checkpoints=[initialized_checkpoint()], store=store)
    engine2.run()
    assert len(store.history) == first
    assert engine2.registry.latest_schema("d", "t").column_names == ("id", "a", "b", "c")


def test_unparsable_ddl_falls_back_to_information_schema():
    events = [
        (FakeQuery("d", "ALTER TABLE t ADD PARTITION (PARTITION p1 VALUES LESS THAN (10))"), "mysql-bin.000001", 220),
    ]
    loader = FakeLoader(
        schemas=[make_schema("d", "t", ["id", "a", "b"], ["id"])], position=BASELINE, server_uuid=UUID, server_id=7
    )
    loader.refresh_schemas[("d", "t")] = make_schema("d", "t", ["id", "a", "b", "z"], ["id"])
    engine, handler, store, _ = build_engine(events, checkpoints=[initialized_checkpoint()], loader=loader)
    engine.run()
    assert loader.load_tables_calls == [[("d", "t")]]
    at_ddl = BinlogPosition("mysql-bin.000001", 220)
    assert engine.registry.schema_at("d", "t", at_ddl).column_names == ("id", "a", "b", "z")
    assert [record.source for record in store.history][-1] == "ddl_fallback"


def test_ddl_without_a_handler_hook_does_not_crash():
    events = [(FakeQuery("d", "ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4"), "mysql-bin.000001", 220)]
    engine, handler, store, _ = build_engine(events, checkpoints=[initialized_checkpoint()])
    engine.run()
    assert engine.registry.latest_schema("d", "t").column_names == ("id", "a", "b")


# ---------------------------------------------------------------------------
# GTID handling
# ---------------------------------------------------------------------------
def test_gtid_mode_skips_transactions_a_pipeline_already_consumed():
    """P0-5: active set must be decided by GTID, not by the file name."""
    # a different server_uuid forces GTID mode
    lead = initialized_checkpoint("p1", file="mysql-bin.000009", pos=5, gtid="%s:1-2" % UUID, server_uuid=UUID2)
    lagging = initialized_checkpoint("p2", file="mysql-bin.000001", pos=50, gtid="%s:1" % UUID, server_uuid=UUID2)
    events = [
        (FakeGtid("%s:2" % UUID), "mysql-bin.000001", 110),
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 120),
        (FakeWriteRows("d", "t", [{"values": {"UNKNOWN_COL0": 1}}], columns=3, bitmap=bytes([0b00000001])), "mysql-bin.000001", 130),
        (FakeXid(), "mysql-bin.000001", 140),
        (FakeGtid("%s:3" % UUID), "mysql-bin.000001", 150),
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 160),
        (FakeWriteRows("d", "t", [{"values": {"UNKNOWN_COL0": 2}}], columns=3, bitmap=bytes([0b00000001])), "mysql-bin.000001", 170),
        (FakeXid(), "mysql-bin.000001", 180),
    ]
    handler = make_handler()
    engine, handler, store, _ = build_engine(
        events, handler=handler, checkpoints=[lead, lagging]
    )
    engine.pipelines = ["p1", "p2"]
    engine.run()

    # GTID uuid:2 was already executed by p1 -> only p2 receives it
    delivered = [(change.after["id"], pipeline) for change, pipeline in handler.calls]
    assert delivered == [(1, "p2"), (2, "p1"), (2, "p2")]

    # each pipeline keeps its own executed set (never the union)
    assert store.checkpoints["p1"].gtid_set == "%s:1-3" % UUID
    assert store.checkpoints["p2"].gtid_set == "%s:1-3" % UUID
    assert engine.streams[0].auto_position == "%s:1" % UUID  # intersection of both


def test_server_uuid_change_without_gtid_is_fatal():
    checkpoint = Checkpoint(
        pipeline="p1",
        log_file="mysql-bin.000001",
        log_pos=10,
        gtid_set=GtidSet(),
        server_uuid=UUID2,
        is_initialized=True,
    )
    engine, _, _, _ = build_engine([], checkpoints=[checkpoint])
    with pytest.raises(FatalEngineError):
        engine.run()


def test_server_uuid_change_uses_auto_position():
    checkpoint = Checkpoint(
        pipeline="p1",
        log_file="mysql-bin.000001",
        log_pos=10,
        gtid_set=GtidSet("%s:1-5" % UUID2),
        server_uuid=UUID2,
        is_initialized=True,
    )
    events = [(FakeHeartbeat(), "mysql-bin.000001", 999)]
    engine, _, store, _ = build_engine(events, checkpoints=[checkpoint])
    engine.run()
    assert engine.streams[0].auto_position == "%s:1-5" % UUID2
    assert engine.streams[0].start is None


# ---------------------------------------------------------------------------
# initialization
# ---------------------------------------------------------------------------
def test_cold_start_initializes_pipelines_at_the_anchor():
    handler = make_handler()
    engine, handler, store, loader = build_engine([], handler=handler, anchor=ANCHOR)
    engine.run()

    assert handler.init_calls and handler.init_calls[0][0] == ["p1"]
    context = handler.init_calls[0][1]
    assert context.anchor == ANCHOR
    assert store.checkpoints["p1"].is_initialized is True
    assert store.checkpoints["p1"].log_pos == ANCHOR.pos
    assert loader.paused == 1 and loader.resumed == 1


def test_on_init_failure_still_resumes_replication_and_fails_fatal():
    handler = make_handler()

    def boom(pipelines, ctx):
        raise RuntimeError("init exploded")

    handler.on_init = boom
    engine, handler, store, loader = build_engine([], handler=handler)
    with pytest.raises(FatalEngineError):
        engine.run()
    assert loader.paused == 1
    assert loader.resumed == 1
    assert handler.failed and handler.failed[0][0] == "p1"


def test_catch_up_initializes_pending_pipeline_at_the_anchor():
    ready = initialized_checkpoint("p1", file="mysql-bin.000001", pos=50)
    pending = Checkpoint(pipeline="p2", is_initialized=False)
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 60),
        (FakeWriteRows("d", "t", [{"values": {"UNKNOWN_COL0": 1}}], columns=3, bitmap=bytes([0b00000001])), "mysql-bin.000001", 70),
        (FakeXid(), "mysql-bin.000001", 80),
        (FakeHeartbeat(), ANCHOR.file, ANCHOR.pos),
        (FakeXid(), ANCHOR.file, ANCHOR.pos),
    ]
    handler = make_handler()
    # p1 must be able to decode the pre-anchor change: give it a history version
    store = FakeStateStore(
        checkpoints=[ready, pending],
        history=[_history_record(BinlogPosition("mysql-bin.000001", 40))],
    )
    engine, handler, store, loader = build_engine(
        events, handler=handler, checkpoints=[ready, pending], store=store, anchor=ANCHOR
    )
    engine.pipelines = ["p1", "p2"]
    engine.run()

    assert [pipelines for pipelines, _ in handler.init_calls] == [["p2"]]
    # only the ready pipeline consumed the pre-anchor change
    assert [(change.after["id"], pipeline) for change, pipeline in handler.calls] == [(1, "p1")]
    assert store.checkpoints["p2"].is_initialized is True
    assert store.checkpoints["p2"].log_pos == ANCHOR.pos
    assert loader.resumed == 1


# ---------------------------------------------------------------------------
# resilience
# ---------------------------------------------------------------------------
def test_transient_stream_failure_reconnects_and_reloads_checkpoints():
    class FlakyStream:
        def __init__(self, events, on_fail) -> None:
            self.events = events
            self.on_fail = on_fail
            self.log_file = "mysql-bin.000001"
            self.log_pos = 0
            self.closed = False

        def __iter__(self):
            for event, log_file, log_pos in self.events:
                self.log_file = log_file
                self.log_pos = log_pos
                yield event
            if self.on_fail:
                raise ConnectionResetError("stream dropped")

        def close(self):
            self.closed = True

    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (FakeWriteRows("d", "t", [{"values": {"UNKNOWN_COL0": 1}}], columns=3, bitmap=bytes([0b00000001])), "mysql-bin.000001", 120),
        (FakeXid(), "mysql-bin.000001", 130),
    ]
    store = FakeStateStore(checkpoints=[initialized_checkpoint()])
    loader = FakeLoader(schemas=[make_schema("d", "t", ["id", "a", "b"], ["id"])], position=BASELINE, server_uuid=UUID, server_id=7)
    attempts = {"count": 0}

    def stream_factory(start=None, auto_position=None, blocking=True, **kwargs):
        attempts["count"] += 1
        return FlakyStream(events, on_fail=attempts["count"] == 1)

    engine = CDCEngine(
        mysql_settings(),
        pg_settings(),
        ["p1"],
        make_handler(),
        EngineConfig(namespace="ns", reconnect_initial_backoff=0.01, reconnect_max_backoff=0.02),
        store=store,
        loader=loader,
        stream_factory=stream_factory,
    )
    engine.run()

    assert attempts["count"] == 2
    assert store.reconnects == 1
    assert store.checkpoints["p1"].log_pos == 130


def test_reconnect_disabled_propagates_the_error():
    class BrokenStream:
        log_file = "mysql-bin.000001"
        log_pos = 0

        def __iter__(self):
            raise ConnectionResetError("nope")

        def close(self):
            pass

    engine, _, store, _ = build_engine([], checkpoints=[initialized_checkpoint()])
    engine.config.reconnect = False
    engine._stream_factory = lambda **kwargs: BrokenStream()
    with pytest.raises(ConnectionResetError):
        engine.run()


def test_stop_request_breaks_the_loop():
    events = [(FakeHeartbeat(), "mysql-bin.000001", 500)]
    engine, _, _, _ = build_engine(events, checkpoints=[initialized_checkpoint()])
    engine.request_stop()

    def factory(**kwargs):
        stream = FakeStream(events)
        engine.request_stop()
        return stream

    engine._stream_factory = factory
    engine.run()
    assert engine.stop_requested is True


def test_non_transactional_rows_commit_immediately():
    """MyISAM tables produce no XidEvent: the checkpoint must still advance."""
    events = [
        (
            FakeWriteRows("d", "t", [{"values": {"UNKNOWN_COL0": 1}}], columns=3, bitmap=bytes([0b00000001])),
            "mysql-bin.000001",
            420,
        ),
    ]
    engine, handler, store, _ = build_engine(events, checkpoints=[initialized_checkpoint()])
    engine.run()

    assert len(handler.calls) == 1
    assert store.checkpoints["p1"].log_pos == 420
    assert store.commits == 1


def test_filtered_tables_still_advance_the_checkpoint():
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (
            FakeWriteRows("d", "ignored", [{"values": {"UNKNOWN_COL0": 1}}], columns=1, bitmap=bytes([0b1])),
            "mysql-bin.000001",
            120,
        ),
        (FakeXid(), "mysql-bin.000001", 130),
    ]
    config = EngineConfig(namespace="ns", filter_rules=["d.ignored"])
    engine, handler, store, _ = build_engine(
        events, checkpoints=[initialized_checkpoint()], config=config
    )
    engine.run()

    assert handler.calls == []
    assert store.checkpoints["p1"].log_pos == 130


def test_unresolvable_schema_parks_the_pipeline():
    """Default policy: never skip a row silently, park the pipeline instead."""
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (
            FakeWriteRows("d", "ghost", [{"values": {"UNKNOWN_COL0": 1}}], columns=1, bitmap=bytes([0b1])),
            "mysql-bin.000001",
            120,
        ),
        (FakeXid(), "mysql-bin.000001", 130),
    ]
    loader = FakeLoader(schemas=[], position=BASELINE, server_uuid=UUID, server_id=7)
    handler = make_handler()
    engine, handler, store, _ = build_engine(
        events, handler=handler, checkpoints=[initialized_checkpoint()], loader=loader
    )
    with pytest.raises(FatalEngineError):
        engine.run()

    assert handler.calls == []
    assert store.checkpoints["p1"].log_pos == 100  # never advanced
    assert handler.failed and handler.failed[0][0] == "p1"
    assert handler.warnings and handler.warnings[-1][0] == "row event without schema"


def test_mysql_savepoints_are_mirrored_into_postgres():
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (
            FakeWriteRows("d", "t", [{"values": {"UNKNOWN_COL0": 1}}], columns=3, bitmap=bytes([0b00000001])),
            "mysql-bin.000001",
            120,
        ),
        (FakeQuery("d", "SAVEPOINT sp1"), "mysql-bin.000001", 125),
        (
            FakeWriteRows("d", "t", [{"values": {"UNKNOWN_COL0": 2}}], columns=3, bitmap=bytes([0b00000001])),
            "mysql-bin.000001",
            130,
        ),
        (FakeQuery("d", "ROLLBACK TO SAVEPOINT sp1"), "mysql-bin.000001", 135),
        (FakeQuery("d", "RELEASE SAVEPOINT sp1"), "mysql-bin.000001", 136),
        (FakeXid(), "mysql-bin.000001", 140),
    ]
    engine, handler, store, _ = build_engine(events, checkpoints=[initialized_checkpoint()])
    engine.run()

    statements = [sql for sql, _ in store.statements]
    assert 'SAVEPOINT "mysql_sp1"' in statements
    assert 'ROLLBACK TO SAVEPOINT "mysql_sp1"' in statements
    assert 'RELEASE SAVEPOINT "mysql_sp1"' in statements
    # the row inside the savepoint was still delivered, the rollback is what
    # discards it in PostgreSQL
    assert len(handler.calls) == 2
    assert store.checkpoints["p1"].log_pos == 140


def test_short_bitmap_is_reported(caplog):
    """A bitmap shorter than the column list must not silently trim payloads."""
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        # 9 columns in the schema, but the present-image bitmap only has 8 bits
        (
            FakeWriteRows("d", "t", [{"values": {"UNKNOWN_COL0": 1}}], columns=9, bitmap=bytes([0b00000001])),
            "mysql-bin.000001",
            120,
        ),
        (FakeXid(), "mysql-bin.000001", 130),
    ]
    loader = FakeLoader(
        schemas=[make_schema("d", "t", ["a", "b", "c", "d", "e", "f", "g", "h", "i"], ["a"])],
        position=BASELINE,
        server_uuid=UUID,
        server_id=7,
    )
    engine, handler, store, _ = build_engine(events, checkpoints=[initialized_checkpoint()], loader=loader)
    with caplog.at_level("WARNING", logger="mylox.engine.engine"):
        engine.run()

    assert handler.calls[0][0].after == {"a": 1}
    assert any("covers 8 bits" in record.message for record in caplog.records)


def test_prepare_failure_still_resumes_replication():
    """A failure *after* pausing (e.g. an unusable anchor) must not leave the
    replica stopped."""
    loader = FakeLoader(
        schemas=[make_schema("d", "t", ["id"], ["id"])],
        position=ANCHOR,
        server_uuid=UUID,
        server_id=7,
        pause_succeeds=True,
    )
    loader.position_value = None  # master_status() returns no coordinates
    engine, handler, store, _ = build_engine([], loader=loader, anchor=ANCHOR)
    with pytest.raises(ReaderError):
        engine.run()
    assert loader.paused == 1
    assert loader.resumed == 1  # START REPLICA was issued in the finally block


def test_strict_pause_failure_aborts_without_touching_replication_again():
    loader = FakeLoader(
        schemas=[make_schema("d", "t", ["id"], ["id"])],
        position=ANCHOR,
        server_uuid=UUID,
        server_id=7,
        pause_succeeds=False,
    )
    engine, handler, store, _ = build_engine([], loader=loader, anchor=ANCHOR)
    engine.config.strict_pause = True
    with pytest.raises(FatalEngineError):
        engine.run()
    assert loader.paused == 1
    assert loader.resumed == 0  # nothing was ever paused


def test_unknown_alter_clause_falls_back_and_advances():
    events = [
        (FakeQuery("d", "ALTER TABLE t FORCE, ALGORITHM=INPLACE"), "mysql-bin.000001", 220),
    ]
    engine, handler, store, loader = build_engine(events, checkpoints=[initialized_checkpoint()])
    engine.run()
    assert store.checkpoints["p1"].log_pos == 220


def test_statement_format_dml_fails_by_default():
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (FakeQuery("d", "INSERT INTO t VALUES (1)"), "mysql-bin.000001", 120),
        (FakeXid(), "mysql-bin.000001", 130),
    ]
    engine, handler, store, _ = build_engine(events, checkpoints=[initialized_checkpoint()])
    with pytest.raises(FatalEngineError):
        engine.run()


def test_statement_format_dml_can_be_downgraded_to_a_warning():
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (FakeQuery("d", "UPDATE t SET a = 1"), "mysql-bin.000001", 120),
        (FakeXid(), "mysql-bin.000001", 130),
    ]
    config = EngineConfig(namespace="ns", on_statement_dml="warn")
    engine, handler, store, _ = build_engine(events, checkpoints=[initialized_checkpoint()], config=config)
    engine.run()
    # the statement was not applied, so the checkpoint deliberately stays put:
    # re-reading it is safer than pretending it was consumed
    assert store.checkpoints["p1"].log_pos == 100
    assert handler.calls == []


# ---------------------------------------------------------------------------
# regression guards for the review findings
# ---------------------------------------------------------------------------
def test_pending_pipeline_is_initialized_when_the_stream_starts_at_the_anchor():
    """P0: adding a pipeline to a caught-up engine must not hang forever."""
    ready = initialized_checkpoint("p1", file="mysql-bin.000001", pos=ANCHOR.pos)
    pending = Checkpoint(pipeline="p2", is_initialized=False)
    events = [(FakeHeartbeat(), ANCHOR.file, ANCHOR.pos)]
    handler = make_handler()
    engine, handler, store, loader = build_engine(
        events, handler=handler, checkpoints=[ready, pending], anchor=ANCHOR
    )
    engine.pipelines = ["p1", "p2"]
    engine.run()

    assert [names for names, _ in handler.init_calls] == [["p2"]]
    assert store.checkpoints["p2"].is_initialized is True
    assert store.checkpoints["p2"].log_pos == ANCHOR.pos
    assert loader.paused == 1 and loader.resumed == 1


def test_pending_pipeline_is_initialized_even_when_only_a_ddl_crosses_the_anchor():
    ready = initialized_checkpoint("p1", file="mysql-bin.000001", pos=50)
    pending = Checkpoint(pipeline="p2", is_initialized=False)
    events = [
        (FakeGtid("%s:300" % UUID), ANCHOR.file, ANCHOR.pos - 5),
        (FakeQuery("d", "ALTER TABLE t ADD COLUMN c INT"), ANCHOR.file, ANCHOR.pos),
        (FakeHeartbeat(), ANCHOR.file, ANCHOR.pos),
    ]
    handler = make_handler()
    engine, handler, store, loader = build_engine(
        events, handler=handler, checkpoints=[ready, pending], anchor=ANCHOR
    )
    engine.pipelines = ["p1", "p2"]
    engine.run()

    assert [names for names, _ in handler.init_calls] == [["p2"]]
    assert store.checkpoints["p2"].is_initialized is True
    assert loader.resumed == 1


def test_failed_checkpoint_commit_does_not_move_the_in_memory_position():
    """P1: memory must never run ahead of the durable checkpoint."""
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (FakeWriteRows("d", "t", [{"values": {"UNKNOWN_COL0": 1}}], columns=3, bitmap=bytes([0b00000001])), "mysql-bin.000001", 120),
        (FakeXid(), "mysql-bin.000001", 130),
    ]
    store = FakeStateStore(checkpoints=[initialized_checkpoint()])
    store.fail_commit = True
    engine, handler, store, _ = build_engine(
        events, checkpoints=[initialized_checkpoint()], store=store
    )
    engine.config.reconnect = False
    with pytest.raises(RuntimeError):
        engine.run()

    assert engine.pipeline_set.get("p1").checkpoint.log_pos == 100
    assert store.checkpoints["p1"].log_pos == 100


def test_source_failover_during_reconnect_switches_to_gtid():
    """P1: a uuid change must be noticed on the reconnect path as well."""
    checkpoint = initialized_checkpoint(gtid="%s:1-5" % UUID)
    store = FakeStateStore(checkpoints=[checkpoint])
    loader = FakeLoader(
        schemas=[make_schema("d", "t", ["id", "a", "b"], ["id"])],
        position=BASELINE,
        server_uuid=UUID,
        server_id=7,
    )
    ids = {"count": 0, "start": "unset", "auto_position": "unset"}

    class FailingStream(FakeStream):
        def __iter__(self):
            yield FakeHeartbeat()
            raise ConnectionResetError("stream dropped")

    def stream_factory(start=None, auto_position=None, blocking=True, **kwargs):
        ids["count"] += 1
        ids["start"] = start
        ids["auto_position"] = auto_position
        if ids["count"] == 1:
            loader.server_uuid = UUID2  # the replica is rebuilt while we stream
        return FailingStream([])

    engine = CDCEngine(
        mysql_settings(),
        pg_settings(),
        ["p1"],
        make_handler(),
        EngineConfig(
            namespace="ns",
            reconnect_initial_backoff=0.01,
            reconnect_max_backoff=0.02,
            reconnect_max_attempts=1,
        ),
        store=store,
        loader=loader,
        stream_factory=stream_factory,
    )
    with pytest.raises(ConnectionResetError):
        engine.run()

    assert ids["count"] == 2
    assert engine._gtid_mode is True
    # the second attempt resumes with GTID, not with the old server's log file
    assert ids["auto_position"] == "%s:1-5" % UUID
    assert ids["start"] is None


def test_blocked_pipeline_stays_blocked_for_the_whole_transaction():
    """P2: a later filtered row must not re-enable a blocked checkpoint."""
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (FakeGtid("%s:400" % UUID), "mysql-bin.000001", 112),
        # d.ghost has no schema -> the pipeline is blocked for this transaction
        (FakeWriteRows("d", "ghost", [{"values": {"UNKNOWN_COL0": 1}}], columns=1, bitmap=bytes([0b1])), "mysql-bin.000001", 115),
        # a filtered table would otherwise re-activate it
        (FakeWriteRows("d", "ignored", [{"values": {"UNKNOWN_COL0": 1}}], columns=1, bitmap=bytes([0b1])), "mysql-bin.000001", 118),
        (FakeXid(), "mysql-bin.000001", 120),
    ]
    config = EngineConfig(namespace="ns", filter_rules=["d.ignored"], on_schema_error="skip")
    engine, handler, store, _ = build_engine(events, checkpoints=[initialized_checkpoint()], config=config)
    engine.run()

    assert store.checkpoints["p1"].log_pos == 100  # untouched
    assert store.checkpoints["p1"].gtid_set.is_empty()
    assert handler.calls == []


def test_schema_error_parks_every_pipeline_that_would_have_received_the_row():
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (
            FakeWriteRows("d", "ghost", [{"values": {"UNKNOWN_COL0": 1}}], columns=1, bitmap=bytes([0b1])),
            "mysql-bin.000001",
            120,
        ),
        (FakeXid(), "mysql-bin.000001", 130),
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 140),
        (
            FakeWriteRows("d", "t", [{"values": {"UNKNOWN_COL0": 1}}], columns=3, bitmap=bytes([0b00000001])),
            "mysql-bin.000001",
            150,
        ),
        (FakeXid(), "mysql-bin.000001", 160),
    ]
    config = EngineConfig(namespace="ns", on_schema_error="drop")
    handler = make_handler()
    store = FakeStateStore(checkpoints=[initialized_checkpoint("p1"), initialized_checkpoint("p2")])
    loader = FakeLoader(
        schemas=[make_schema("d", "t", ["id", "a", "b"], ["id"])],
        position=BASELINE,
        server_uuid=UUID,
        server_id=7,
    )
    engine, handler, store, _ = build_engine(
        events, handler=handler, checkpoints=[initialized_checkpoint("p1"), initialized_checkpoint("p2")],
        store=store, loader=loader, config=config,
    )
    engine.pipelines = ["p1", "p2"]
    # every pipeline consumes the same stream, so the ghost table parks all of
    # them and the engine stops instead of silently dropping the row
    with pytest.raises(FatalEngineError):
        engine.run()

    assert sorted(name for name, _ in handler.failed) == ["p1", "p2"]
    assert store.checkpoints["p1"].log_pos == 100
    assert store.checkpoints["p2"].log_pos == 100


def test_schema_error_skip_policy_keeps_going():
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (
            FakeWriteRows("d", "ghost", [{"values": {"UNKNOWN_COL0": 1}}], columns=1, bitmap=bytes([0b1])),
            "mysql-bin.000001",
            120,
        ),
        (FakeXid(), "mysql-bin.000001", 130),
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 140),
        (
            FakeWriteRows("d", "t", [{"values": {"UNKNOWN_COL0": 1}}], columns=3, bitmap=bytes([0b00000001])),
            "mysql-bin.000001",
            150,
        ),
        (FakeXid(), "mysql-bin.000001", 160),
    ]
    config = EngineConfig(namespace="ns", on_schema_error="skip")
    handler = make_handler()
    engine, handler, store, _ = build_engine(
        events, handler=handler, checkpoints=[initialized_checkpoint()], config=config
    )
    engine.run()

    # the ghost row is skipped, the following transaction is still delivered and
    # the checkpoint only moves with it
    assert [change.after for change, _ in handler.calls] == [{"id": 1}]
    assert store.checkpoints["p1"].log_pos == 160


def test_rename_replay_does_not_fall_back_or_duplicate_history():
    """P3: re-applying the same RENAME at the same position is a no-op."""
    events = [(FakeQuery("d", "RENAME TABLE d.t TO d.t2"), "mysql-bin.000001", 220)]
    engine, handler, store, loader = build_engine(events, checkpoints=[initialized_checkpoint()])
    engine.run()
    first_history = len(store.history)
    assert loader.load_tables_calls == []

    engine2, _, store2, loader2 = build_engine(
        events, checkpoints=[initialized_checkpoint()], store=store
    )
    engine2._prepare()
    report = engine2.schema_manager.handle_ddl(
        "RENAME TABLE d.t TO d.t2", BinlogPosition("mysql-bin.000001", 220), default_db="d"
    )
    assert report.mode == "sqlglot"
    assert len(store.history) == first_history
    assert loader2.load_tables_calls == []


# ---------------------------------------------------------------------------
# decode robustness (found against a real BLOB column)
# ---------------------------------------------------------------------------
def test_undecodable_row_event_parks_the_pipeline_instead_of_looping():
    """A row the driver cannot decode must not reconnect forever."""

    class BrokenRows(FakeWriteRows):
        @property
        def rows(self):
            raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")

    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (BrokenRows("d", "t", [], columns=3, bitmap=bytes([0b00000001])), "mysql-bin.000001", 120),
        (FakeXid(), "mysql-bin.000001", 130),
    ]
    handler = make_handler()
    engine, handler, store, _ = build_engine(events, handler=handler, checkpoints=[initialized_checkpoint()])
    with pytest.raises(FatalEngineError):
        engine.run()

    assert handler.calls == []
    assert store.checkpoints["p1"].log_pos == 100  # never advanced past it
    assert handler.failed and handler.failed[0][0] == "p1"
    assert any(warning[0] == "undecodable row event" for warning in handler.warnings)


def test_undecodable_row_event_can_be_skipped():
    class BrokenRows(FakeWriteRows):
        @property
        def rows(self):
            raise ValueError("unsupported column type")

    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (BrokenRows("d", "t", [], columns=3, bitmap=bytes([0b00000001])), "mysql-bin.000001", 120),
        (FakeXid(), "mysql-bin.000001", 130),
    ]
    config = EngineConfig(namespace="ns", on_schema_error="skip")
    handler = make_handler()
    engine, handler, store, _ = build_engine(
        events, handler=handler, checkpoints=[initialized_checkpoint()], config=config
    )
    engine.run()

    assert handler.failed == []
    assert store.checkpoints["p1"].log_pos == 100  # blocked for this transaction


def test_binary_columns_are_reported_once(caplog):
    events = []
    loader = FakeLoader(
        schemas=[
            make_schema("d", "blobby", ["id", "payload"], ["id"], types={"payload": "longblob"}),
            make_schema("d", "plain", ["id"], ["id"]),
        ],
        position=BASELINE,
        server_uuid=UUID,
        server_id=7,
    )
    engine, _, _, _ = build_engine(events, checkpoints=[initialized_checkpoint()], loader=loader)
    with caplog.at_level("WARNING", logger="mylox.engine.engine"):
        engine.run()

    messages = [record.getMessage() for record in caplog.records]
    assert any("binary column(s) payload" in message for message in messages)
    assert not any("plain" in message for message in messages)


def test_unsigned_bigint_payload_is_not_wrapped_negative():
    """A real-world case: the max `bigint unsigned` arrives as -1."""
    events = [
        (FakeQuery("d", "BEGIN"), "mysql-bin.000001", 110),
        (
            FakeWriteRows(
                "d",
                "t",
                [{"values": {"UNKNOWN_COL0": 1, "UNKNOWN_COL1": -1}}],
                columns=2,
                bitmap=bytes([0b11]),
            ),
            "mysql-bin.000001",
            120,
        ),
        (FakeXid(), "mysql-bin.000001", 130),
    ]
    loader = FakeLoader(
        schemas=[make_schema("d", "t", ["id", "amount"], ["id"], types={"amount": "bigint(20) unsigned"})],
        position=BASELINE,
        server_uuid=UUID,
        server_id=7,
    )
    engine, handler, store, _ = build_engine(events, checkpoints=[initialized_checkpoint()], loader=loader)
    engine.run()

    assert handler.calls[0][0].after == {"id": 1, "amount": 18446744073709551615}
