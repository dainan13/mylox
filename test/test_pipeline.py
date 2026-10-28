"""Per-pipeline checkpoints and active set decisions (P0-1 / P0-5)."""

from __future__ import annotations

import pytest

from mylox.engine.pipeline import PipelineSet, PipelineState
from mylox.errors import FatalEngineError
from mylox.gtid import GtidSet
from mylox.state.models import Checkpoint
from mylox.utils.positions import BinlogPosition

UUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
UUID2 = "11111111-2222-3333-4444-555555555555"
P100 = BinlogPosition("mysql-bin.000001", 100)
P200 = BinlogPosition("mysql-bin.000001", 200)


def checkpoint(name="p1", file="mysql-bin.000001", pos=100, gtid="", uuid=UUID, initialized=True):
    return Checkpoint(
        pipeline=name,
        log_file=file,
        log_pos=pos,
        gtid_set=GtidSet(gtid) if gtid else GtidSet(),
        server_uuid=uuid,
        server_id=1,
        is_initialized=initialized,
    )


def test_position_activity():
    state = PipelineState(checkpoint(pos=100))
    assert state.is_active_for_position(P200) is True
    assert state.is_active_for_position(P100) is True
    assert state.is_active_for_position(BinlogPosition("mysql-bin.000001", 99)) is False
    assert state.is_active_for_position(None) is False


def test_gtid_activity_uses_the_executed_set():
    state = PipelineState(checkpoint(gtid="%s:1-5" % UUID))
    assert state.is_active_for_gtid("%s:3" % UUID) is False
    assert state.is_active_for_gtid("%s:6" % UUID) is True
    assert state.is_active_for_gtid(None) is False


def test_advance_merges_the_executed_set_and_never_shrinks():
    state = PipelineState(checkpoint(gtid="%s:1-5" % UUID))
    session = GtidSet("%s:6-8" % UUID)
    state.advance(P200, session, "uuid-x", 9)
    assert str(state.gtid_set) == "%s:1-8" % UUID
    assert state.position == P200
    assert state.checkpoint.server_uuid == "uuid-x"


def test_mark_initialized_merges_anchor_gtids():
    state = PipelineState(checkpoint(initialized=False))
    state.mark_initialized(P200, GtidSet("%s:1-3" % UUID), UUID, 1)
    assert state.initialized is True
    assert state.is_active_for_gtid("%s:2" % UUID) is False
    assert state.is_active_for_gtid("%s:4" % UUID) is True


def test_pipeline_set_active_pipelines():
    states = [
        PipelineState(checkpoint("lead", file="mysql-bin.000009", pos=5, gtid="%s:1-2" % UUID)),
        PipelineState(checkpoint("lag", gtid="%s:1" % UUID)),
    ]
    pipelines = PipelineSet([state.checkpoint for state in states])
    # mysql-bin.000009 is *after* mysql-bin.000001: the lead already consumed it
    assert pipelines.active_pipelines(P100, None, gtid_mode=False) == {"lag"}
    assert pipelines.active_pipelines(BinlogPosition("mysql-bin.000010", 1), None, gtid_mode=False) == {
        "lead",
        "lag",
    }
    # GTID mode: `lead` already executed uuid:2
    assert pipelines.active_pipelines(P100, "%s:2" % UUID, gtid_mode=True) == {"lag"}
    assert pipelines.active_pipelines(P100, "%s:3" % UUID, gtid_mode=True) == {"lead", "lag"}


def test_pipeline_set_start_position_is_the_earliest():
    pipelines = PipelineSet(
        [
            checkpoint("a", file="mysql-bin.000002", pos=5),
            checkpoint("b", file="mysql-bin.000010", pos=1),
            checkpoint("c", file="mysql-bin.000001", pos=999999),
        ]
    )
    assert pipelines.start_position() == BinlogPosition("mysql-bin.000001", 999999)


def test_gtid_position_is_the_intersection():
    pipelines = PipelineSet(
        [
            checkpoint("a", gtid="%s:1-10" % UUID),
            checkpoint("b", gtid="%s:4-20" % UUID),
        ]
    )
    assert str(pipelines.gtid_position()) == "%s:4-10" % UUID


def test_failed_and_pending_pipelines_are_excluded():
    pipelines = PipelineSet(
        [
            checkpoint("ready"),
            checkpoint("pending", initialized=False),
            checkpoint("broken"),
        ]
    )
    pipelines.drop("broken", "boom")
    assert [state.name for state in pipelines.ready] == ["ready"]
    assert [state.name for state in pipelines.pending_init] == ["pending"]
    assert pipelines.active_pipelines(P200, None, gtid_mode=False) == {"ready"}
    assert pipelines.has_ready() is True


def test_server_uuid_change_requires_gtid():
    same = PipelineSet([checkpoint("p1")])
    assert same.require_gtid_resume(UUID) is False
    assert same.validate_gtid_resume(UUID) is False

    changed = PipelineSet([checkpoint("p1", uuid=UUID2, gtid="%s:1-3" % UUID2)])
    assert changed.require_gtid_resume(UUID) is True
    assert changed.validate_gtid_resume(UUID) is True

    no_gtid = PipelineSet([checkpoint("p1", uuid=UUID2)])
    with pytest.raises(FatalEngineError):
        no_gtid.validate_gtid_resume(UUID)
