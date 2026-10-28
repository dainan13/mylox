"""Multi-pipeline bookkeeping.

Each pipeline has its own durable checkpoint.  A row event is delivered to a
pipeline only when that pipeline still has to consume it:

* file/pos mode - when the pipeline's checkpoint is *behind* the event;
* GTID mode -- when the event's GTID is **not** part of the pipeline's executed
  set.  Comparing binlog file names across a failover is meaningless, which is
  exactly the bug the old implementation had.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Set

from ..errors import FatalEngineError
from ..gtid import GtidSet, intersection
from ..logging_utils import get_logger
from ..state.models import Checkpoint
from ..utils.positions import BinlogPosition, is_pos_greater_or_equal

logger = get_logger(__name__)


class PipelineState:
    """A pipeline's checkpoint plus its runtime status."""

    def __init__(self, checkpoint: Checkpoint) -> None:
        self.checkpoint = checkpoint
        self.failed = False
        self.failure_reason: Optional[str] = None
        self.initialized = checkpoint.is_initialized

    # -- identity ------------------------------------------------------
    @property
    def name(self) -> str:
        return self.checkpoint.pipeline

    @property
    def position(self) -> Optional[BinlogPosition]:
        return self.checkpoint.position

    @property
    def gtid_set(self) -> GtidSet:
        return self.checkpoint.gtid_set

    # -- decisions -----------------------------------------------------
    def is_active_for_position(self, position: Optional[BinlogPosition]) -> bool:
        if position is None:
            return False
        own = self.position
        if own is None:
            return True
        return is_pos_greater_or_equal(position.file, position.pos, own.file, own.pos)

    def is_active_for_gtid(self, gtid: Optional[str]) -> bool:
        if not gtid:
            return False
        try:
            return gtid not in self.checkpoint.gtid_set
        except Exception:  # pragma: no cover - defensive
            return True

    def is_active(self, position: Optional[BinlogPosition], gtid: Optional[str], gtid_mode: bool) -> bool:
        if gtid_mode:
            if gtid:
                return self.is_active_for_gtid(gtid)
            return self.is_active_for_position(position)
        return self.is_active_for_position(position)

    # -- mutation ------------------------------------------------------
    def advance(self, position: BinlogPosition, session_gtids: GtidSet, server_uuid: Optional[str], server_id: Optional[int]) -> None:
        self.checkpoint.log_file = position.file
        self.checkpoint.log_pos = position.pos
        if session_gtids is not None and not session_gtids.is_empty():
            merged = self.checkpoint.gtid_set.copy()
            merged.update(session_gtids)
            self.checkpoint.gtid_set = merged
        self.checkpoint.server_uuid = server_uuid
        self.checkpoint.server_id = server_id

    def mark_initialized(self, position: Optional[BinlogPosition], gtid_set: GtidSet, server_uuid: Optional[str], server_id: Optional[int]) -> None:
        self.initialized = True
        self.checkpoint.is_initialized = True
        if position is not None:
            self.checkpoint.log_file = position.file
            self.checkpoint.log_pos = position.pos
        if gtid_set is not None and not gtid_set.is_empty():
            merged = self.checkpoint.gtid_set.copy()
            merged.update(gtid_set)
            self.checkpoint.gtid_set = merged
        self.checkpoint.server_uuid = server_uuid
        self.checkpoint.server_id = server_id

    def fail(self, reason: str) -> None:
        self.failed = True
        self.failure_reason = reason

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return "PipelineState(%s, pos=%s, failed=%s)" % (self.name, self.position, self.failed)


class PipelineSet:
    """All pipelines served by one engine instance."""

    def __init__(self, checkpoints: Iterable[Checkpoint]) -> None:
        self._pipelines: Dict[str, PipelineState] = {}
        for checkpoint in checkpoints:
            self._pipelines[checkpoint.pipeline] = PipelineState(checkpoint)

    # -- container protocol --------------------------------------------
    def __contains__(self, name: object) -> bool:
        return name in self._pipelines

    def __len__(self) -> int:
        return len(self._pipelines)

    def __iter__(self):
        return iter(self._pipelines.values())

    def get(self, name: str) -> Optional[PipelineState]:
        return self._pipelines.get(name)

    def names(self) -> List[str]:
        return list(self._pipelines.keys())

    def states(self) -> List[PipelineState]:
        return list(self._pipelines.values())

    # -- status --------------------------------------------------------
    @property
    def ready(self) -> List[PipelineState]:
        return [state for state in self._pipelines.values() if state.initialized and not state.failed]

    @property
    def pending_init(self) -> List[PipelineState]:
        return [state for state in self._pipelines.values() if not state.initialized and not state.failed]

    @property
    def failed(self) -> List[PipelineState]:
        return [state for state in self._pipelines.values() if state.failed]

    def has_ready(self) -> bool:
        return bool(self.ready)

    # -- decisions -----------------------------------------------------
    def active_pipelines(
        self, position: Optional[BinlogPosition], gtid: Optional[str], gtid_mode: bool
    ) -> Set[str]:
        return {
            state.name
            for state in self._pipelines.values()
            if not state.failed and state.initialized and state.is_active(position, gtid, gtid_mode)
        }

    def start_position(self) -> Optional[BinlogPosition]:
        """Earliest checkpoint among the ready pipelines."""
        earliest: Optional[BinlogPosition] = None
        for state in self.ready:
            position = state.position
            if position is None:
                continue
            if earliest is None or position < earliest:
                earliest = position
        return earliest

    def gtid_position(self) -> GtidSet:
        """Set of GTIDs consumed by **every** ready pipeline.

        ``auto_position`` replays everything missing from the supplied set, so
        the intersection is what serves the slowest consumer; each pipeline then
        filters out the transactions it already executed (see
        :meth:`PipelineState.is_active_for_gtid`).
        """
        sets = [state.gtid_set for state in self.ready]
        if not sets:
            return GtidSet()
        return intersection(sets)

    def server_uuid(self) -> Optional[str]:
        for state in self.ready:
            if state.checkpoint.server_uuid:
                return state.checkpoint.server_uuid
        return None

    # -- mutation ------------------------------------------------------
    def mark_initialized(
        self,
        names: Iterable[str],
        position: Optional[BinlogPosition],
        gtid_set: Optional[GtidSet],
        server_uuid: Optional[str],
        server_id: Optional[int],
    ) -> None:
        empty = GtidSet()
        for name in names:
            state = self._pipelines.get(name)
            if state is None:
                continue
            state.mark_initialized(position, gtid_set or empty, server_uuid, server_id)

    def drop(self, name: str, reason: str) -> None:
        state = self._pipelines.get(name)
        if state is None:
            return
        state.fail(reason)
        logger.error("pipeline [%s] dropped: %s", name, reason)

    def require_gtid_resume(self, current_server_uuid: Optional[str]) -> bool:
        """``True`` when the source changed and GTID mode is required."""
        previous = self.server_uuid()
        if not previous or not current_server_uuid:
            return False
        return previous != current_server_uuid

    def validate_gtid_resume(self, current_server_uuid: Optional[str]) -> bool:
        """Decide whether to resume with ``auto_position``.

        Raises :class:`FatalEngineError` when the source changed but a pipeline
        has no GTID set to resume from: silently falling back to file/pos would
        re-read or skip arbitrary transactions.
        """
        if not self.require_gtid_resume(current_server_uuid):
            return False
        for state in self.ready:
            if not state.initialized:
                continue
            if state.gtid_set.is_empty():
                raise FatalEngineError(
                    "source server_uuid changed (%s -> %s) but pipeline [%s] has no gtid_set; "
                    "refusing to guess a resume position"
                    % (state.checkpoint.server_uuid, current_server_uuid, state.name)
                )
        return True
