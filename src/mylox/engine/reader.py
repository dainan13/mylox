"""Thin factory around :class:`pymysqlreplication.BinLogStreamReader`.

Wrapping the reader keeps the engine testable (the factory can be replaced by a
fake stream) and centralises the options that used to be hardcoded - most
importantly ``server_id``, which was ``99999`` for every process and made two
CDC instances evict each other.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pymysqlreplication import BinLogStreamReader

from ..config import MySQLSettings
from ..errors import ReaderError
from ..logging_utils import get_logger
from ..utils.positions import BinlogPosition

logger = get_logger(__name__)


class BinlogReaderFactory:
    """Create ``BinLogStreamReader`` instances."""

    def __init__(
        self,
        settings: MySQLSettings,
        server_id: int,
        heartbeat: float = 1.0,
        extra_options: Optional[Dict[str, Any]] = None,
        ignore_decode_errors: bool = True,
    ) -> None:
        self.settings = settings
        self.server_id = server_id
        self.heartbeat = heartbeat
        self.extra_options = dict(extra_options or {})
        #: ``False`` makes the driver raise on non UTF-8 column data (a BLOB
        #: value, for instance), which would otherwise wedge the engine.
        self.ignore_decode_errors = ignore_decode_errors

    def create(
        self,
        start: Optional[BinlogPosition] = None,
        auto_position: Optional[str] = None,
        blocking: bool = True,
    ) -> BinLogStreamReader:
        options: Dict[str, Any] = {
            "connection_settings": self.settings.stream_settings(),
            "server_id": self.server_id,
            "blocking": blocking,
            "resume_stream": True,
            # Names come from mylox's own schema model; letting
            # pymysqlreplication query information_schema per event would defeat
            # the whole point.
            "use_column_name_cache": False,
            "slave_heartbeat": self.heartbeat,
            "ignore_decode_errors": self.ignore_decode_errors,
        }
        if auto_position:
            options["auto_position"] = auto_position
            options["log_file"] = None
            options["log_pos"] = None
        else:
            options["auto_position"] = None
            options["log_file"] = start.file if start else None
            options["log_pos"] = start.pos if start else None
        options.update(self.extra_options)

        try:
            return BinLogStreamReader(**options)
        except Exception as exc:  # pragma: no cover - driver specific
            raise ReaderError("could not open the binlog stream: %s" % (exc,)) from exc

    def close(self, stream: Any) -> None:
        try:
            if stream is not None:
                stream.close()
        except Exception as exc:  # pragma: no cover - best effort
            logger.debug("error while closing the binlog stream: %s", exc)
