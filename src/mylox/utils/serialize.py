"""Payload sanitising helpers."""

from __future__ import annotations

from decimal import Decimal
from typing import Any


def _decode(value: bytes) -> Any:
    """``bytes`` -> ``str`` when it is valid UTF-8, otherwise the raw bytes.

    Silently replacing undecodable bytes (the original behaviour) turns binary
    data into plausible looking text, which is worse than handing the raw bytes
    to the handler: psycopg maps ``bytes`` onto ``bytea`` and a handler can
    always ``hex()`` them, while corrupted text cannot be undone.
    """
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return value


def stringify_bytes(obj: Any) -> Any:
    """Recursively decode ``bytes`` so payloads are JSON/psycopg safe.

    Column *names* are always converted to ``str`` (a mapping key must be one);
    values keep their raw bytes when they are not valid UTF-8.
    """
    if isinstance(obj, (bytes, bytearray)):
        return _decode(bytes(obj))
    if isinstance(obj, dict):
        return {
            (k.decode("utf-8", errors="replace") if isinstance(k, bytes) else str(k)): stringify_bytes(v)
            for k, v in obj.items()
        }
    if isinstance(obj, (list, tuple, set)):
        return [stringify_bytes(item) for item in obj]
    if isinstance(obj, Decimal):
        return obj
    return obj
