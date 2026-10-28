"""Logging helpers (thin wrapper so library users can stay in control)."""

from __future__ import annotations

import logging
import sys

DEFAULT_LOGGER_NAME = "mylox"


def get_logger(name: str = DEFAULT_LOGGER_NAME) -> logging.Logger:
    if name == DEFAULT_LOGGER_NAME or name.startswith(DEFAULT_LOGGER_NAME + "."):
        return logging.getLogger(name)
    return logging.getLogger("%s.%s" % (DEFAULT_LOGGER_NAME, name))


def configure_logging(level: object = logging.INFO, stream=None) -> None:
    """Attach a simple stderr handler to the ``mylox`` logger once."""
    logger = logging.getLogger(DEFAULT_LOGGER_NAME)
    logger.setLevel(level)
    if any(getattr(handler, "_mylox_handler", False) for handler in logger.handlers):
        return
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)-7s [%(name)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    )
    handler._mylox_handler = True  # type: ignore[attr-defined]
    logger.addHandler(handler)
