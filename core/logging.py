"""Centralized logging configuration.

Usage:
    from core.logging import get_logger, setup_logging

    setup_logging(level="INFO")  # once at startup
    LOG = get_logger(__name__)   # in each module

Log levels by component (per architectural audit):
- DEBUG: socket payload drops, WS message details, per-event loop ticks
- INFO: event loop ticks, file flushes, universe selection, gauntlet progress
- WARNING: API 429 backoffs, retry attempts, degraded data
- ERROR: venue timeouts, exceptions, validation failures (e.g. bid > ask)
"""

from __future__ import annotations

import logging
import sys


_configured = False


def setup_logging(level: str = "INFO", fmt: str | None = None) -> None:
    """Configure root logger once. Safe to call multiple times."""
    global _configured
    if _configured:
        return

    fmt = fmt or "%(asctime)s %(name)s %(levelname)s %(message)s"
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S"))

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    # Avoid duplicate handlers if called twice
    if not root.handlers:
        root.addHandler(handler)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Get a module-level logger. Call setup_logging() first at startup."""
    return logging.getLogger(name)
