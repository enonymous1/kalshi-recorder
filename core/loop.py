"""Event loop management: uvloop where supported, default elsewhere."""

from __future__ import annotations

import asyncio


def install_loop() -> str:
    """Install the best available event-loop policy. Returns the loop name."""
    try:
        import uvloop  # noqa: F401  (no Windows support; guard is the point)

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        return "uvloop"
    except ImportError:
        return "asyncio-default"
