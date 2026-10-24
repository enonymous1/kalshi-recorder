"""Networking utilities: TCP_NODELAY, token-bucket rate limiting, backoff."""

from __future__ import annotations

import asyncio
import socket
import time

import aiohttp

from core.logging import get_logger

LOG = get_logger(__name__)


class NoDelayConnector(aiohttp.TCPConnector):
    """TCPConnector that sets TCP_NODELAY on every socket (portable)."""

    async def _wrap_create_connection(self, *args, **kwargs):
        transport, protocol = await super()._wrap_create_connection(*args, **kwargs)
        sock = transport.get_extra_info("socket")
        if sock is not None:
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
        return transport, protocol


class TokenBucket:
    """Async token-bucket rate limiter (REST read budget)."""

    def __init__(self, rate_per_s: float, burst: int = 10) -> None:
        self._rate = rate_per_s
        self._burst = burst
        self._tokens = float(burst)
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def take(self, n: float = 1.0) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(self._burst, self._tokens + (now - self._updated) * self._rate)
                self._updated = now
                if self._tokens >= n:
                    self._tokens -= n
                    return
                await asyncio.sleep((n - self._tokens) / self._rate)


def backoff_delay(attempt: int, initial_s: float, max_s: float) -> float:
    """Exponential backoff with jitter, capped."""
    import random

    delay = min(max_s, initial_s * (2 ** attempt))
    return delay * (0.8 + 0.4 * random.random())


async def sleep_or_stop(stop: asyncio.Event, delay: float) -> bool:
    """Sleep up to delay seconds; return True if stop was set during the wait."""
    if delay <= 0:
        return stop.is_set()
    try:
        await asyncio.wait_for(stop.wait(), timeout=delay)
        return True
    except asyncio.TimeoutError:
        return False
