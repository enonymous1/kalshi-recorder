#!/usr/bin/env python3
"""Kalshi REST client with rate limiting, 429 backoff, and auth support."""

from __future__ import annotations

import asyncio
import logging
import time

import aiohttp
import orjson

from data.networking import NoDelayConnector, TokenBucket, backoff_delay, sleep_or_stop

LOG = logging.getLogger("recorder")


class KalshiThrottleTimeout(RuntimeError):
    """A request burned the full 429-backoff budget (REC_MAX_429_WAIT_S), or
    the venue tripwire was already tripped by another request.

    Venue-level, not a bad request: the venue is hard-throttled. Callers
    should abort the run and let the supervisor restart (the incremental
    cache preserves enumeration progress), not swallow this per-call."""


class KalshiREST:
    def __init__(self, settings, bucket: TokenBucket,
                 stop: asyncio.Event | None = None) -> None:
        import yarl

        self.s = settings
        self.bucket = bucket
        self.stop = stop
        self._base = yarl.URL(settings.rest_base.rstrip("/"))
        self.session: aiohttp.ClientSession | None = None
        # Venue-wide 429 throttle: a single 429 backs off ALL callers of this
        # client (poll loops included), not just the call that hit it. The
        # /markets listing endpoint throttles around ~1 req/s (observed), far
        # below the documented account-level ceiling.
        self._throttle_until: float = 0.0
        # Venue 429 tripwire: once one request gives up after max_429_wait_s
        # of 429 backoff, the venue is hard-throttled — every subsequent
        # request fails fast instead of each burning another 30 minutes.
        self.throttle_tripped: bool = False

    async def __aenter__(self):
        connector = NoDelayConnector(limit=20)
        timeout = aiohttp.ClientTimeout(total=self.s.request_timeout_s)
        # trust_env=True lets aiohttp honor HTTPS_PROXY/NO_PROXY. On this Linux
        # build box all egress goes through a MITM proxy; on the Mac mini
        # target there is no proxy configured and this is a harmless no-op.
        self.session = aiohttp.ClientSession(
            connector=connector, timeout=timeout, trust_env=True,
            headers={"User-Agent": "kalshi-recorder/1.0"})
        return self

    async def __aexit__(self, *exc):
        if self.session:
            await self.session.close()

    async def _get(self, path: str, params: dict | list | None = None,
               attempt: int = 0, waited_429_s: float = 0.0) -> dict:
        if self.throttle_tripped:
            # Another request already proved the venue is hard-throttled.
            # Fail fast: don't burn another max_429_wait_s per request.
            raise KalshiThrottleTimeout(
                f"GET {path}: venue 429 tripwire already tripped; failing fast")
        now = time.monotonic()
        if now < self._throttle_until:
            await asyncio.sleep(self._throttle_until - now)
        await self.bucket.take()
        assert self.session is not None
        url = str(self._base / path.lstrip("/"))
        try:
            async with self.session.get(url, params=params) as r:
                if r.status == 429:
                    # Honor Retry-After header if present. NOTE: Kalshi docs
                    # explicitly state 429s do NOT include Retry-After or
                    # X-RateLimit-* headers ("no penalty or cooldown").
                    # This is defensive only; the exponential backoff below
                    # is what actually handles throttling.
                    headers = getattr(r, "headers", None)
                    retry_after = headers.get("Retry-After") if headers else None
                    if retry_after:
                        try:
                            delay = float(retry_after)
                        except (TypeError, ValueError):
                            delay = backoff_delay(attempt, 5.0, 300.0)
                    else:
                        delay = backoff_delay(attempt, 5.0, 300.0)
                    self._throttle_until = time.monotonic() + delay
                    if waited_429_s + delay > self.s.max_429_wait_s:
                        self.throttle_tripped = True
                        LOG.error(
                            "429 on %s persisted %.0fs over %d attempts; "
                            "aborting request (REC_MAX_429_WAIT_S=%.0f)",
                            path, waited_429_s, attempt + 1,
                            self.s.max_429_wait_s)
                        raise KalshiThrottleTimeout(
                            f"GET {path}: 429-throttled for {waited_429_s:.0f}s "
                            f"over {attempt + 1} attempts; venue hard-throttled, "
                            f"aborting for supervisor restart")
                    LOG.warning("429 on %s; throttling REST %.1fs (attempt %d)",
                                path, delay, attempt + 1)
                    if self.stop is not None:
                        if await sleep_or_stop(self.stop, delay):
                            raise KalshiThrottleTimeout(
                                f"GET {path}: stop requested during 429 backoff")
                    else:
                        await asyncio.sleep(delay)
                    return await self._get(path, params, attempt + 1,
                                           waited_429_s + delay)
                if r.status >= 500 and attempt < 5:
                    delay = backoff_delay(attempt, 1.0, 15.0)
                    LOG.warning("%s on %s; retrying in %.1fs", r.status, path, delay)
                    await asyncio.sleep(delay)
                    return await self._get(path, params, attempt + 1)
                r.raise_for_status()
                return orjson.loads(await r.read())
        except aiohttp.ClientResponseError:
            raise  # 4xx (non-429): retrying won't help
        except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
            if attempt >= 8:
                raise
            delay = backoff_delay(attempt, 1.0, 60.0)
            LOG.warning("network error on %s (%r); retrying in %.1fs", path, exc, delay)
            await asyncio.sleep(delay)
            return await self._get(path, params, attempt + 1)

    async def list_markets(self, cursor: str | None = None,
                       series_ticker: str | None = None,
                       status: str | None = "active") -> tuple[list[dict], str | None]:
        # Default status="active" (the API's real status for tradable markets).
        # Pass status=None for no filter, or "open"/"closed"/"settled" as needed.
        # (2026-10-17: was hardcoded "open" but API returns "active"; 2026-10-19:
        #  made it a parameter.)
        params = {"limit": 200}
        if status:
            params["status"] = status
        if cursor:
            params["cursor"] = cursor
        if series_ticker:
            params["series_ticker"] = series_ticker
        d = await self._get("/markets", params)
        # Pace listing calls: the /markets endpoint throttles around ~1 req/s.
        # Sleeping here (not in the caller) ensures ALL listing callers
        # (recorder enumeration, census, catalog) stay under the limit.
        await asyncio.sleep(self.s.enumerate_pace_s)
        return d.get("markets", []), d.get("cursor")

    async def list_series(self, cursor: str | None = None) -> tuple[list[dict], str | None]:
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        d = await self._get("/series", params)
        # Same pacing as list_markets: /series also throttles on rapid paging.
        await asyncio.sleep(self.s.enumerate_pace_s)
        return d.get("series", []), d.get("cursor")

    async def batch_orderbooks(self, tickers: list[str], depth: int) -> dict:
        """One call for many books. Returns {ticker: orderbook_fp}.

        NOTE: /markets/orderbooks takes tickers as REPEATED query params
        (?tickers=A&tickers=B). A comma-joined value is treated as ONE ticker
        and fails server-side length validation (400). The response is a list
        of {"ticker": ..., "orderbook_fp": {...}} objects.
        """
        if not tickers:
            return {}
        # NOTE: `depth` is not documented for the batch endpoint (only for
        # single-market /markets/{ticker}/orderbook). It may be silently
        # ignored; we pass it anyway in case the API honors it.
        params = [("tickers", t) for t in tickers] + [("depth", depth)]
        d = await self._get("/markets/orderbooks", params)
        out: dict = {}
        for item in d.get("orderbooks") or []:
            t = item.get("ticker")
            if t:
                out[t] = item.get("orderbook_fp", item)
        return out

    async def get_trades(self, ticker: str, cursor: str | None = None,
                         limit: int = 200) -> tuple[list[dict], str | None]:
        params = {"ticker": ticker, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        d = await self._get("/markets/trades", params)
        return d.get("trades", []), d.get("cursor")
