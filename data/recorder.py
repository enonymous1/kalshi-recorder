#!/usr/bin/env python3
"""Phase 0 recorder: Kalshi public market data -> partitioned parquet.

Records order-book snapshots (REST batch endpoint) and public trade prints for a
configurable contract universe. See data/util.py, data/kalshi.py,
data/universe.py, data/watermark.py for the extracted modules.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import time

import orjson
import polars as pl

# Allow `python data/recorder.py` as well as `python -m data.recorder`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import SETTINGS, Settings
from core.loop import install_loop
from data.auth import sign_request, ws_headers_kwarg
from data.kalshi import KalshiREST, KalshiThrottleTimeout
from data.networking import TokenBucket, backoff_delay
from data.schemas import (
    BOOK_SCHEMA,
    TRADE_SCHEMA,
    derive_book_row,
    trade_row,
)
from data.universe import UniverseCache, select_series, select_universe
from data.watermark import Watermark

LOG = logging.getLogger("recorder")


class Recorder:
    def __init__(self, settings: Settings) -> None:
        self.s = settings
        self.bucket = TokenBucket(settings.max_read_rps)
        self.wm = Watermark.load(settings.watermark_path)
        self.ucache = UniverseCache(settings.recordings_dir, settings)
        self.book_rows: dict[str, list[dict]] = {}
        self.trade_rows: dict[str, list[dict]] = {}
        self.universe: list[dict] = []
        self.stop = asyncio.Event()
        self._lock = asyncio.Lock()
        self.dry_run = False

    # -- universe (delegates to UniverseCache) ----------------------------------
    def _load_universe_cache(self) -> list[dict] | None:
        return self.ucache.load_universe()

    def _save_universe_cache(self, markets: list[dict]) -> None:
        self.ucache.save_universe(markets)

    def _load_series_cache(self) -> dict:
        return self.ucache.load_series()

    def _save_series_cache(self, cache: dict) -> None:
        self.ucache.save_series(cache)

    async def _pull_series_markets(self, rest: KalshiREST, series_ticker: str) -> list[dict]:
        """Pull all markets for one series, paginated. Separated so the
        caller can wrap it in a timeout."""
        out: list[dict] = []
        cursor = None
        while True:
            if self.stop.is_set():
                return out
            batch, cursor = await rest.list_markets(cursor, series_ticker=series_ticker)
            out.extend(batch)
            if not cursor:
                break
            # Note: pacing is handled inside list_markets() itself,
            # so all listing callers stay under the rate limit.
        return out

    async def enumerate(self, rest: KalshiREST) -> list[dict]:
        """Two-stage enumeration: series first, then markets per series.

        The open-market catalog is 500k+ (mostly dead combos); paging it all on
        every start is wasteful. /series is ~15k entries, so we filter series
        by the include/exclude rules first and only pull markets for matches.

        The /markets listing endpoint throttles around ~1 req/s (observed
        2026-10-12), so per-series pulls are paced and a Ctrl-C aborts promptly
        instead of grinding through the backlog.
        """
        # Try the series-list cache first (24h TTL) — avoids ~80s of /series paging
        series = self.ucache.load_series_list()
        if series is None:
            series = []
            cursor = None
            while True:
                if self.stop.is_set():
                    LOG.info("enumeration aborted by stop signal")
                    return []
                batch, cursor = await rest.list_series(cursor)
                series.extend(batch)
                if not cursor:
                    break
            # Cache the fresh series list for next time
            if not self.dry_run and series:
                self.ucache.save_series_list(series)
        sel_series = select_series(series, self.s)
        if self.s.max_series > 0 and len(sel_series) > self.s.max_series:
            LOG.info("series capped: %d -> %d for this run (REC_MAX_SERIES)",
                     len(sel_series), self.s.max_series)
            sel_series = sel_series[:self.s.max_series]
        LOG.info("series: %d selected (from %d)", len(sel_series), len(series))

        # Series-level cache: skip re-pulling markets for series that were
        # checked recently and had no high-volume markets. A series with
        # max_volume < $500 is unlikely to suddenly spike, so we use a 24h
        # TTL for the cache. This cuts a 15k-series re-enumeration to ~2-3k
        # API calls on incremental refreshes.
        series_cache = self._load_series_cache()
        now = time.time()
        cache_ttl_s = 24 * 3600  # 24h for low-volume series
        universe_tickers = {m.get("ticker") for m in self.universe} if self.universe else set()

        # Concurrent per-series market pulls. The old serial 1.1s pacing took
        # 4.6h for 15k series; with a semaphore-limited worker pool we stay
        # well under the 20 reads/sec basic-tier budget while cutting the
        # time to ~30-50 min. 429s are handled by the shared throttle in _get.
        sem = asyncio.Semaphore(self.s.enumerate_concurrency)
        markets: list[dict] = []
        completed = 0
        cache_hits = 0
        new_cache: dict = {}
        skipped_series: list[dict] = []  # track timeouts/failures for later analysis

        async def pull_one(s: dict) -> list[dict]:
            nonlocal completed, cache_hits
            st = s.get("ticker", "")
            # Skip blacklisted series (manual override for known-bad)
            if st in self.s.enumerate_skip_series:
                LOG.debug("skipping blacklisted series %s", st)
                completed += 1
                return []
            # Check cache: skip if fresh and low-volume (and not in universe)
            cached = series_cache.get(st)
            if cached:
                at = cached.get("at", 0)
                cmarkets = cached.get("markets", [])
                max_vol = max((m.get("volume_24h", 0) or 0) for m in cmarkets) if cmarkets else 0
                # Use cache if: fresh (<24h), low volume (<$500), not in current universe
                if (now - at) < cache_ttl_s and max_vol < 500 and st not in universe_tickers:
                    cache_hits += 1
                    new_cache[st] = cached  # carry forward
                    completed += 1
                    if completed % 500 == 0:
                        LOG.info("enumeration progress: %d/%d series (%d cache hits)",
                                 completed, len(sel_series), cache_hits)
                    return cmarkets
            # Cache miss or stale: pull from API with per-series timeout
            out: list[dict] = []
            try:
                async with sem:
                    if self.stop.is_set():
                        return []
                    # Wrap the whole per-series pull in a timeout so one
                    # pathological series can't hang the entire enumeration.
                    # The timeout is dynamic, not a hardcoded blacklist.
                    out = await asyncio.wait_for(
                        self._pull_series_markets(rest, st),
                        timeout=self.s.enumerate_series_timeout_s,
                    )
            except asyncio.TimeoutError:
                LOG.warning("series %s timed out after %.0fs; skipping",
                            st, self.s.enumerate_series_timeout_s)
                skipped_series.append({
                    "ticker": st,
                    "reason": "timeout",
                    "timeout_s": self.s.enumerate_series_timeout_s,
                    "at": time.time(),
                    "title": s.get("title", ""),
                })
                completed += 1
                return []
            except KalshiThrottleTimeout:
                # Venue hard-throttled, not a bad series: let it abort the
                # whole enumeration. The recorder exits and the LaunchAgent
                # restarts it; the incremental series cache preserves the
                # progress made so far.
                raise
            except Exception as exc:
                # One bad series must not kill the whole enumeration.
                LOG.warning("series %s enumeration failed (%r); continuing", st, exc)
                skipped_series.append({
                    "ticker": st,
                    "reason": f"error: {type(exc).__name__}: {exc}",
                    "at": time.time(),
                    "title": s.get("title", ""),
                })
                return []
            # Update cache
            # Update cache with fresh timestamp (not the stale enumeration-start time)
            new_cache[st] = {"at": time.time(), "markets": out}
            completed += 1
            if completed % 500 == 0:
                LOG.info("enumeration progress: %d/%d series (%d cache hits)",
                         completed, len(sel_series), cache_hits)
                # Incremental save: if we're interrupted, we don't lose the
                # progress so far. The next run will pick up from here.
                if not self.dry_run:
                    self._save_series_cache(new_cache)
            return out

        tasks = [asyncio.create_task(pull_one(s), name=f"enum-{s.get('ticker', '')}")
                 for s in sel_series]
        try:
            results = await asyncio.gather(*tasks)
        except KalshiThrottleTimeout as exc:
            # Venue hard-throttled mid-enumeration (the tripwire in _get
            # already stops the other workers burning more backoff): cancel
            # the rest, keep the incremental series cache so the next run
            # resumes with cache hits, and exit so the LaunchAgent restarts.
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if not self.dry_run:
                self._save_series_cache(new_cache)
            LOG.critical("enumeration aborted: %s", exc)
            return []
        if self.stop.is_set():
            LOG.info("enumeration aborted by stop signal")
            return []
        for r in results:
            markets.extend(r)
        LOG.info("markets: %d across %d series (%d cache hits)",
                 len(markets), len(sel_series), cache_hits)
        # Save skipped series for later analysis
        if skipped_series and not self.dry_run:
            import json
            skip_path = os.path.join(self.s.recordings_dir, "skipped_series.json")
            # Load existing, append new, dedupe by ticker (keep latest)
            existing = {}
            if os.path.exists(skip_path):
                try:
                    with open(skip_path) as f:
                        for entry in json.load(f):
                            existing[entry["ticker"]] = entry
                except Exception:
                    pass
            for entry in skipped_series:
                existing[entry["ticker"]] = entry
            with open(skip_path, "w") as f:
                json.dump(sorted(existing.values(), key=lambda e: e["ticker"]), f, indent=2)
            LOG.info("skipped %d series (see %s)", len(skipped_series), skip_path)
        # Save the series cache for the next incremental refresh
        if not self.dry_run:
            self._save_series_cache(new_cache)

        self.universe = select_universe(markets, self.s)
        now = time.time()
        for m in self.universe:
            self.wm.touch_market(m["ticker"], now)
        if not self.dry_run:
            self.wm.enumerated_at = now
            self.wm.save()
            self._save_universe_cache(self.universe)
        LOG.info("universe: %d markets", len(self.universe))
        return self.universe

    # -- polling loops ----------------------------------------------------
    async def poll_books(self, rest: KalshiREST) -> None:
        last_tickers: list[str] | None = None
        chunks: list[list[str]] = []
        while not self.stop.is_set():
            tickers = [m["ticker"] for m in self.universe]
            if tickers != last_tickers:
                # universe refreshed (e.g. periodic re-enumeration): rebuild
                chunks = [tickers[i:i + 25] for i in range(0, len(tickers), 25)]
                last_tickers = tickers
                LOG.info("book poller: tracking %d markets in %d chunks",
                         len(tickers), len(chunks))
            ts = time.time_ns()
            for chunk in chunks:
                try:
                    books = await rest.batch_orderbooks(chunk, self.s.orderbook_depth)
                except KalshiThrottleTimeout as exc:
                    # Venue hard-throttled for the full 429 budget: grinding
                    # on achieves nothing. Exit cleanly (final flush runs in
                    # run()) and let the LaunchAgent restart us.
                    LOG.critical("book poller: %s — exiting for restart", exc)
                    self.stop.set()
                    return
                except Exception as exc:
                    LOG.warning("book batch failed: %s", exc)
                    continue
                async with self._lock:
                    for t, ob in books.items():
                        self.book_rows.setdefault(t, []).append(
                            derive_book_row(t, ob, ts, "rest_poll"))
                        self.wm.touch_market(t)["last_book_ts_ns"] = ts
                if self.stop.is_set():
                    break
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=self.s.book_poll_interval_s)
            except asyncio.TimeoutError:
                pass

    async def poll_trades(self, rest: KalshiREST) -> None:
        while not self.stop.is_set():
            for m in self.universe:
                t = m["ticker"]
                entry = self.wm.touch_market(t)
                try:
                    trades, cursor = await rest.get_trades(t, entry.get("trade_cursor"))
                except KalshiThrottleTimeout as exc:
                    LOG.critical("trade poller: %s — exiting for restart", exc)
                    self.stop.set()
                    return
                except Exception as exc:
                    LOG.warning("trades poll failed for %s: %s", t, exc)
                    continue
                if trades:
                    async with self._lock:
                        buf = self.trade_rows.setdefault(t, [])
                        for tr in trades:
                            buf.append(trade_row(tr, "rest_poll"))
                    entry["trade_cursor"] = cursor or entry.get("trade_cursor")
                    entry["last_trade_id"] = trades[-1].get("trade_id")
                if self.stop.is_set():
                    break
                await asyncio.sleep(0.2)  # be gentle across tickers
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=self.s.trades_poll_interval_s)
            except asyncio.TimeoutError:
                pass

    # -- websocket (optional; needs API keys per current Kalshi docs) ------
    async def ws_feed(self) -> None:
        import websockets  # local import: only needed when enabled

        key_id = os.environ.get("KALSHI_API_KEY_ID", "")
        key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")
        if not (key_id and key_path and os.path.exists(key_path)):
            LOG.info("WS disabled: KALSHI_API_KEY_ID / KALSHI_PRIVATE_KEY_PATH not set")
            return
        pem = open(key_path, "rb").read()
        attempt = 0
        while not self.stop.is_set():
            try:
                kid, ts, sig = sign_request(key_id, pem, "GET", "/trade-api/ws/v2")
                headers = {"KALSHI-ACCESS-KEY": kid,
                           "KALSHI-ACCESS-TIMESTAMP": ts,
                           "KALSHI-ACCESS-SIGNATURE": sig}
                async with websockets.connect(self.s.ws_url,
                                               **{ws_headers_kwarg(): headers},
                                               ping_interval=20) as ws:
                    LOG.info("WS connected")
                    attempt = 0
                    tickers = [m["ticker"] for m in self.universe]
                    await ws.send(orjson.dumps({
                        "id": 1, "cmd": "subscribe",
                        "params": {"channels": ["orderbook_delta", "trade"],
                                   "market_tickers": tickers}}).decode())
                    async for raw in ws:
                        if self.stop.is_set():
                            break
                        self._handle_ws_msg(orjson.loads(raw))
            except Exception as exc:
                delay = backoff_delay(attempt, self.s.reconnect_initial_s, self.s.reconnect_max_s)
                LOG.warning("WS dropped (%s); reconnecting in %.1fs", exc, delay)
                attempt += 1
                try:
                    await asyncio.wait_for(self.stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass

    def _handle_ws_msg(self, msg: dict) -> None:
        mtype = msg.get("type", "")
        ts = time.time_ns()
        if mtype == "error":
            # Log WS error messages per docs (codes 1-25)
            err = msg.get("msg", msg)
            code = err.get("code", "?") if isinstance(err, dict) else "?"
            text = err.get("message", str(err)) if isinstance(err, dict) else str(err)
            LOG.warning("WS error %s: %s", code, text)
        elif mtype == "orderbook_snapshot":
            t = msg.get("market_ticker", "")
            ob = msg.get("msg", {})
            if isinstance(ob, dict) and ("yes_dollars" in ob or "no_dollars" in ob):
                self.book_rows.setdefault(t, []).append(derive_book_row(t, ob, ts, "ws"))
                self.wm.touch_market(t)["last_book_ts_ns"] = ts
        elif mtype == "orderbook_delta":
            # Deltas are incremental (price_dollars, delta_fp, side) per docs,
            # not full snapshots. We don't maintain local book state, so we
            # skip deltas rather than misrecording them.
            # TODO: maintain local book from snapshot + deltas for full WS coverage
            pass
        elif mtype == "trade":
            tr = msg.get("msg", msg)
            t = tr.get("market_ticker") or tr.get("ticker", "")
            if t:
                row = trade_row({"ticker": t,
                                 "trade_id": tr.get("trade_id", f"ws-{ts}"),
                                 "yes_price_dollars": tr.get("yes_price_dollars"),
                                 "count_fp": tr.get("count_fp"),
                                 "taker_outcome_side": tr.get("taker_side"),
                                 "created_time": tr.get("created_time", "")}, "ws")
                self.trade_rows.setdefault(t, []).append(row)

    # -- flush ------------------------------------------------------------
    def flush(self) -> int:
        """Merge buffers into day-partitioned parquet. Returns rows written."""
        written = 0
        day = time.strftime("%Y-%m-%d", time.gmtime())
        for kind, schema, key in (("books", BOOK_SCHEMA, ["ts_ns", "ticker"]),
                                  ("trades", TRADE_SCHEMA, ["trade_id"])):
            buffers = self.book_rows if kind == "books" else self.trade_rows
            for ticker, rows in list(buffers.items()):
                if not rows:
                    continue
                path = os.path.join(self.s.recordings_dir, kind, day, f"{ticker}.parquet")
                os.makedirs(os.path.dirname(path), exist_ok=True)
                new_df = pl.DataFrame(rows, schema=schema, orient="row")
                if os.path.exists(path):
                    old = pl.read_parquet(path)
                    df = pl.concat([old, new_df]).unique(subset=key, keep="last")
                else:
                    df = new_df.unique(subset=key, keep="last")
                df.write_parquet(path)
                written += len(new_df)
                buffers[ticker] = []
        if written:
            self.wm.save()
        return written

    async def flush_loop(self) -> None:
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=self.s.flush_interval_s)
            except asyncio.TimeoutError:
                pass
            n = self.flush()
            if n:
                LOG.info("flushed %d rows", n)

    # -- run --------------------------------------------------------------
    async def reenumerate_loop(self, rest: KalshiREST) -> None:
        """Periodically refresh the universe while running.

        Without this, a long-lived run never sees newly listed markets
        (e.g. today's games). Replaces self.universe in place; poll_books
        and poll_trades pick up the new ticker list on their next iteration.
        """
        interval_s = self.s.reenumerate_hours * 3600.0
        if interval_s <= 0:
            LOG.info("periodic re-enumeration disabled")
            return
        LOG.info("periodic re-enumeration every %.1fh", self.s.reenumerate_hours)
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=interval_s)
            except asyncio.TimeoutError:
                pass
            if self.stop.is_set():
                break
            LOG.info("periodic re-enumeration starting")
            try:
                new_universe = await self.enumerate(rest)
            except Exception as exc:
                LOG.warning("periodic re-enumeration failed: %s", exc)
                continue
            if not new_universe:
                LOG.warning("periodic re-enumeration empty; keeping old universe")
                continue
            old = {m["ticker"] for m in self.universe}
            new = {m["ticker"] for m in new_universe}
            self.universe = new_universe
            now = time.time()
            for m in new_universe:
                self.wm.touch_market(m["ticker"], now)
            LOG.info("universe refreshed: %d markets (+%d/-%d)",
                     len(new_universe), len(new - old), len(old - new))

    async def run(self, dry_run: bool = False, once: bool = False) -> None:
        self.dry_run = dry_run
        # Pass stop so 429-backoff sleeps abort promptly on Ctrl-C instead of
        # sleeping out the full delay.
        async with KalshiREST(self.s, self.bucket, stop=self.stop) as rest:
            cached = None if dry_run else self._load_universe_cache()
            if cached is not None:
                self.universe = cached
                now = time.time()
                for m in self.universe:
                    self.wm.touch_market(m["ticker"], now)
                universe = self.universe
            else:
                universe = await self.enumerate(rest)
            if dry_run:
                self.report_dry_run(universe)
                return
            if not universe:
                LOG.warning("universe empty; nothing to record")
                return
            tasks = [asyncio.create_task(self.poll_books(rest), name="books"),
                     asyncio.create_task(self.poll_trades(rest), name="trades"),
                     asyncio.create_task(self.flush_loop(), name="flush"),
                     asyncio.create_task(self.reenumerate_loop(rest),
                                         name="reenumerate")]
            if self.s.ws_enabled:
                tasks.append(asyncio.create_task(self.ws_feed(), name="ws"))
            if once:
                await asyncio.sleep(max(self.s.book_poll_interval_s,
                                        self.s.trades_poll_interval_s) + 5)
                self.stop.set()
            await self.stop.wait()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            n = self.flush()
            LOG.info("shutdown flush: %d rows", n)

    def report_dry_run(self, universe: list[dict]) -> None:
        LOG.info("Would record %d markets (series-first enumeration; no writes in dry-run):",
                 len(universe))
        LOG.info("%-44s%12s  TITLE", "TICKER", "24H VOL $")
        total_vol = 0.0
        for m in universe:
            vol = float(m.get("volume_24h_fp") or 0)
            total_vol += vol
            LOG.info("%-44s%12,.0f  %s", m['ticker'], vol, m.get('title', '')[:60])
        n = len(universe)
        book_calls_day = n / self.s.book_poll_interval_s * 86400 / 25  # batched 25/chunk
        trade_calls_day = n / self.s.trades_poll_interval_s * 86400
        LOG.info("Estimated REST load: ~%,.0f book-batch + ~%,.0f trade calls/day "
                 "(budget: %,.0f/day)",
                 book_calls_day, trade_calls_day, self.s.max_read_rps * 86400)
        LOG.info("WS feed: %s", 'enabled' if self.s.ws_enabled else 'disabled (no API keys)')
        LOG.info("Recordings dir: %s  (untouched)", self.s.recordings_dir)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def install_signal_handlers(loop: asyncio.AbstractEventLoop, stop: asyncio.Event) -> None:
    def _stop() -> None:
        LOG.info("shutdown signal received")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except (NotImplementedError, RuntimeError):
            # Windows / non-main-thread: fall back to the stdlib handler.
            signal.signal(sig, lambda *_: _stop())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Kalshi Phase 0 market-data recorder")
    parser.add_argument("--dry-run", action="store_true",
                        help="enumerate the universe and report; write nothing")
    parser.add_argument("--once", action="store_true",
                        help="single poll pass then exit (smoke test)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")

    loop_name = install_loop()
    LOG.info("event loop: %s", loop_name)

    recorder = Recorder(SETTINGS)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    install_signal_handlers(loop, recorder.stop)
    try:
        loop.run_until_complete(recorder.run(dry_run=args.dry_run, once=args.once))
    except KeyboardInterrupt:
        LOG.info("keyboard interrupt")
    finally:
        loop.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
