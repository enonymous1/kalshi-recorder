#!/usr/bin/env python3
"""Universe selection: series filtering, market ranking, and cache management."""

from __future__ import annotations

import logging
import os
import time

import orjson

LOG = logging.getLogger("recorder")


def _blob(m: dict) -> str:
    return f"{m.get('ticker','')} {m.get('event_ticker','')} {m.get('title','')}".lower()


_SPORTS_PATTERNS = ("nfl", "nba", "mlb", "nhl", "kxnfl", "kxnba", "kxmlb", "kxnhl",
                    "super bowl", "world series", "stanley cup", "nba finals")


def select_universe(markets: list[dict], settings) -> list[dict]:
    """Apply include/exclude substring rules + volume floor + cap.

    Note: enumeration is series-first (see Recorder.enumerate), so the include
    rules have usually already been applied at the series level; this remains
    as a second gate at the market level.

    Sports get reserved slots (REC_SPORTS_RESERVE): pure volume ranking lets
    hot categories crowd sports out entirely, which defeats live-game testing.
    """
    picked: list[dict] = []
    for m in markets:
        # The API returns 'active' for live markets (docs say 'open', but
        # real responses use 'active'). Accept both.
        if m.get("status") not in ("active", "open"):
            continue
        blob = _blob(m)
        # empty include list = no category filter (take all by volume)
        if settings.universe_include and not any(s.lower() in blob for s in settings.universe_include):
            continue
        if any(s.lower() in blob for s in settings.universe_exclude):
            continue
        try:
            vol = float(m.get("volume_24h_fp") or 0)
        except (TypeError, ValueError):
            vol = 0.0
        if vol < settings.min_volume_24h:
            continue
        picked.append(m)

    def vol_key(m: dict) -> float:
        try:
            return float(m.get("volume_24h_fp") or 0)
        except (TypeError, ValueError):
            return 0.0

    sports = [m for m in picked
              if any(p in _blob(m) for p in _SPORTS_PATTERNS)]
    sports.sort(key=vol_key, reverse=True)
    rest = [m for m in picked if m not in sports]
    rest.sort(key=vol_key, reverse=True)

    out = sports[: settings.sports_reserve]
    out.extend(rest[: max(0, settings.max_markets - len(out))])
    return out


def select_series(all_series: list[dict], settings) -> list[dict]:
    """First-stage filter: pick series whose ticker/title/category/tags match."""
    picked: list[dict] = []
    for s in all_series:
        tags = s.get("tags")
        tags = " ".join(tags) if isinstance(tags, list) else (tags or "")
        cats = s.get("categories")
        cats = " ".join(cats) if isinstance(cats, list) else (cats or "")
        blob = f"{s.get('ticker','')} {s.get('title','')} {s.get('category','')} {cats} {tags}".lower()
        # empty include list = no category filter (take all by volume)
        if settings.universe_include and not any(p.lower() in blob for p in settings.universe_include):
            continue
        if any(p.lower() in blob for p in settings.universe_exclude):
            continue
        picked.append(s)
    return picked


class UniverseCache:
    """Manages the universe_cache.json and series_cache.json files."""

    def __init__(self, recordings_dir: str, settings):
        self.recordings_dir = recordings_dir
        self.s = settings

    def _universe_cache_path(self) -> str:
        return os.path.join(self.recordings_dir, "universe_cache.json")

    def _series_cache_path(self) -> str:
        return os.path.join(self.recordings_dir, "series_cache.json")

    def _series_list_cache_path(self) -> str:
        return os.path.join(self.recordings_dir, "series_list_cache.json")

    def _fingerprint(self) -> str:
        """Settings that shape the universe.

        A cache hit is only valid if these match what produced it: a smoke
        test run with REC_UNIVERSE_INCLUDE / REC_MAX_SERIES overrides must
        not poison the following full run (it would silently record the
        capped test universe instead of re-enumerating).
        """
        import hashlib

        blob = orjson.dumps({
            "include": sorted(s.lower() for s in self.s.universe_include),
            "exclude": sorted(s.lower() for s in self.s.universe_exclude),
            "min_volume_24h": self.s.min_volume_24h,
            "max_markets": self.s.max_markets,
            "sports_reserve": self.s.sports_reserve,
            "max_series": self.s.max_series,
        })
        return hashlib.sha256(blob).hexdigest()[:16]

    def load_universe(self) -> list[dict] | None:
        """Return cached universe markets if fresh and settings-matching."""
        path = self._universe_cache_path()
        try:
            with open(path, "rb") as f:
                payload = orjson.loads(f.read())
        except (OSError, orjson.JSONDecodeError):
            return None
        at = float(payload.get("at", 0))
        markets = payload.get("markets") or []
        if not markets or (time.time() - at) > self.s.universe_cache_ttl_s:
            return None
        if payload.get("fingerprint") != self._fingerprint():
            return None
        return markets

    def save_universe(self, markets: list[dict]) -> None:
        if not markets:
            return
        path = self._universe_cache_path()
        tmp = path + ".tmp"
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(tmp, "wb") as f:
                f.write(orjson.dumps({"at": time.time(),
                                      "fingerprint": self._fingerprint(),
                                      "markets": markets}))
            os.replace(tmp, path)
        except OSError as exc:
            LOG.warning("universe cache save failed: %s", exc)

    def load_series(self) -> dict:
        """Return {series_ticker: {at, markets}} from the previous enumeration."""
        try:
            with open(self._series_cache_path(), "rb") as f:
                return orjson.loads(f.read())
        except (OSError, orjson.JSONDecodeError):
            return {}

    def save_series(self, cache: dict) -> None:
        path = self._series_cache_path()
        tmp = path + ".tmp"
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(tmp, "wb") as f:
                f.write(orjson.dumps(cache))
            os.replace(tmp, path)
        except OSError as exc:
            LOG.warning("series cache save failed: %s", exc)

    def load_series_list(self, max_age_s: float = 86400) -> list[dict] | None:
        """Return cached /series list if fresh (default 24h TTL).

        The series catalog changes slowly — new series appear, but the list
        itself is stable. Caching avoids ~80s of paged /series fetches on
        every enumeration.
        """
        try:
            with open(self._series_list_cache_path(), "rb") as f:
                payload = orjson.loads(f.read())
        except (OSError, orjson.JSONDecodeError):
            return None
        at = float(payload.get("at", 0))
        series = payload.get("series") or []
        if not series or (time.time() - at) > max_age_s:
            return None
        age_h = (time.time() - at) / 3600
        LOG.info("series list cache hit: %d series (%.1fh old)", len(series), age_h)
        return series

    def save_series_list(self, series: list[dict]) -> None:
        path = self._series_list_cache_path()
        tmp = path + ".tmp"
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(tmp, "wb") as f:
                f.write(orjson.dumps({"at": time.time(), "series": series}))
            os.replace(tmp, path)
        except OSError as exc:
            LOG.warning("series list cache save failed: %s", exc)
