"""Minimal configuration for kalshi-recorder.

No secrets in this file. Private credentials come from environment variables
or a `.env` file in the project root (see `.env.example`); real environment
variables always win over `.env`. Copy to config/settings_local.py if you need
per-machine overrides (that file is git-ignored).
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:  # python-dotenv not installed; env vars still work
    pass


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _env_list(name: str, default: list[str]) -> list[str]:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return [p.strip() for p in raw.split(",") if p.strip()]


# ----------------------------------------------------------------------------
# Kalshi API
# ----------------------------------------------------------------------------
KALSHI_REST_BASE = _env("KALSHI_REST_BASE", "https://api.elections.kalshi.com/trade-api/v2")
KALSHI_WS_URL = _env("KALSHI_WS_URL", "wss://api.elections.kalshi.com/trade-api/ws/v2")
# Demo (paper) venue mirrors prod; keys are NOT interchangeable between them.
KALSHI_DEMO_REST_BASE = "https://demo-api.kalshi.co/trade-api/v2"
KALSHI_DEMO_WS_URL = "wss://demo-api.kalshi.co/trade-api/ws/v2"
USE_DEMO = _env("KALSHI_USE_DEMO", "0") == "1"

# Private-endpoint credentials. NEVER hardcode; set in the environment or a
# secrets manager on the recording host (Mac mini).
KALSHI_API_KEY_ID = _env("KALSHI_API_KEY_ID", "")          # UUID from Kalshi dashboard
KALSHI_PRIVATE_KEY_PATH = _env("KALSHI_PRIVATE_KEY_PATH", "")  # path to Ed25519 (default) or RSA private-key PEM

AUTH_HEADERS_AVAILABLE = bool(KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH)


# ----------------------------------------------------------------------------
# Contract universe (Phase 0 recorder scope)
# ----------------------------------------------------------------------------
@dataclass
class UniverseRule:
    """One inclusion/exclusion rule for market enumeration."""
    # Match against market ticker / event_ticker / title (case-insensitive substring).
    match: str = ""
    # Optional: only include markets whose title also matches this.
    title_match: str = ""
    # Optional: minimum 24h volume in dollars to bother recording.
    min_volume_24h: float = 0.0
    exclude: bool = False


@dataclass
class Settings:
    rest_base: str = KALSHI_REST_BASE if not USE_DEMO else KALSHI_DEMO_REST_BASE
    ws_url: str = KALSHI_WS_URL if not USE_DEMO else KALSHI_DEMO_WS_URL

    # Universe: start small. Seed lists are substrings matched against ticker /
    # event_ticker / title at enumeration time; volume filter keeps the tail out.
    # Empty (default) = no category filter: take the top markets by volume
    # across ALL categories. Set REC_UNIVERSE_INCLUDE="FED,NFL,..." to restrict.
    universe_include: list = field(default_factory=lambda: _env_list("REC_UNIVERSE_INCLUDE", []))
    universe_exclude: list = field(default_factory=lambda: [
        "COMBO",                                  # multivariate combos: skip in Phase 0
    ])
    min_volume_24h: float = _env_float("REC_MIN_VOLUME_24H", 1000.0)
    max_markets: int = _env_int("REC_MAX_MARKETS", 60)
    # Reserved universe slots for sports (by volume within sports). Pure
    # volume ranking lets hot categories (e.g. elections) crowd sports out
    # entirely; this guarantees live games stay in the universe.
    sports_reserve: int = _env_int("REC_SPORTS_RESERVE", 40)
    # Cap series pulled per run (0 = unlimited). Smoke-test knob:
    # REC_MAX_SERIES=5 verifies end-to-end quickly without scanning everything.
    max_series: int = _env_int("REC_MAX_SERIES", 0)

    # Enumeration pacing: the /markets listing endpoint 429s above ~1 req/s
    # (observed 2026-10-12), so pace per-series pulls and let the shared 429
    # throttle in KalshiREST absorb the rest.
    enumerate_pace_s: float = _env_float("REC_ENUMERATE_PACE_S", 1.1)
    # Max concurrent per-series market pulls during enumeration. The /markets
    # listing endpoint throttles around ~1 req/s (observed 2026-10-12), so 2
    # is the safe max — higher values 429-spam and end up slower than serial.
    enumerate_concurrency: int = _env_int("REC_ENUMERATE_CONCURRENCY", 2)
    # Universe cache: skip re-enumeration on restart when the cache is fresh.
    universe_cache_ttl_s: float = _env_float("REC_UNIVERSE_CACHE_TTL_S", 21600.0)
    # Periodic background re-enumeration while running (hours; 0 = disabled).
    # The universe goes stale otherwise — e.g. today's games aren't listed
    # until they're listed, so a run started yesterday never sees them.
    # Defaults to 12h: enumeration itself takes ~2h at current series counts.
    reenumerate_hours: float = _env_float("REC_REENUMERATE_HOURS", 12.0)
    # Series to skip during enumeration (comma-separated). For pathological
    # series that hang the API — e.g. KXPRICEINCREASEUSPSFOREVER hung
    # enumeration 3x at position 10501 on 2026-10-17.
    enumerate_skip_series: list[str] = field(default_factory=lambda: _env_list(
        "REC_ENUMERATE_SKIP_SERIES",
        [],  # dynamic timeout handles hangs; manual blacklist if needed
    ))
    # Per-series timeout during enumeration (seconds). If a single series
    # takes longer than this, skip it and continue. Prevents one pathological
    # series from hanging the entire enumeration (seen 2026-10-17).
    enumerate_series_timeout_s: float = _env_float("REC_ENUMERATE_SERIES_TIMEOUT_S", 120.0)

    # Polling
    book_poll_interval_s: float = _env_float("REC_BOOK_POLL_S", 15.0)
    orderbook_depth: int = _env_int("REC_BOOK_DEPTH", 10)
    trades_poll_interval_s: float = _env_float("REC_TRADES_POLL_S", 30.0)

    # REST rate budget: stay well under Basic tier (20 read/s). Writes are unused
    # by the recorder.
    max_read_rps: float = _env_float("REC_MAX_READ_RPS", 8.0)

    # WS (needs API keys even for public channels on current Kalshi docs)
    ws_enabled: bool = _env("REC_WS_ENABLED", "1") == "1" and AUTH_HEADERS_AVAILABLE

    # Storage
    recordings_dir: str = _env("REC_DIR", "data/recordings")
    watermark_path: str = _env("REC_WATERMARK", "data/recordings/watermark.json")
    flush_interval_s: float = _env_float("REC_FLUSH_S", 60.0)

    # Resilience
    reconnect_initial_s: float = 2.0
    reconnect_max_s: float = 300.0

    request_timeout_s: float = _env_float("REC_TIMEOUT_S", 45.0)
    # 429 budget: a single request that burns more than this (seconds) in
    # 429 backoff raises KalshiThrottleTimeout instead of retrying forever
    # (the 2026-10-16 incident stalled the recorder ~18h in the indefinite
    # retry loop on a hard-throttled /markets listing). The recorder treats
    # it as venue-level: abort and let the supervisor restart with the
    # incremental cache, rather than hang silently.
    max_429_wait_s: float = _env_float("REC_MAX_429_WAIT_S", 1800.0)


SETTINGS = Settings()
