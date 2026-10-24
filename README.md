# kalshi-recorder

Standalone Kalshi market data recorder. Captures order book snapshots and trades to Parquet files.

Extracted from `ss-predictionMarkets` on 2026-10-19. The simulation (`sim/`) and analysis (`analysis/`) components remain in the parent project.

## Structure

```
config/settings.py    # Configuration via .env (all REC_* knobs)
core/loop.py          # Event loop (uvloop where available)
core/logging.py       # Centralized logging setup
data/recorder.py      # Main orchestration (Recorder class + CLI)
data/kalshi.py        # Kalshi REST + WS clients (rate-limited, retry)
data/universe.py      # Universe selection & caching (175 markets by volume)
data/watermark.py     # Atomic watermark for crash-safe resume
data/schemas.py       # Parquet schemas & row derivation (with bid>ask validation)
data/networking.py    # TCP_NODELAY, TokenBucket, backoff, sleep_or_stop
data/auth.py          # Ed25519/RSA request signing, WS headers
mac/                  # LaunchAgent plist + README for macOS deployment
```

## Quick Start

```bash
# Install (creates `kalshi-record` console script)
pip install -e .

# Configure — copy example and fill in your Kalshi API credentials
# (read-only key; lives only on the recording host, never in chat)
cp .env.example .env
# Edit .env: KALSHI_KEY_ID, KALSHI_PRIVATE_KEY_PATH, REC_* knobs

# Dry run (enumerate universe, no writes)
python -m data.recorder --dry-run

# Run foreground
kalshi-record
```

## Configuration (.env)

| Key | Default | Notes |
|-----|---------|-------|
| `REC_MAX_MARKETS` | 175 | Universe size |
| `REC_MIN_VOLUME_24H` | 500 | Min $24h volume to qualify |
| `REC_UNIVERSE_INCLUDE` | (empty) | Empty = all categories (full universe) |
| `REC_ENUMERATE_HOURS` | 12 | Background re-enumeration interval |
| `REC_BOOK_POLL_S` | 15 | Book poll interval |
| `REC_TRADES_POLL_S` | 30 | Trade poll interval |

## macOS LaunchAgent

See `mac/README.md` for full deployment notes (TCC permissions, external volumes).

Install (edit paths in the plist first — it ships with `/path/to/kalshi-recorder` placeholders):
```bash
cp mac/com.kalshi-recorder.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.kalshi-recorder.plist
```

> Do NOT run this alongside the old `com.ss-predictionmarkets.recorder`
> LaunchAgent — two recorders will contend over the same watermark and
> Parquet files. Unload the old one first.

Useful commands:
```bash
# Stop
launchctl unload ~/Library/LaunchAgents/com.kalshi-recorder.plist
# Tail logs
tail -f ~/Library/Logs/kalshi-recorder.log
```

## Data Layout

```
data/recordings/
├── books/2026-10-19/<TICKER>.parquet   # Book snapshots (15s cadence)
├── trades/2026-10-19/<TICKER>.parquet  # Trades (30s cadence)
├── universe_cache.json                  # 175-market universe (6h TTL)
├── series_cache.json                    # Per-series market lists
└── watermark.json                       # Atomic resume state
```

Book schema: `ts_ns, ticker, yes_bid, yes_ask, no_bid, no_ask, *_size, source`.
`derive_book_row` validates `bid <= ask` and raises on violation (fail loud).

## History

- 2026-10-19: Extracted as standalone from `ss-predictionMarkets`
- 2026-10-18: Fixed critical book-parsing bug (`yes_dollars` ascending, not best-first)
- 2026-10-17: Fixed universe:0 bug (API returns `status='active'`, not `'open'`)
- 2026-10-15: Recorder split from 1,054-line monolith into 5 modules
