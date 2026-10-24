#!/usr/bin/env python3
"""Watermark: resume enumeration + per-market pagination without gaps/replays."""

from __future__ import annotations

import logging
import os
import tempfile
import time
from dataclasses import dataclass, field

import orjson

LOG = logging.getLogger("recorder")


@dataclass
class Watermark:
    path: str
    markets_cursor: str | None = None
    enumerated_at: float = 0.0
    markets: dict = field(default_factory=dict)  # ticker -> {first_seen, last_book_ts_ns, trade_cursor}

    @classmethod
    def load(cls, path: str) -> "Watermark":
        if os.path.exists(path):
            try:
                raw = orjson.loads(open(path, "rb").read())
                return cls(path=path,
                           markets_cursor=raw.get("markets_cursor"),
                           enumerated_at=raw.get("enumerated_at", 0.0),
                           markets=raw.get("markets", {}))
            except Exception as exc:  # corrupt sidecar: start clean, never crash
                LOG.warning("watermark corrupt (%s); starting fresh", exc)
        return cls(path=path)

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        payload = {
            "markets_cursor": self.markets_cursor,
            "enumerated_at": self.enumerated_at,
            "markets": self.markets,
        }
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(self.path) or ".", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(orjson.dumps(payload))
            os.replace(tmp, self.path)  # atomic
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)

    def touch_market(self, ticker: str, now: float | None = None) -> dict:
        entry = self.markets.setdefault(ticker, {"first_seen": now or time.time()})
        return entry
