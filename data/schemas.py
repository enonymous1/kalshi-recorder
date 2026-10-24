"""Polars schemas and row derivation for recorded book/trade data.

Book levels from the Kalshi API arrive ASCENDING (worst-first); the best bid
is the LAST element. The derive functions handle this and validate that
bid <= ask (fail loud on parse errors — see 2026-10-18 bug).
"""

from __future__ import annotations

import time

import polars as pl

from core.logging import get_logger

LOG = get_logger(__name__)


BOOK_SCHEMA = {
    "ts_ns": pl.Int64,
    "ticker": pl.String,
    "yes_bid": pl.Float64,
    "yes_ask": pl.Float64,
    "no_bid": pl.Float64,
    "no_ask": pl.Float64,
    "yes_bid_size": pl.Float64,
    "yes_ask_size": pl.Float64,
    "no_bid_size": pl.Float64,
    "no_ask_size": pl.Float64,
    "source": pl.String,
}

TRADE_SCHEMA = {
    "ts_ns": pl.Int64,
    "trade_id": pl.String,
    "ticker": pl.String,
    "price": pl.Float64,        # yes_price_dollars
    "size": pl.Float64,         # count_fp
    "taker_side": pl.String,    # taker_outcome_side: yes | no
    "taker_book_side": pl.String,  # bid | ask
    "source": pl.String,
}


def _f(x) -> float | None:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def derive_book_row(ticker: str, orderbook_fp: dict, ts_ns: int, source: str) -> dict:
    """Build one book row from a Kalshi orderbook_fp payload.

    Kalshi returns yes bids and no bids only; asks are the mirror image
    (yes ask @ X == no bid @ 1-X). Each side is [[price_dollars, count], ...]
    sorted ASCENDING by price (worst-first). Best bid is the LAST element.
    """
    yes_levels = orderbook_fp.get("yes_dollars") or []
    no_levels = orderbook_fp.get("no_dollars") or []

    # API sorts ascending (1c first); best bid is the highest price = last element
    yes_bid, yes_bid_size = (_f(yes_levels[-1][0]), _f(yes_levels[-1][1])) if yes_levels else (None, None)
    no_bid, no_bid_size = (_f(no_levels[-1][0]), _f(no_levels[-1][1])) if no_levels else (None, None)

    yes_ask = (1.0 - no_bid) if no_bid is not None else None
    no_ask = (1.0 - yes_bid) if yes_bid is not None else None

    # Validation: bid must never exceed ask. If this fires, the book
    # parsing is wrong (e.g. wrong sort order assumption). Fail loud
    # instead of silently recording bad data.
    # (Added 2026-10-18 after the ascending-vs-best-first bug
    #  recorded 2 days of bad book data.)
    if yes_bid is not None and yes_ask is not None and yes_bid > yes_ask:
        raise ValueError(
            f"Invalid book for {ticker}: yes_bid ({yes_bid}) > yes_ask ({yes_ask}). "
            f"Book parsing is broken - check level ordering."
        )
    if no_bid is not None and no_ask is not None and no_bid > no_ask:
        raise ValueError(
            f"Invalid book for {ticker}: no_bid ({no_bid}) > no_ask ({no_ask}). "
            f"Book parsing is broken - check level ordering."
        )

    return {
        "ts_ns": ts_ns,
        "ticker": ticker,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "yes_bid_size": yes_bid_size,
        "yes_ask_size": no_bid_size,   # size mirrors the no bid it derives from
        "no_bid_size": no_bid_size,
        "no_ask_size": yes_bid_size,
        "source": source,
    }


def trade_row(trade: dict, source: str) -> dict:
    """Build one trade row from a Kalshi /markets/trades record."""
    import datetime

    created = trade.get("created_time", "")
    try:
        ts_ns = int(datetime.datetime.fromisoformat(created.replace("Z", "+00:00")).timestamp() * 1e9)
    except (ValueError, TypeError):
        ts_ns = time.time_ns()
    return {
        "ts_ns": ts_ns,
        "trade_id": str(trade.get("trade_id", "")),
        "ticker": str(trade.get("ticker", "")),
        "price": _f(trade.get("yes_price_dollars")),
        "size": _f(trade.get("count_fp")),
        "taker_side": str(trade.get("taker_outcome_side") or trade.get("taker_side") or ""),
        "taker_book_side": str(trade.get("taker_book_side") or ""),
        "source": source,
    }
