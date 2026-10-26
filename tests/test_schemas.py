"""Tests for data.schemas — book/trade row derivation and validation.

The 2026-10-18 ascending-vs-best-first bug recorded two days of bad book
data. These tests pin the correct behavior: best bid is the LAST level,
and bid > ask fails loud instead of recording garbage.
"""

import pytest

from data.schemas import derive_book_row, trade_row


def test_derive_book_row_takes_best_bid():
    """API returns levels ascending (worst-first); we take the LAST (best)."""
    ob = {
        "yes_dollars": [[0.01, 100], [0.50, 200], [0.74, 300]],
        "no_dollars": [[0.01, 100], [0.25, 200]],
    }
    row = derive_book_row("TEST", ob, 12345, "test")
    # Best YES bid = 0.74 (last element), size 300
    assert row["yes_bid"] == 0.74
    assert row["yes_bid_size"] == 300.0
    # Best NO bid = 0.25 -> YES ask = 1 - 0.25 = 0.75
    assert row["yes_ask"] == 0.75
    assert row["yes_ask_size"] == 200.0


def test_derive_book_row_rejects_bid_gt_ask():
    """Validation: bid > ask raises ValueError (fail loud, not silent)."""
    # YES bid 0.80, NO bid 0.30 -> YES ask 0.70 < bid 0.80
    ob = {
        "yes_dollars": [[0.80, 100]],
        "no_dollars": [[0.30, 100]],
    }
    with pytest.raises(ValueError, match="yes_bid.*>.*yes_ask"):
        derive_book_row("TEST", ob, 12345, "test")


def test_derive_book_row_empty_levels():
    """Empty level lists -> None bids/asks (not crash)."""
    row = derive_book_row("TEST", {"yes_dollars": [], "no_dollars": []}, 12345, "test")
    assert row["yes_bid"] is None
    assert row["yes_ask"] is None


def test_trade_row_extracts_fields():
    trade = {
        "trade_id": "abc123",
        "ticker": "TEST-26",
        "yes_price_dollars": "0.65",
        "count_fp": "10.5",
        "taker_outcome_side": "yes",
        "created_time": "2026-10-19T12:00:00Z",
    }
    row = trade_row(trade, "test")
    assert row["trade_id"] == "abc123"
    assert row["price"] == 0.65
    assert row["size"] == 10.5
    assert row["taker_side"] == "yes"
