import logging

from kalshi_bot.kalshi.contracts import (
    bounds_from_payload,
    bounds_from_ticker,
    build_contract_row,
)


def test_bounds_from_payload_between():
    market = {"floor_strike": 65000, "cap_strike": 70000, "strike_type": "between"}
    assert bounds_from_payload(market) == (65000.0, 70000.0, "between")


def test_bounds_from_payload_custom_between():
    market = {
        "floor_strike": 91000,
        "cap_strike": 91249,
        "strike_type": "custom",
        "custom_strike": {"note": "range"},
    }
    assert bounds_from_payload(market) == (91000.0, 91249.0, "between")


def test_bounds_from_payload_greater():
    market = {"floor_strike": 65000, "strike_type": "greater"}
    assert bounds_from_payload(market) == (65000.0, None, "greater")


def test_bounds_from_payload_less():
    market = {"cap_strike": 65000, "strike_type": "less"}
    assert bounds_from_payload(market) == (None, 65000.0, "less")


def test_bounds_from_ticker_below():
    assert bounds_from_ticker("KXBTC-24JUN28-B65000") == (None, 65000.0, "less")


def test_bounds_from_ticker_above():
    assert bounds_from_ticker("KXBTC15M-24JUN28-A70000") == (70000.0, None, "greater")


def test_build_contract_row_ambiguous():
    row = build_contract_row(
        "KXBTC-24JUN28-RANGE",
        settlement_ts=1700000000,
        market=None,
        logger=logging.getLogger("test"),
    )
    assert row["lower"] is None
    assert row["upper"] is None
    assert row["strike_type"] is None
    assert row["settlement_ts"] == 1700000000


def test_build_contract_row_close_over_expiration():
    market = {
        "close_time": "2026-01-13T03:00:00Z",
        "expected_expiration_time": "2026-01-13T03:05:00Z",
        "expiration_time": "2026-01-20T03:00:00Z",
        "floor_strike": 91000,
        "cap_strike": 91249,
        "strike_type": "custom",
    }
    row = build_contract_row(
        "KXBTC-26JAN1222-B91125",
        settlement_ts=1768878000,
        market=market,
        logger=logging.getLogger("test"),
    )
    assert row["settlement_ts"] == 1768273500
    assert row["expiration_ts"] == 1768878000
