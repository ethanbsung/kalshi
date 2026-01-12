import json

from kalshi_bot.kalshi.ws_client import (
    compute_best_prices,
    parse_orderbook_delta,
    parse_orderbook_snapshot,
    parse_ticker_message,
)


def test_parse_ticker_message():
    message = {
        "type": "ticker",
        "sid": 11,
        "msg": {
            "market_ticker": "FED-23DEC-T3.00",
            "price": 48,
            "yes_bid": 45,
            "yes_ask": 53,
            "price_dollars": "0.480",
            "yes_bid_dollars": "0.345",
            "no_bid_dollars": "0.655",
            "volume": 33896,
            "open_interest": 20422,
            "dollar_volume": 16948,
            "dollar_open_interest": 10211,
            "ts": 1669149841,
        },
    }
    row = parse_ticker_message(message, received_ts=1700000000)
    assert row is not None
    assert row["market_id"] == "FED-23DEC-T3.00"
    assert row["ts"] == 1669149841
    assert row["best_yes_bid"] == 45.0
    assert row["best_yes_ask"] == 53.0
    assert row["best_no_bid"] == 47.0
    assert row["best_no_ask"] == 55.0


def test_parse_orderbook_snapshot_and_delta():
    snapshot = {
        "type": "orderbook_snapshot",
        "sid": 2,
        "seq": 2,
        "msg": {
            "market_ticker": "FED-23DEC-T3.00",
            "yes": [[8, 300], [22, 333]],
            "yes_dollars": [["0.080", 300], ["0.220", 333]],
            "no": [[54, 20], [56, 146]],
            "no_dollars": [["0.540", 20], ["0.560", 146]],
        },
    }
    snapshot_row = parse_orderbook_snapshot(snapshot, received_ts=1700000000)
    assert snapshot_row is not None
    assert snapshot_row["market_id"] == "FED-23DEC-T3.00"
    assert snapshot_row["seq"] == 2
    yes_levels = json.loads(snapshot_row["yes_bids_json"])
    no_levels = json.loads(snapshot_row["no_bids_json"])
    assert yes_levels == [[8, 300], [22, 333]]
    assert no_levels == [[54, 20], [56, 146]]
    best = compute_best_prices(yes_levels, no_levels)
    assert best["best_yes_bid"] == 22.0
    assert best["best_no_bid"] == 56.0
    assert best["best_yes_ask"] == 44.0
    assert best["best_no_ask"] == 78.0

    delta = {
        "type": "orderbook_delta",
        "sid": 2,
        "seq": 3,
        "msg": {
            "market_ticker": "FED-23DEC-T3.00",
            "price": 96,
            "price_dollars": "0.960",
            "delta": -54,
            "side": "yes",
        },
    }
    delta_row = parse_orderbook_delta(delta, received_ts=1700000000)
    assert delta_row is not None
    assert delta_row["market_id"] == "FED-23DEC-T3.00"
    assert delta_row["seq"] == 3
    assert delta_row["side"] == "yes"
    assert delta_row["price"] == 96.0
    assert delta_row["size"] == -54.0


def test_parse_ignores_irrelevant_messages():
    message = {"type": "subscribed", "msg": {"channel": "ticker", "sid": 1}}
    assert parse_ticker_message(message, received_ts=1700000000) is None
    assert parse_orderbook_snapshot(message, received_ts=1700000000) is None
    assert parse_orderbook_delta(message, received_ts=1700000000) is None
