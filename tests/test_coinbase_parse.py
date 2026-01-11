from datetime import datetime, timezone

from kalshi_bot.feeds.coinbase_ws import parse_ticker_message


def test_parse_ticker_message_returns_row():
    message = {
        "channel": "ticker",
        "timestamp": "2024-01-01T00:00:00.000000Z",
        "sequence_num": 101,
        "events": [
            {
                "type": "ticker",
                "product_id": "BTC-USD",
                "price": "42000.5",
                "best_bid": "41999.9",
                "best_ask": "42001.1",
                "bid_qty": "0.25",
                "ask_qty": "0.3",
            }
        ],
    }
    received_ts = 1700000000.0
    row = parse_ticker_message(message, received_ts, "BTC-USD")

    assert row is not None
    assert row["product_id"] == "BTC-USD"
    assert row["price"] == 42000.5
    assert row["best_bid"] == 41999.9
    assert row["best_ask"] == 42001.1
    assert row["bid_qty"] == 0.25
    assert row["ask_qty"] == 0.3
    assert row["sequence_num"] == 101
    expected_ts = int(
        datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp()
    )
    assert row["ts"] == expected_ts
    assert isinstance(row["raw_json"], str)


def test_parse_ticker_message_missing_fields():
    message = {
        "channel": "ticker",
        "timestamp": "2024-01-01T00:00:01Z",
        "events": [
            {
                "type": "ticker",
                "product_id": "BTC-USD",
                "price": "42010.0",
            }
        ],
    }
    row = parse_ticker_message(message, 1700000000.0, "BTC-USD")

    assert row is not None
    assert row["price"] == 42010.0
    assert row["best_bid"] is None
    assert row["best_ask"] is None
    assert row["bid_qty"] is None
    assert row["ask_qty"] is None
    assert row["sequence_num"] is None


def test_parse_non_ticker_messages_return_none():
    heartbeat = {"channel": "heartbeats", "events": [{"type": "heartbeat"}]}
    irrelevant = {"channel": "status", "type": "subscriptions"}

    assert parse_ticker_message(heartbeat, 1700000000.0, "BTC-USD") is None
    assert parse_ticker_message(irrelevant, 1700000000.0, "BTC-USD") is None
