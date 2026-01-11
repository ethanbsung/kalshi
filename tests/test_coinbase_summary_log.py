import asyncio
import json

from kalshi_bot.app import collector
from kalshi_bot.config import Settings


async def _message_source():
    yield {
        "channel": "ticker",
        "timestamp": "2024-01-01T00:00:00Z",
        "sequence_num": 9,
        "events": [
            {
                "type": "snapshot",
                "tickers": [
                    {
                        "product_id": "BTC-USD",
                        "price": "43000.0",
                        "best_bid": "42999.0",
                        "best_ask": "43001.0",
                        "best_bid_quantity": "0.4",
                        "best_ask_quantity": "0.6",
                    }
                ],
            }
        ],
    }


def test_coinbase_run_summary_logged(tmp_path):
    db_path = tmp_path / "test.sqlite"
    log_path = tmp_path / "app.jsonl"
    settings = Settings(
        db_path=db_path,
        log_path=log_path,
        coinbase_product_id="BTC-USD",
        coinbase_ws_url="wss://example.invalid",
        collector_seconds=1,
    )

    asyncio.run(
        collector.run_collector(
            settings,
            coinbase=True,
            seconds=1,
            message_source=_message_source(),
        )
    )

    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    summary = None
    for line in lines:
        payload = json.loads(line)
        if payload.get("msg") == "coinbase_run_summary":
            summary = payload
            break

    assert summary is not None
    assert summary.get("parsed_row_count", 0) >= 1
