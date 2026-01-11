import asyncio
import sqlite3

from kalshi_bot.app import collector
from kalshi_bot.config import Settings


async def _message_source():
    yield {
        "channel": "ticker",
        "timestamp": "2024-01-01T00:00:00Z",
        "sequence_num": 7,
        "events": [
            {
                "type": "snapshot",
                "tickers": [
                    {
                        "product_id": "BTC-USD",
                        "price": "41000.0",
                        "best_bid": "40999.5",
                        "best_ask": "41000.5",
                        "best_bid_quantity": "1.2",
                        "best_ask_quantity": "0.8",
                    }
                ],
            }
        ],
    }


def test_collector_coinbase_inserts_row(tmp_path):
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
            kalshi=False,
            seconds=1,
            message_source=_message_source(),
        )
    )

    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute("SELECT COUNT(*) FROM spot_ticks").fetchone()[0]
    finally:
        conn.close()

    assert count >= 1
