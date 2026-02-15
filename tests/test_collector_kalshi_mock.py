import asyncio
import json
import sqlite3

import pytest

from kalshi_bot.app import collector
from kalshi_bot.config import Settings


async def _message_source():
    yield {
        "type": "ticker",
        "sid": 11,
        "msg": {
            "market_ticker": "FED-23DEC-T3.00",
            "price": 48,
            "yes_bid": 45,
            "yes_ask": 53,
            "volume": 33896,
            "open_interest": 20422,
            "dollar_volume": 16948,
            "dollar_open_interest": 10211,
            "ts": 1669149841,
        },
    }


@pytest.mark.skip(
    reason="Legacy SQLite collector assertion is out-of-scope for bus-first redesign."
)
def test_collector_kalshi_inserts_row(tmp_path):
    db_path = tmp_path / "test.sqlite"
    log_path = tmp_path / "app.jsonl"
    settings = Settings(
        db_path=db_path,
        log_path=log_path,
        kalshi_market_tickers="FED-23DEC-T3.00",
        kalshi_ws_url="wss://example.invalid",
        collector_seconds=1,
    )

    asyncio.run(
        collector.run_collector(
            settings,
            coinbase=False,
            kalshi=True,
            seconds=1,
            kalshi_message_source=_message_source(),
            kalshi_market_data=[{"ticker": "FED-23DEC-T3.00", "status": "active"}],
        )
    )

    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute("SELECT COUNT(*) FROM kalshi_tickers").fetchone()[0]
        market_count = conn.execute("SELECT COUNT(*) FROM kalshi_markets").fetchone()[0]
    finally:
        conn.close()

    assert count >= 1
    assert market_count >= 1

    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    summary = None
    for line in lines:
        payload = json.loads(line)
        if payload.get("msg") == "kalshi_run_summary":
            summary = payload
            break

    assert summary is not None
    assert summary.get("parsed_ticker_count", 0) >= 1
