import asyncio
import json

from kalshi_bot.app import collector
from kalshi_bot.config import Settings


def test_kalshi_tickers_skip_rest(monkeypatch, tmp_path):
    called = {"rest": False}
    captured = {}

    async def fake_list_markets(self, *args, **kwargs):
        called["rest"] = True
        return []

    class StubWsClient:
        def __init__(self, *args, **kwargs):
            captured["tickers"] = kwargs.get("market_tickers")
            self.connect_attempts = 0
            self.connect_successes = 0
            self.recv_count = 0
            self.parsed_ticker_count = 0
            self.parsed_snapshot_count = 0
            self.parsed_delta_count = 0
            self.error_message_count = 0
            self.close_count = 0

        async def run(self, *args, **kwargs):
            return

    monkeypatch.setattr(collector.KalshiRestClient, "list_markets", fake_list_markets)
    monkeypatch.setattr(collector, "KalshiWsClient", StubWsClient)

    db_path = tmp_path / "test.sqlite"
    log_path = tmp_path / "app.jsonl"
    settings = Settings(
        db_path=db_path,
        log_path=log_path,
        kalshi_market_tickers="TICKER-1,TICKER-2",
        collector_seconds=1,
    )

    asyncio.run(
        collector.run_collector(
            settings,
            coinbase=False,
            kalshi=True,
            seconds=1,
        )
    )

    assert called["rest"] is False
    assert captured["tickers"] == ["TICKER-1", "TICKER-2"]

    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    payloads = [json.loads(line) for line in lines]
    summary = next(
        (p for p in payloads if p.get("msg") == "kalshi_subscribe_tickers_count"),
        None,
    )
    assert summary is not None
    assert summary.get("count") == 2
