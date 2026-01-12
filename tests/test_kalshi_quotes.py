import asyncio
import logging
import sqlite3

import aiosqlite

from kalshi_bot.data import init_db
from kalshi_bot.kalshi.quotes import KalshiQuotePoller, build_quote_row


def test_build_quote_row_mid_fields():
    market = {
        "ticker": "KXBTC-24JUN28-B65000",
        "yes_bid": 40,
        "yes_ask": 60,
        "no_bid": 42,
        "no_ask": 58,
        "volume": 10,
        "volume_24h": 20,
        "open_interest": 5,
    }
    row = build_quote_row(market, ts=1700000000)
    assert row["yes_mid"] == 50.0
    assert row["no_mid"] == 50.0
    assert row["p_mid"] == 0.5


def test_quote_insert_with_mock_fetcher(tmp_path):
    db_path = tmp_path / "quotes.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("KXBTC-24JUN28-B65000", 1700000000, "open"),
            )
            await conn.commit()

            class DummyRestClient:
                async def get_market(self, ticker: str, end_time: float | None):
                    return {
                        "ticker": ticker,
                        "yes_bid": 40,
                        "yes_ask": 60,
                        "no_bid": 42,
                        "no_ask": 58,
                        "volume": 10,
                        "volume_24h": 20,
                        "open_interest": 5,
                    }

            poller = KalshiQuotePoller(
                rest_client=DummyRestClient(),  # type: ignore[arg-type]
                logger=logging.getLogger("test"),
            )
            await poller.poll_once(conn, ["KXBTC-24JUN28-B65000"])

        conn = sqlite3.connect(db_path)
        try:
            count = conn.execute("SELECT COUNT(*) FROM kalshi_quotes").fetchone()[0]
        finally:
            conn.close()

        assert count == 1

    asyncio.run(_run())


def test_poll_once_rejects_invalid_bid_ask(tmp_path):
    db_path = tmp_path / "invalid.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("KXBTC-24JUN28-B65000", 1700000000, "active"),
            )
            await conn.commit()

            class DummyRestClient:
                async def get_market(self, ticker: str, end_time: float | None):
                    return {
                        "ticker": ticker,
                        "yes_bid": 60,
                        "yes_ask": 50,
                        "no_bid": 40,
                        "no_ask": 60,
                    }

            poller = KalshiQuotePoller(
                rest_client=DummyRestClient(),  # type: ignore[arg-type]
                logger=logging.getLogger("test"),
            )
            summary = await poller.poll_once(conn, ["KXBTC-24JUN28-B65000"])
            assert summary["inserted"] == 0
            assert summary["error_counts"]["invalid_payload"] == 1
            assert "KXBTC-24JUN28-B65000" in summary["failed_tickers_sample"]

    asyncio.run(_run())


def test_poll_once_rejects_out_of_bounds_p_mid(tmp_path):
    db_path = tmp_path / "invalid_pmid.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("KXBTC-24JUN28-B65000", 1700000000, "active"),
            )
            await conn.commit()

            class DummyRestClient:
                async def get_market(self, ticker: str, end_time: float | None):
                    return {
                        "ticker": ticker,
                        "yes_bid": 150,
                        "yes_ask": 160,
                        "no_bid": 0,
                        "no_ask": 1,
                    }

            poller = KalshiQuotePoller(
                rest_client=DummyRestClient(),  # type: ignore[arg-type]
                logger=logging.getLogger("test"),
            )
            summary = await poller.poll_once(conn, ["KXBTC-24JUN28-B65000"])
            assert summary["inserted"] == 0
            assert summary["error_counts"]["invalid_payload"] == 1
            assert "KXBTC-24JUN28-B65000" in summary["failed_tickers_sample"]

    asyncio.run(_run())
