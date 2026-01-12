import asyncio

import aiosqlite

from kalshi_bot.data import init_db
from kalshi_bot.kalshi.contracts import load_market_rows
from kalshi_bot.kalshi.market_filters import fetch_status_counts, no_markets_message
from kalshi_bot.kalshi.quotes import load_market_tickers


def test_default_selection_includes_active_markets(tmp_path):
    db_path = tmp_path / "markets.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("KXBTC-AAA", 1700000000, "active"),
            )
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("KXBTC15M-BBB", 1700000000, "active"),
            )
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("OTHER-CCC", 1700000000, "active"),
            )
            await conn.commit()

            tickers = await load_market_tickers(
                conn, status=None, series=["KXBTC", "KXBTC15M"]
            )
            rows = await load_market_rows(
                conn, status=None, series=["KXBTC", "KXBTC15M"]
            )

            assert tickers == ["KXBTC-AAA", "KXBTC15M-BBB"]
            assert [row[0] for row in rows] == ["KXBTC-AAA", "KXBTC15M-BBB"]

    asyncio.run(_run())


def test_status_filter_and_message(tmp_path):
    db_path = tmp_path / "markets.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("KXBTC-AAA", 1700000000, "active"),
            )
            await conn.commit()

            tickers = await load_market_tickers(
                conn, status="open", series=["KXBTC"]
            )
            assert tickers == []

            counts = await fetch_status_counts(conn)
            message = no_markets_message("open", counts)
            assert "active" in message
            assert "status=open" in message

    asyncio.run(_run())


def test_series_prefix_filter(tmp_path):
    db_path = tmp_path / "markets.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("KXBTC-AAA", 1700000000, "active"),
            )
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("OTHER-CCC", 1700000000, "active"),
            )
            await conn.commit()

            tickers = await load_market_tickers(conn, status=None, series=["KXBTC"])
            assert tickers == ["KXBTC-AAA"]

    asyncio.run(_run())
