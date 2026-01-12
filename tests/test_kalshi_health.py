import asyncio
import time

import aiosqlite

from kalshi_bot.data import init_db
from kalshi_bot.kalshi.health import get_quote_coverage, get_quote_freshness


def test_health_metrics_with_quotes(tmp_path):
    db_path = tmp_path / "health.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.executemany(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                [
                    ("KXBTC-AAA", now, "active"),
                    ("KXBTC15M-BBB", now, "active"),
                    ("OTHER-CCC", now, "active"),
                ],
            )
            await conn.executemany(
                "INSERT INTO kalshi_quotes (ts, market_id, raw_json) VALUES (?, ?, ?)",
                [
                    (now - 10, "KXBTC-AAA", "{}"),
                    (now - 5, "OTHER-CCC", "{}"),
                ],
            )
            await conn.commit()

            freshness = await get_quote_freshness(
                conn, within_seconds=30, status=None, series=["KXBTC"]
            )
            assert freshness["latest_ts"] == now - 10
            assert freshness["is_fresh"] is True

            coverage = await get_quote_coverage(
                conn, lookback_seconds=30, status=None, series=["KXBTC", "KXBTC15M"]
            )
            assert coverage["total_markets"] == 2
            assert coverage["markets_with_quotes"] == 1
            assert coverage["coverage_pct"] == 50.0

    asyncio.run(_run())


def test_health_metrics_with_no_quotes(tmp_path):
    db_path = tmp_path / "health_empty.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("KXBTC-AAA", now, "active"),
            )
            await conn.commit()

            freshness = await get_quote_freshness(
                conn, within_seconds=30, status=None, series=["KXBTC"]
            )
            assert freshness["latest_ts"] is None
            assert freshness["is_fresh"] is False

            coverage = await get_quote_coverage(
                conn, lookback_seconds=30, status=None, series=["KXBTC"]
            )
            assert coverage["total_markets"] == 1
            assert coverage["markets_with_quotes"] == 0
            assert coverage["coverage_pct"] == 0.0

    asyncio.run(_run())
