import asyncio
import time

import aiosqlite

from kalshi_bot.data import init_db
from kalshi_bot.kalshi.health import (
    evaluate_smoke_conditions,
    get_relevant_quote_coverage,
    get_relevant_universe,
)


def test_relevant_universe_pct_band(tmp_path):
    db_path = tmp_path / "relevance.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO spot_ticks (ts, product_id, price, raw_json) VALUES (?, ?, ?, ?)",
                (now, "BTC-USD", 100.0, "{}"),
            )
            await conn.executemany(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                [
                    ("KXBTC-AAA", now, "active"),
                    ("KXBTC15M-BBB", now, "active"),
                    ("KXBTC15M-CCC", now, "active"),
                    ("OTHER-DDD", now, "active"),
                ],
            )
            await conn.executemany(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ("KXBTC-AAA", 102.0, None, "greater", None, now),
                    ("KXBTC15M-BBB", None, 98.0, "less", None, now),
                    ("KXBTC15M-CCC", 95.0, 105.0, "between", None, now),
                    ("OTHER-DDD", 110.0, None, "greater", None, now),
                ],
            )
            await conn.commit()

            relevant_ids, spot_price, summary = await get_relevant_universe(
                conn,
                pct_band=3.0,
                top_n=5,
                status="active",
                series=["KXBTC", "KXBTC15M"],
                product_id="BTC-USD",
            )

            assert spot_price == 100.0
            assert summary["method"] == "pct_band"
            assert set(relevant_ids) == {
                "KXBTC-AAA",
                "KXBTC15M-BBB",
                "KXBTC15M-CCC",
            }

    asyncio.run(_run())


def test_relevant_universe_top_n_fallback(tmp_path):
    db_path = tmp_path / "relevance_fallback.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO spot_ticks (ts, product_id, price, raw_json) VALUES (?, ?, ?, ?)",
                (now, "BTC-USD", 100.0, "{}"),
            )
            await conn.executemany(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                [
                    ("KXBTC-AAA", now, "active"),
                    ("KXBTC15M-BBB", now, "active"),
                    ("KXBTC15M-CCC", now, "active"),
                ],
            )
            await conn.executemany(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ("KXBTC-AAA", 102.0, None, "greater", None, now),
                    ("KXBTC15M-BBB", None, 98.0, "less", None, now),
                    ("KXBTC15M-CCC", 95.0, 105.0, "between", None, now),
                ],
            )
            await conn.commit()

            relevant_ids, _, summary = await get_relevant_universe(
                conn,
                pct_band=1.0,
                top_n=2,
                status="active",
                series=["KXBTC", "KXBTC15M"],
                product_id="BTC-USD",
            )

            assert summary["method"] == "top_n"
            assert relevant_ids == ["KXBTC15M-CCC", "KXBTC-AAA"]

    asyncio.run(_run())


def test_relevant_quote_coverage(tmp_path):
    db_path = tmp_path / "relevance_coverage.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.executemany(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                [
                    ("KXBTC-AAA", now, "active"),
                    ("KXBTC15M-BBB", now, "active"),
                ],
            )
            await conn.executemany(
                "INSERT INTO kalshi_quotes (ts, market_id, raw_json) VALUES (?, ?, ?)",
                [
                    (now - 10, "KXBTC-AAA", "{}"),
                ],
            )
            await conn.commit()

            coverage = await get_relevant_quote_coverage(
                conn,
                market_ids=["KXBTC-AAA", "KXBTC15M-BBB"],
                freshness_seconds=30,
            )
            assert coverage["relevant_total"] == 2
            assert coverage["relevant_with_recent_quotes"] == 1
            assert coverage["relevant_coverage_pct"] == 50.0

    asyncio.run(_run())


def test_smoke_conditions_ignore_global():
    failures, reasons = evaluate_smoke_conditions(
        latest_age_seconds=10,
        freshness_seconds=30,
        relevant_total=10,
        relevant_coverage_pct=90.0,
        min_relevant_coverage=80.0,
    )
    assert failures is False
    assert reasons == []

    failures, reasons = evaluate_smoke_conditions(
        latest_age_seconds=10,
        freshness_seconds=30,
        relevant_total=10,
        relevant_coverage_pct=50.0,
        min_relevant_coverage=80.0,
    )
    assert failures is True
    assert "low_relevant_coverage" in reasons
