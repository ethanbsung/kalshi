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
                    ("KXBTC-AAA", 102.0, None, "greater", now + 3600, now),
                    ("KXBTC15M-BBB", None, 98.0, "less", now + 3600, now),
                    ("KXBTC15M-CCC", 95.0, 105.0, "between", now + 3600, now),
                    ("OTHER-DDD", 110.0, None, "greater", now + 3600, now),
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
                now_ts=now,
                require_quotes=False,
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
                    ("KXBTC-AAA", 102.0, None, "greater", now + 3600, now),
                    ("KXBTC15M-BBB", None, 98.0, "less", now + 3600, now),
                    ("KXBTC15M-CCC", 95.0, 105.0, "between", now + 3600, now),
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
                now_ts=now,
                require_quotes=False,
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


def test_relevant_universe_excludes_expired(tmp_path):
    db_path = tmp_path / "relevance_expired.sqlite"

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
                    ("KXBTC-EXP", now, "active"),
                    ("KXBTC-OK", now, "active"),
                ],
            )
            await conn.executemany(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ("KXBTC-EXP", None, 99.0, "less", now - 10, now),
                    ("KXBTC-OK", None, 99.0, "less", now + 3600, now),
                ],
            )
            await conn.commit()

            relevant_ids, _, summary = await get_relevant_universe(
                conn,
                pct_band=5.0,
                top_n=10,
                status="active",
                series=["KXBTC"],
                product_id="BTC-USD",
                now_ts=now,
                require_quotes=False,
            )
            assert relevant_ids == ["KXBTC-OK"]
            assert summary["excluded_expired"] == 1

    asyncio.run(_run())


def test_relevant_universe_excludes_horizon_out_of_range(tmp_path):
    db_path = tmp_path / "relevance_horizon.sqlite"

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
                    ("KXBTC-FAR", now, "active"),
                ],
            )
            await conn.executemany(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        "KXBTC-FAR",
                        None,
                        99.0,
                        "less",
                        now + 11 * 24 * 3600,
                        now,
                    ),
                ],
            )
            await conn.commit()

            relevant_ids, _, summary = await get_relevant_universe(
                conn,
                pct_band=5.0,
                top_n=10,
                status="active",
                series=["KXBTC"],
                product_id="BTC-USD",
                now_ts=now,
                max_horizon_seconds=10 * 24 * 3600,
                grace_seconds=0,
                require_quotes=False,
            )
            assert relevant_ids == []
            assert summary["excluded_horizon_out_of_range"] == 1

    asyncio.run(_run())


def test_relevant_universe_quote_freshness(tmp_path):
    db_path = tmp_path / "relevance_quotes.sqlite"

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
                    ("KXBTC-OLD", now, "active"),
                    ("KXBTC-NEW", now, "active"),
                ],
            )
            await conn.executemany(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ("KXBTC-OLD", None, 99.0, "less", now + 3600, now),
                    ("KXBTC-NEW", None, 99.0, "less", now + 3600, now),
                ],
            )
            await conn.executemany(
                "INSERT INTO kalshi_quotes (ts, market_id, yes_bid, yes_ask, no_bid, no_ask, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (now - 400, "KXBTC-OLD", 40.0, 60.0, 40.0, 60.0, "{}"),
                    (now - 10, "KXBTC-NEW", 40.0, 60.0, 40.0, 60.0, "{}"),
                ],
            )
            await conn.commit()

            relevant_ids, _, summary = await get_relevant_universe(
                conn,
                pct_band=5.0,
                top_n=10,
                status="active",
                series=["KXBTC"],
                product_id="BTC-USD",
                now_ts=now,
                freshness_seconds=60,
                require_quotes=True,
            )
            assert relevant_ids == ["KXBTC-NEW"]
            assert summary["excluded_missing_recent_quote"] == 1

    asyncio.run(_run())


def test_relevant_universe_excludes_untradable_asks(tmp_path):
    db_path = tmp_path / "relevance_untradable.sqlite"

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
                    ("KXBTC-BAD", now, "active"),
                    ("KXBTC-GOOD", now, "active"),
                ],
            )
            await conn.executemany(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ("KXBTC-BAD", None, 99.0, "less", now + 3600, now),
                    ("KXBTC-GOOD", None, 99.0, "less", now + 3600, now),
                ],
            )
            await conn.executemany(
                "INSERT INTO kalshi_quotes (ts, market_id, yes_bid, yes_ask, no_bid, no_ask, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (now - 10, "KXBTC-BAD", 0.0, 0.0, 100.0, 100.0, "{}"),
                    (now - 10, "KXBTC-GOOD", 40.0, 60.0, 40.0, 60.0, "{}"),
                ],
            )
            await conn.commit()

            relevant_ids, _, summary = await get_relevant_universe(
                conn,
                pct_band=5.0,
                top_n=10,
                status="active",
                series=["KXBTC"],
                product_id="BTC-USD",
                now_ts=now,
                freshness_seconds=60,
                require_quotes=True,
            )
            assert relevant_ids == ["KXBTC-GOOD"]
            assert summary["excluded_untradable"] == 1

    asyncio.run(_run())


def test_relevant_universe_allows_missing_quotes_when_disabled(tmp_path):
    db_path = tmp_path / "relevance_no_quotes.sqlite"

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
                    ("KXBTC-NOQUOTE", now, "active"),
                ],
            )
            await conn.executemany(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ("KXBTC-NOQUOTE", None, 99.0, "less", now + 3600, now),
                ],
            )
            await conn.commit()

            relevant_ids, _, summary = await get_relevant_universe(
                conn,
                pct_band=5.0,
                top_n=10,
                status="active",
                series=["KXBTC"],
                product_id="BTC-USD",
                now_ts=now,
                require_quotes=False,
            )
            assert relevant_ids == ["KXBTC-NOQUOTE"]
            assert summary["excluded_missing_recent_quote"] == 0

    asyncio.run(_run())
