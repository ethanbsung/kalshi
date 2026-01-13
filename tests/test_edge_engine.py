import asyncio
import time

import aiosqlite

from kalshi_bot.data import init_db
from kalshi_bot.models.probability import (
    prob_between,
    prob_greater_equal,
    prob_less_equal,
)
from kalshi_bot.strategy.edge_engine import (
    EdgeInputs,
    compute_edge_for_market,
    compute_edges,
    prob_yes_for_contract,
)


def test_prob_yes_for_contract_variants():
    spot = 100.0
    sigma = 0.5
    horizon = 3600

    contract_less = {"strike_type": "less", "upper": 105.0}
    assert prob_yes_for_contract(spot, sigma, horizon, contract_less) == prob_less_equal(
        spot, 105.0, horizon, sigma
    )

    contract_greater = {"strike_type": "greater", "lower": 95.0}
    assert prob_yes_for_contract(spot, sigma, horizon, contract_greater) == prob_greater_equal(
        spot, 95.0, horizon, sigma
    )

    contract_between = {"strike_type": "between", "lower": 95.0, "upper": 105.0}
    assert prob_yes_for_contract(spot, sigma, horizon, contract_between) == prob_between(
        spot, 95.0, 105.0, horizon, sigma
    )


def test_prob_yes_for_less_not_extreme():
    spot = 100.0
    sigma = 0.6
    horizon = 3600
    contract = {"strike_type": "less", "upper": 101.0}
    prob = prob_yes_for_contract(spot, sigma, horizon, contract)
    assert prob is not None
    assert 0.05 < prob < 0.95


def test_prob_yes_for_less_monotonic():
    spot = 100.0
    sigma = 0.6
    horizon = 3600
    low = prob_yes_for_contract(
        spot, sigma, horizon, {"strike_type": "less", "upper": 95.0}
    )
    high = prob_yes_for_contract(
        spot, sigma, horizon, {"strike_type": "less", "upper": 105.0}
    )
    assert low is not None and high is not None
    assert low <= high


def test_prob_yes_missing_bounds_returns_none():
    spot = 100.0
    sigma = 0.5
    horizon = 3600
    contract = {"strike_type": "less", "upper": None}
    assert prob_yes_for_contract(spot, sigma, horizon, contract) is None


def test_compute_edge_returns_none_on_invalid_prob():
    inputs = EdgeInputs(
        spot_price=100.0,
        spot_ts=0,
        sigma_annualized=0.0,
        now_ts=0,
    )
    contract = {"ticker": "KXBTC-AAA", "strike_type": "less", "upper": 100.0, "settlement_ts": 100}
    quote = {"yes_ask": 50.0, "no_ask": 50.0, "ts": 0}

    edge = compute_edge_for_market(
        contract, quote, inputs, lambda *_: 0.0, horizon_seconds=3600
    )
    assert edge is None


def test_compute_edges_inserts_row(tmp_path):
    db_path = tmp_path / "edges.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.executemany(
                "INSERT INTO spot_ticks (ts, product_id, price, raw_json) VALUES (?, ?, ?, ?)",
                [
                    (now - 120, "BTC-USD", 30000.0, "{}"),
                    (now - 60, "BTC-USD", 30100.0, "{}"),
                    (now, "BTC-USD", 30200.0, "{}"),
                ],
            )
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status, title) VALUES (?, ?, ?, ?)",
                ("KXBTC-AAA", now, "active", "BTC test market"),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) VALUES (?, ?, ?, ?, ?, ?)",
                ("KXBTC-AAA", 30000.0, None, "greater", now + 3600, now),
            )
            await conn.execute(
                "INSERT INTO kalshi_quotes (ts, market_id, yes_bid, yes_ask, no_bid, no_ask, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (now, "KXBTC-AAA", 45.0, 55.0, 40.0, 60.0, "{}"),
            )
            await conn.commit()

            summary = await compute_edges(
                conn,
                product_id="BTC-USD",
                lookback_seconds=3600,
                max_spot_points=10,
                ewma_lambda=0.9,
                min_points=1,
                sigma_default=0.1,
                sigma_max=2.0,
                status="active",
                series=["KXBTC"],
                pct_band=10.0,
                top_n=10,
                freshness_seconds=60,
                max_horizon_seconds=7 * 24 * 3600,
                contracts=1,
                now_ts=now,
            )
            assert summary["edges_inserted"] == 1
            assert summary["relevant_titles_sample"].get("KXBTC-AAA") == "BTC test market"
            assert summary["latest_quote_ts"] == now
            assert summary["quotes_distinct_markets_recent"] == 1
            assert summary["relevant_with_recent_quotes"] == 1

            cursor = await conn.execute(
                "SELECT market_id, prob_yes, ev_take_yes FROM kalshi_edges"
            )
            row = await cursor.fetchone()
            assert row[0] == "KXBTC-AAA"
            assert row[1] is not None
            assert row[2] is not None

    asyncio.run(_run())


def test_compute_edges_uses_spot_ts_for_horizon(tmp_path):
    db_path = tmp_path / "horizon.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        spot_ts = now - 600
        settlement_ts = now + 600
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO spot_ticks (ts, product_id, price, raw_json) VALUES (?, ?, ?, ?)",
                (spot_ts, "BTC-USD", 30000.0, "{}"),
            )
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status, title) VALUES (?, ?, ?, ?)",
                ("KXBTC-HORIZON", now, "active", "BTC test"),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("KXBTC-HORIZON", 29900.0, None, "greater", settlement_ts, now),
            )
            await conn.execute(
                "INSERT INTO kalshi_quotes (ts, market_id, yes_bid, yes_ask, no_bid, no_ask, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (now, "KXBTC-HORIZON", 45.0, 55.0, 40.0, 60.0, "{}"),
            )
            await conn.commit()

            summary = await compute_edges(
                conn,
                product_id="BTC-USD",
                lookback_seconds=3600,
                max_spot_points=10,
                ewma_lambda=0.9,
                min_points=1,
                min_sigma_lookback_seconds=0,
                resample_seconds=5,
                sigma_default=0.1,
                sigma_max=2.0,
                status="active",
                series=["KXBTC"],
                pct_band=10.0,
                top_n=10,
                freshness_seconds=60,
                max_horizon_seconds=7 * 24 * 3600,
                now_ts=now,
            )
            assert summary["edges_inserted"] == 1

            cursor = await conn.execute(
                "SELECT horizon_seconds FROM kalshi_edges WHERE market_id = ?",
                ("KXBTC-HORIZON",),
            )
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == settlement_ts - spot_ts

    asyncio.run(_run())


def test_compute_edges_uses_resample_seconds(tmp_path):
    db_path = tmp_path / "resample.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.executemany(
                "INSERT INTO spot_ticks (ts, product_id, price, raw_json) VALUES (?, ?, ?, ?)",
                [
                    (now - 9, "BTC-USD", 30000.0, "{}"),
                    (now - 5, "BTC-USD", 30010.0, "{}"),
                    (now - 1, "BTC-USD", 30020.0, "{}"),
                ],
            )
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("KXBTC-RESAMPLE", now, "active"),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("KXBTC-RESAMPLE", 30000.0, None, "greater", now + 3600, now),
            )
            await conn.execute(
                "INSERT INTO kalshi_quotes (ts, market_id, yes_bid, yes_ask, no_bid, no_ask, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (now, "KXBTC-RESAMPLE", 45.0, 55.0, 40.0, 60.0, "{}"),
            )
            await conn.commit()

            summary = await compute_edges(
                conn,
                product_id="BTC-USD",
                lookback_seconds=3600,
                max_spot_points=10,
                ewma_lambda=0.9,
                min_points=1,
                min_sigma_lookback_seconds=0,
                resample_seconds=5,
                sigma_default=0.1,
                sigma_max=2.0,
                status="active",
                series=["KXBTC"],
                pct_band=10.0,
                top_n=10,
                freshness_seconds=60,
                max_horizon_seconds=7 * 24 * 3600,
                now_ts=now,
            )
            assert summary["step_seconds"] == 5.0
            assert summary["resample_seconds"] == 5
            assert summary["resampled_points"] <= summary["raw_points"]

    asyncio.run(_run())


def test_compute_edges_sigma_fallback_when_span_short(tmp_path):
    db_path = tmp_path / "sigma_span.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.executemany(
                "INSERT INTO spot_ticks (ts, product_id, price, raw_json) VALUES (?, ?, ?, ?)",
                [
                    (now - 20, "BTC-USD", 30000.0, "{}"),
                    (now - 15, "BTC-USD", 30010.0, "{}"),
                    (now - 10, "BTC-USD", 30020.0, "{}"),
                    (now - 5, "BTC-USD", 30030.0, "{}"),
                ],
            )
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("KXBTC-SIGMA", now, "active"),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("KXBTC-SIGMA", 30000.0, None, "greater", now + 3600, now),
            )
            await conn.execute(
                "INSERT INTO kalshi_quotes (ts, market_id, yes_bid, yes_ask, no_bid, no_ask, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (now, "KXBTC-SIGMA", 45.0, 55.0, 40.0, 60.0, "{}"),
            )
            await conn.commit()

            summary = await compute_edges(
                conn,
                product_id="BTC-USD",
                lookback_seconds=3600,
                max_spot_points=50,
                ewma_lambda=0.9,
                min_points=2,
                min_sigma_lookback_seconds=120,
                resample_seconds=10,
                sigma_default=0.1,
                sigma_max=2.0,
                status="active",
                series=["KXBTC"],
                pct_band=10.0,
                top_n=10,
                freshness_seconds=60,
                max_horizon_seconds=7 * 24 * 3600,
                now_ts=now,
            )
            assert summary["sigma_source"] == "default"
            assert summary["sigma_reason"] == "insufficient_history_span"
            assert summary["sigma_quality"] == "fallback_default"

    asyncio.run(_run())


def test_compute_edges_sigma_ewma_when_span_sufficient(tmp_path):
    db_path = tmp_path / "sigma_ok.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.executemany(
                "INSERT INTO spot_ticks (ts, product_id, price, raw_json) VALUES (?, ?, ?, ?)",
                [
                    (now - 120, "BTC-USD", 30000.0, "{}"),
                    (now - 60, "BTC-USD", 30050.0, "{}"),
                    (now, "BTC-USD", 30100.0, "{}"),
                ],
            )
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("KXBTC-SIGMA-OK", now, "active"),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("KXBTC-SIGMA-OK", 30000.0, None, "greater", now + 3600, now),
            )
            await conn.execute(
                "INSERT INTO kalshi_quotes (ts, market_id, yes_bid, yes_ask, no_bid, no_ask, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (now, "KXBTC-SIGMA-OK", 45.0, 55.0, 40.0, 60.0, "{}"),
            )
            await conn.commit()

            summary = await compute_edges(
                conn,
                product_id="BTC-USD",
                lookback_seconds=3600,
                max_spot_points=50,
                ewma_lambda=0.9,
                min_points=1,
                min_sigma_lookback_seconds=60,
                resample_seconds=60,
                sigma_default=0.1,
                sigma_max=2.0,
                status="active",
                series=["KXBTC"],
                pct_band=10.0,
                top_n=10,
                freshness_seconds=60,
                max_horizon_seconds=7 * 24 * 3600,
                now_ts=now,
            )
            assert summary["sigma_source"] == "ewma"
            assert summary["sigma_quality"] == "ok"

    asyncio.run(_run())


def test_compute_edges_inserts_with_missing_side(tmp_path):
    db_path = tmp_path / "edges_missing_side.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.executemany(
                "INSERT INTO spot_ticks (ts, product_id, price, raw_json) VALUES (?, ?, ?, ?)",
                [
                    (now - 120, "BTC-USD", 30000.0, "{}"),
                    (now - 60, "BTC-USD", 30100.0, "{}"),
                    (now, "BTC-USD", 30200.0, "{}"),
                ],
            )
            await conn.executemany(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                [
                    ("KXBTC-MISS-NO", now, "active"),
                    ("KXBTC-MISS-YES", now, "active"),
                ],
            )
            await conn.executemany(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ("KXBTC-MISS-NO", 30000.0, None, "greater", now + 3600, now),
                    ("KXBTC-MISS-YES", 30000.0, None, "greater", now + 3600, now),
                ],
            )
            await conn.executemany(
                "INSERT INTO kalshi_quotes (ts, market_id, yes_bid, yes_ask, no_bid, no_ask, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (now, "KXBTC-MISS-NO", 45.0, 55.0, 40.0, None, "{}"),
                    (now, "KXBTC-MISS-YES", 45.0, None, 40.0, 60.0, "{}"),
                ],
            )
            await conn.commit()

            summary = await compute_edges(
                conn,
                product_id="BTC-USD",
                lookback_seconds=3600,
                max_spot_points=10,
                ewma_lambda=0.9,
                min_points=1,
                sigma_default=0.1,
                sigma_max=2.0,
                status="active",
                series=["KXBTC"],
                pct_band=10.0,
                top_n=10,
                freshness_seconds=60,
                max_horizon_seconds=7 * 24 * 3600,
                now_ts=now,
            )
            assert summary["edges_inserted"] == 2
            assert summary["skip_reasons"].get("missing_no_ask") == 1
            assert summary["skip_reasons"].get("missing_yes_ask") == 1

            cursor = await conn.execute(
                "SELECT market_id, ev_take_yes, ev_take_no FROM kalshi_edges "
                "ORDER BY market_id"
            )
            rows = await cursor.fetchall()
            assert rows[0][0] == "KXBTC-MISS-NO"
            assert rows[0][1] is not None
            assert rows[0][2] is None
            assert rows[1][0] == "KXBTC-MISS-YES"
            assert rows[1][1] is None
            assert rows[1][2] is not None

    asyncio.run(_run())


def test_compute_edges_skips_crossed_market(tmp_path):
    db_path = tmp_path / "crossed_market.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.executemany(
                "INSERT INTO spot_ticks (ts, product_id, price, raw_json) VALUES (?, ?, ?, ?)",
                [
                    (now - 60, "BTC-USD", 30000.0, "{}"),
                    (now, "BTC-USD", 30100.0, "{}"),
                ],
            )
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("KXBTC-CROSS", now, "active"),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("KXBTC-CROSS", 30000.0, None, "greater", now + 3600, now),
            )
            await conn.execute(
                "INSERT INTO kalshi_quotes (ts, market_id, yes_bid, yes_ask, no_bid, no_ask, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (now, "KXBTC-CROSS", 35.0, 40.0, 45.0, 50.0, "{}"),
            )
            await conn.commit()

            summary = await compute_edges(
                conn,
                product_id="BTC-USD",
                lookback_seconds=3600,
                max_spot_points=10,
                ewma_lambda=0.9,
                min_points=1,
                min_sigma_lookback_seconds=0,
                resample_seconds=5,
                sigma_default=0.1,
                sigma_max=2.0,
                status="active",
                series=["KXBTC"],
                pct_band=10.0,
                top_n=10,
                freshness_seconds=60,
                max_horizon_seconds=7 * 24 * 3600,
                now_ts=now,
            )
            assert summary["edges_inserted"] == 0
            assert summary["skip_reasons"].get("crossed_market") == 1

    asyncio.run(_run())


def test_compute_edges_horizon_grace_allows_slight_over(tmp_path):
    db_path = tmp_path / "edges_grace.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.executemany(
                "INSERT INTO spot_ticks (ts, product_id, price, raw_json) VALUES (?, ?, ?, ?)",
                [
                    (now - 120, "BTC-USD", 30000.0, "{}"),
                    (now - 60, "BTC-USD", 30100.0, "{}"),
                    (now, "BTC-USD", 30200.0, "{}"),
                ],
            )
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("KXBTC-GRACE", now, "active"),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "KXBTC-GRACE",
                    30000.0,
                    None,
                    "greater",
                    now + 7 * 24 * 3600 + 1800,
                    now,
                ),
            )
            await conn.execute(
                "INSERT INTO kalshi_quotes (ts, market_id, yes_bid, yes_ask, no_bid, no_ask, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (now, "KXBTC-GRACE", 45.0, 55.0, 40.0, 60.0, "{}"),
            )
            await conn.commit()

            summary = await compute_edges(
                conn,
                product_id="BTC-USD",
                lookback_seconds=3600,
                max_spot_points=10,
                ewma_lambda=0.9,
                min_points=1,
                sigma_default=0.1,
                sigma_max=2.0,
                status="active",
                series=["KXBTC"],
                pct_band=10.0,
                top_n=10,
                freshness_seconds=60,
                max_horizon_seconds=7 * 24 * 3600,
                now_ts=now,
            )
            assert summary["edges_inserted"] == 1
            assert summary["skip_reasons"].get("horizon_out_of_range") is None

    asyncio.run(_run())


def test_compute_edges_between_probability_reasonable(tmp_path):
    db_path = tmp_path / "edges_between.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.executemany(
                "INSERT INTO spot_ticks (ts, product_id, price, raw_json) VALUES (?, ?, ?, ?)",
                [
                    (now - 120, "BTC-USD", 91100.0, "{}"),
                    (now - 60, "BTC-USD", 91100.0, "{}"),
                    (now, "BTC-USD", 91100.0, "{}"),
                ],
            )
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("KXBTC-BETWEEN", now, "active"),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "KXBTC-BETWEEN",
                    91000.0,
                    91249.0,
                    "between",
                    now + 7 * 24 * 3600,
                    now,
                ),
            )
            await conn.execute(
                "INSERT INTO kalshi_quotes (ts, market_id, yes_bid, yes_ask, no_bid, no_ask, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (now, "KXBTC-BETWEEN", 45.0, 55.0, 40.0, 60.0, "{}"),
            )
            await conn.commit()

            summary = await compute_edges(
                conn,
                product_id="BTC-USD",
                lookback_seconds=3600,
                max_spot_points=10,
                ewma_lambda=0.9,
                min_points=1,
                sigma_default=0.2,
                sigma_max=5.0,
                status="active",
                series=["KXBTC"],
                pct_band=1.0,
                top_n=10,
                freshness_seconds=3600,
                max_horizon_seconds=10 * 24 * 3600,
                now_ts=now,
            )
            assert summary["edges_inserted"] == 1

            cursor = await conn.execute(
                "SELECT prob_yes FROM kalshi_edges WHERE market_id = ?",
                ("KXBTC-BETWEEN",),
            )
            row = await cursor.fetchone()
            assert row is not None
            prob = row[0]
            assert prob is not None
            assert 0.01 < prob < 0.2

    asyncio.run(_run())

def test_compute_edges_skips_expired_contract(tmp_path):
    db_path = tmp_path / "expired.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.executemany(
                "INSERT INTO spot_ticks (ts, product_id, price, raw_json) VALUES (?, ?, ?, ?)",
                [
                    (now - 60, "BTC-USD", 30000.0, "{}"),
                    (now, "BTC-USD", 30100.0, "{}"),
                ],
            )
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("KXBTC-EXP", now, "active"),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("KXBTC-EXP", 30000.0, None, "greater", now - 1000, now),
            )
            await conn.execute(
                "INSERT INTO kalshi_quotes (ts, market_id, yes_bid, yes_ask, no_bid, no_ask, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (now, "KXBTC-EXP", 45.0, 55.0, 40.0, 60.0, "{}"),
            )
            await conn.commit()

            summary = await compute_edges(
                conn,
                product_id="BTC-USD",
                lookback_seconds=3600,
                max_spot_points=10,
                ewma_lambda=0.9,
                min_points=1,
                sigma_default=0.1,
                sigma_max=2.0,
                status="active",
                series=["KXBTC"],
                pct_band=10.0,
                top_n=10,
                freshness_seconds=60,
                max_horizon_seconds=7 * 24 * 3600,
                now_ts=now,
            )
            assert summary["edges_inserted"] == 0
            assert summary.get("error") == "no_relevant_markets"
            selection = summary.get("selection", {})
            assert selection.get("excluded_expired") == 1

    asyncio.run(_run())


def test_compute_edges_skips_invalid_edge(tmp_path):
    db_path = tmp_path / "invalid_edge.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.executemany(
                "INSERT INTO spot_ticks (ts, product_id, price, raw_json) VALUES (?, ?, ?, ?)",
                [
                    (now - 60, "BTC-USD", 30000.0, "{}"),
                    (now, "BTC-USD", 30100.0, "{}"),
                ],
            )
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status) VALUES (?, ?, ?)",
                ("KXBTC-BAD", now, "active"),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("KXBTC-BAD", None, -1.0, "less", now + 3600, now),
            )
            await conn.execute(
                "INSERT INTO kalshi_quotes (ts, market_id, yes_bid, yes_ask, no_bid, no_ask, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (now, "KXBTC-BAD", 45.0, 55.0, 40.0, 60.0, "{}"),
            )
            await conn.commit()

            summary = await compute_edges(
                conn,
                product_id="BTC-USD",
                lookback_seconds=3600,
                max_spot_points=10,
                ewma_lambda=0.9,
                min_points=1,
                sigma_default=0.1,
                sigma_max=2.0,
                status="active",
                series=["KXBTC"],
                pct_band=0.1,
                top_n=1,
                freshness_seconds=60,
                max_horizon_seconds=7 * 24 * 3600,
                now_ts=now,
            )
            assert summary["edges_inserted"] == 0
            assert summary["skip_reasons"].get("invalid_edge") == 1

    asyncio.run(_run())


def test_latest_quote_selection(tmp_path):
    db_path = tmp_path / "quotes_latest.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.executemany(
                "INSERT INTO kalshi_quotes (ts, market_id, yes_bid, yes_ask, no_bid, no_ask, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (now - 400, "KXBTC-AAA", 10.0, 20.0, 80.0, 90.0, "{}"),
                    (now - 50, "KXBTC-AAA", 30.0, 40.0, 60.0, 70.0, "{}"),
                    (now - 200, "KXBTC-BBB", 15.0, 25.0, 75.0, 85.0, "{}"),
                ],
            )
            await conn.commit()

            from kalshi_bot.strategy.edge_engine import _load_latest_quotes

            quotes = await _load_latest_quotes(
                conn,
                ["KXBTC-AAA", "KXBTC-BBB"],
                freshness_seconds=300,
                now_ts=now,
            )
            assert quotes["KXBTC-AAA"]["ts"] == now - 50
            assert quotes["KXBTC-BBB"]["ts"] == now - 200

            quotes = await _load_latest_quotes(
                conn,
                ["KXBTC-AAA", "KXBTC-BBB"],
                freshness_seconds=100,
                now_ts=now,
            )
            assert "KXBTC-AAA" in quotes
            assert "KXBTC-BBB" not in quotes

    asyncio.run(_run())
