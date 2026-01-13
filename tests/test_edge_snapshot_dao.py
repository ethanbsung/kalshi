import asyncio
import time
import importlib.util
from pathlib import Path

import aiosqlite

from kalshi_bot.data import init_db
from kalshi_bot.data.dao import Dao


def test_insert_kalshi_edge_snapshot(tmp_path):
    db_path = tmp_path / "edge_snapshot.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status, raw_json) "
                "VALUES (?, ?, ?, ?)",
                ("KXBTC-SNAP", now, "active", "{}"),
            )
            await conn.commit()

            dao = Dao(conn)
            row = {
                "asof_ts": now,
                "market_id": "KXBTC-SNAP",
                "settlement_ts": now + 3600,
                "spot_ts": now - 60,
                "spot_price": 30000.0,
                "sigma_annualized": 0.5,
                "prob_yes": 0.6,
                "prob_yes_raw": 0.6000000001,
                "horizon_seconds": (now + 3600) - (now - 60),
                "quote_ts": now - 10,
                "yes_bid": 45.0,
                "yes_ask": 55.0,
                "no_bid": 40.0,
                "no_ask": 60.0,
                "yes_mid": 50.0,
                "no_mid": 50.0,
                "ev_take_yes": 0.01,
                "ev_take_no": -0.02,
                "spot_age_seconds": 60,
                "quote_age_seconds": 10,
                "skip_reason": None,
                "raw_json": "{}",
            }
            await dao.insert_kalshi_edge_snapshot(row)
            await conn.commit()

            cursor = await conn.execute(
                "SELECT COUNT(*) FROM kalshi_edge_snapshots"
            )
            count = (await cursor.fetchone())[0]
            assert count == 1

            cursor = await conn.execute(
                "SELECT market_id, prob_yes FROM kalshi_edge_snapshots"
            )
            snapshot = await cursor.fetchone()
            assert snapshot == ("KXBTC-SNAP", 0.6)

    asyncio.run(_run())


def _load_run_live_edges() -> object:
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_live_edges.py"
    spec = importlib.util.spec_from_file_location("run_live_edges", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_run_live_edges_once(tmp_path):
    db_path = tmp_path / "live_edges.sqlite"
    module = _load_run_live_edges()

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO spot_ticks (ts, product_id, price, raw_json) VALUES (?, ?, ?, ?)",
                (now, "BTC-USD", 30000.0, "{}"),
            )
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status, raw_json) "
                "VALUES (?, ?, ?, ?)",
                ("KXBTC-LIVE", now, "active", "{}"),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("KXBTC-LIVE", 29900.0, None, "greater", now + 3600, now),
            )
            await conn.execute(
                "INSERT INTO kalshi_quotes (ts, market_id, yes_bid, yes_ask, no_bid, no_ask, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (now, "KXBTC-LIVE", 45.0, 55.0, 40.0, 60.0, "{}"),
            )
            await conn.commit()

            args = type(
                "Args",
                (),
                {
                    "product_id": "BTC-USD",
                    "lookback_seconds": 3600,
                    "max_spot_points": 10,
                    "ewma_lambda": 0.9,
                    "min_points": 1,
                    "min_sigma_lookback_seconds": 0,
                    "sigma_resample_seconds": 5,
                    "sigma_default": 0.1,
                    "sigma_max": 2.0,
                    "status": "active",
                    "series": ["KXBTC"],
                    "pct_band": 10.0,
                    "top_n": 10,
                    "freshness_seconds": 60,
                    "max_horizon_seconds": 7 * 24 * 3600,
                    "min_ask_cents": 1.0,
                    "max_ask_cents": 99.0,
                    "contracts": 1,
                    "debug_market": [],
                    "debug": False,
                    "show_titles": False,
                    "now_ts": now,
                },
            )()

            summary = await module.run_tick(conn, args)
            assert summary.get("snapshots_inserted") == 1

            cursor = await conn.execute(
                "SELECT asof_ts, market_id, spot_ts, spot_price, sigma_annualized, prob_yes, raw_json "
                "FROM kalshi_edge_snapshots"
            )
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == now
            assert row[1] == "KXBTC-LIVE"
            assert row[2] == now
            assert row[3] == 30000.0
            assert row[4] is not None
            assert row[5] is not None
            assert row[6] is not None

    asyncio.run(_run())
