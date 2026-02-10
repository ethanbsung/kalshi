import asyncio
import time
import importlib.util
from pathlib import Path

import aiosqlite
import pytest

from kalshi_bot.data import init_db
from kalshi_bot.data.dao import Dao


def _load_report_module() -> object:
    script_path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "report_model_performance.py"
    )
    spec = importlib.util.spec_from_file_location(
        "report_model_performance", script_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_report_model_performance_summary(tmp_path):
    db_path = tmp_path / "report.sqlite"
    module = _load_report_module()

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status, raw_json) "
                "VALUES (?, ?, ?, ?)",
                ("KXBTC-REPORT", now, "active", "{}"),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, settled_ts, outcome, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "KXBTC-REPORT",
                    30000.0,
                    None,
                    "greater",
                    now - 10,
                    now - 5,
                    1,
                    now,
                ),
            )
            await conn.execute(
                "INSERT INTO kalshi_edge_snapshot_scores (asof_ts, market_id, settled_ts, outcome, pnl_take_yes, pnl_take_no, "
                "brier, logloss, error, created_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    now,
                    "KXBTC-REPORT",
                    now - 5,
                    1,
                    0.7,
                    -0.3,
                    0.01,
                    0.02,
                    None,
                    now,
                ),
            )
            await conn.commit()

            dao = Dao(conn)
            row = {
                "ts_eval": now,
                "market_id": "KXBTC-REPORT",
                "settlement_ts": now - 10,
                "strike": None,
                "spot_price": 30000.0,
                "sigma": 0.5,
                "tau": 1.0,
                "p_model": 0.6,
                "p_market": 0.55,
                "best_yes_bid": 45.0,
                "best_yes_ask": 30.0,
                "best_no_bid": 70.0,
                "best_no_ask": 70.0,
                "spread": 25.0,
                "eligible": 1,
                "reason_not_eligible": None,
                "would_trade": 1,
                "side": "YES",
                "ev_raw": 0.3,
                "ev_net": 0.3,
                "cost_buffer": None,
                "raw_json": "{\"price_used_cents\": 30}",
            }
            await dao.insert_opportunities([row])
            await conn.commit()

            report = await module.compute_report(conn, since_ts=now - 60)
            assert report["total"] == 1
            assert report["settled"] == 1
            assert report["unsettled"] == 0
            assert report["avg_model_brier"] == pytest.approx(0.16)
            assert report["avg_model_logloss"] == pytest.approx(0.5108256238)
            assert report["avg_market_brier"] == pytest.approx(0.2025)
            assert report["avg_market_logloss"] == pytest.approx(0.5978370008)
            assert report["delta_brier"] == pytest.approx(-0.0425)
            assert report["delta_logloss"] == pytest.approx(-0.0870113770)
            assert report["snapshot_score_matches_total"] == 1
            assert report["snapshot_score_matches_settled"] == 1
            assert report["snapshot_score_coverage_total"] == pytest.approx(1.0)
            assert report["snapshot_score_coverage_settled"] == pytest.approx(1.0)
            assert report["avg_brier"] == pytest.approx(report["avg_model_brier"])
            assert report["avg_logloss"] == pytest.approx(report["avg_model_logloss"])

    asyncio.run(_run())


def test_report_model_performance_uses_contract_outcome_without_score_row(tmp_path):
    db_path = tmp_path / "report_no_score.sqlite"
    module = _load_report_module()

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status, raw_json) "
                "VALUES (?, ?, ?, ?)",
                ("KXBTC-NOSCORE", now, "active", "{}"),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, settled_ts, outcome, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "KXBTC-NOSCORE",
                    30000.0,
                    None,
                    "greater",
                    now - 10,
                    now - 5,
                    1,
                    now,
                ),
            )
            await conn.commit()

            dao = Dao(conn)
            row = {
                "ts_eval": now,
                "market_id": "KXBTC-NOSCORE",
                "settlement_ts": now - 10,
                "strike": None,
                "spot_price": 30000.0,
                "sigma": 0.5,
                "tau": 1.0,
                "p_model": 0.6,
                "p_market": 0.55,
                "best_yes_bid": 45.0,
                "best_yes_ask": 30.0,
                "best_no_bid": 70.0,
                "best_no_ask": 70.0,
                "spread": 25.0,
                "eligible": 1,
                "reason_not_eligible": None,
                "would_trade": 1,
                "side": "YES",
                "ev_raw": 0.3,
                "ev_net": 0.3,
                "cost_buffer": None,
                "raw_json": "{\"price_used_cents\": 30}",
            }
            await dao.insert_opportunities([row])
            await conn.commit()

            report = await module.compute_report(conn, since_ts=now - 60)
            assert report["total"] == 1
            assert report["settled"] == 1
            assert report["unsettled"] == 0
            assert report["snapshot_score_matches_total"] == 0
            assert report["snapshot_score_matches_settled"] == 0
            assert report["snapshot_score_coverage_total"] == pytest.approx(0.0)
            assert report["snapshot_score_coverage_settled"] == pytest.approx(0.0)
            assert report["avg_model_brier"] == pytest.approx(0.16)
            assert report["avg_model_logloss"] == pytest.approx(0.5108256238)

    asyncio.run(_run())


def test_report_model_performance_counts_no_side_win(tmp_path):
    db_path = tmp_path / "report_no_side_win.sqlite"
    module = _load_report_module()

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status, raw_json) "
                "VALUES (?, ?, ?, ?)",
                ("KXBTC-NOWIN", now, "active", "{}"),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, settled_ts, outcome, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "KXBTC-NOWIN",
                    30000.0,
                    None,
                    "greater",
                    now - 10,
                    now - 5,
                    0,
                    now,
                ),
            )
            await conn.commit()

            dao = Dao(conn)
            row = {
                "ts_eval": now,
                "market_id": "KXBTC-NOWIN",
                "settlement_ts": now - 10,
                "strike": None,
                "spot_price": 30000.0,
                "sigma": 0.5,
                "tau": 1.0,
                "p_model": 0.2,
                "p_market": 0.25,
                "best_yes_bid": 20.0,
                "best_yes_ask": 25.0,
                "best_no_bid": 75.0,
                "best_no_ask": 80.0,
                "spread": 5.0,
                "eligible": 1,
                "reason_not_eligible": None,
                "would_trade": 1,
                "side": "NO",
                "ev_raw": 0.1,
                "ev_net": 0.1,
                "cost_buffer": None,
                "raw_json": "{\"price_used_cents\": 80}",
            }
            await dao.insert_opportunities([row])
            await conn.commit()

            report = await module.compute_report(conn, since_ts=now - 60)
            assert report["settled"] == 1
            # NO trade with outcome=0 should count as a win.
            by_day = next(iter(report["by_day"].values()))
            assert by_day["wins"] == 1
            bucket = report["buckets"]["0.8-0.9"]
            assert bucket["wins"] == 1

    asyncio.run(_run())
