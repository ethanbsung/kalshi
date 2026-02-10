import asyncio
import math
import time

import importlib.util
from pathlib import Path

import aiosqlite

from kalshi_bot.data import init_db
from kalshi_bot.data.dao import Dao
from kalshi_bot.models.probability import EPS
from kalshi_bot.strategy.edge_snapshot_scoring import score_snapshot


def test_score_snapshot_pnl_yes():
    snapshot = {"prob_yes": 0.5, "yes_ask": 30.0, "no_ask": 70.0}
    score = score_snapshot(snapshot, outcome=1)
    assert score["pnl_take_yes"] == 1.0 - 0.30 - 0.02
    score = score_snapshot(snapshot, outcome=0)
    assert score["pnl_take_yes"] == -0.30 - 0.02


def test_score_snapshot_logloss_clamped():
    snapshot = {"prob_yes": 1.0, "yes_ask": 30.0, "no_ask": 70.0}
    score = score_snapshot(snapshot, outcome=0)
    assert score["logloss"] is not None
    prob_clamped = max(EPS, min(1.0 - EPS, snapshot["prob_yes"]))
    expected = -math.log(1.0 - prob_clamped)
    assert abs(score["logloss"] - expected) < 1e-9


def test_score_snapshot_missing_asks_and_prob():
    snapshot = {"prob_yes": None, "yes_ask": None, "no_ask": None}
    score = score_snapshot(snapshot, outcome=1)
    assert score["brier"] is None
    assert score["logloss"] is None
    assert score["pnl_take_yes"] is None
    assert score["pnl_take_no"] is None
    assert score["error"] == "missing_no_ask,missing_prob_yes,missing_yes_ask"


def test_score_snapshot_missing_yes_ask():
    snapshot = {"prob_yes": 0.5, "yes_ask": None, "no_ask": 70.0}
    score = score_snapshot(snapshot, outcome=1)
    assert score["pnl_take_yes"] is None
    assert score["pnl_take_no"] is not None
    assert score["error"] == "missing_yes_ask"


def test_score_snapshot_missing_no_ask():
    snapshot = {"prob_yes": 0.5, "yes_ask": 30.0, "no_ask": None}
    score = score_snapshot(snapshot, outcome=1)
    assert score["pnl_take_no"] is None
    assert score["pnl_take_yes"] is not None
    assert score["error"] == "missing_no_ask"


def test_score_snapshot_integration(tmp_path):
    db_path = tmp_path / "scores.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status, raw_json) "
                "VALUES (?, ?, ?, ?)",
                ("KXBTC-SCORE", now, "active", "{}"),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, settled_ts, outcome, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "KXBTC-SCORE",
                    30000.0,
                    None,
                    "greater",
                    now - 60,
                    now - 10,
                    1,
                    now,
                ),
            )
            await conn.execute(
                "INSERT INTO kalshi_edge_snapshots (asof_ts, market_id, settlement_ts, spot_ts, spot_price, sigma_annualized, "
                "prob_yes, prob_yes_raw, horizon_seconds, quote_ts, yes_bid, yes_ask, no_bid, no_ask, "
                "yes_mid, no_mid, ev_take_yes, ev_take_no, spot_age_seconds, quote_age_seconds, skip_reason, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    now,
                    "KXBTC-SCORE",
                    now - 60,
                    now - 60,
                    30000.0,
                    0.5,
                    0.6,
                    0.6001,
                    3660,
                    now - 10,
                    45.0,
                    55.0,
                    40.0,
                    60.0,
                    50.0,
                    50.0,
                    0.01,
                    -0.02,
                    60,
                    10,
                    None,
                    "{}",
                ),
            )
            await conn.commit()

            dao = Dao(conn)
            snapshots = await dao.get_unscored_edge_snapshots(limit=10, now_ts=now)
            assert len(snapshots) == 1
            snapshot = snapshots[0]
            score = score_snapshot(snapshot, outcome=snapshot["outcome"])
            row = {
                "asof_ts": snapshot["asof_ts"],
                "market_id": snapshot["market_id"],
                "settled_ts": snapshot["settled_ts"],
                "outcome": snapshot["outcome"],
                "pnl_take_yes": score["pnl_take_yes"],
                "pnl_take_no": score["pnl_take_no"],
                "brier": score["brier"],
                "logloss": score["logloss"],
                "error": score["error"],
                "created_ts": now,
            }
            await dao.insert_kalshi_edge_snapshot_score(row)
            await conn.commit()

            cursor = await conn.execute(
                "SELECT market_id, outcome FROM kalshi_edge_snapshot_scores"
            )
            row = await cursor.fetchone()
            assert row == ("KXBTC-SCORE", 1)

    asyncio.run(_run())


def test_score_insert_with_foreign_keys_on_fresh_db(tmp_path):
    db_path = tmp_path / "fk_scores.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status, raw_json) "
                "VALUES (?, ?, ?, ?)",
                ("KXBTC-FK", now, "active", "{}"),
            )
            await conn.execute(
                "INSERT INTO kalshi_edge_snapshots (asof_ts, market_id, settlement_ts, spot_ts, spot_price, sigma_annualized, "
                "prob_yes, prob_yes_raw, horizon_seconds, quote_ts, yes_bid, yes_ask, no_bid, no_ask, "
                "yes_mid, no_mid, ev_take_yes, ev_take_no, spot_age_seconds, quote_age_seconds, skip_reason, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    now,
                    "KXBTC-FK",
                    now + 300,
                    now,
                    30000.0,
                    0.5,
                    0.55,
                    0.5501,
                    300,
                    now,
                    45.0,
                    55.0,
                    45.0,
                    55.0,
                    50.0,
                    50.0,
                    0.0,
                    0.0,
                    0,
                    0,
                    None,
                    "{}",
                ),
            )
            await conn.commit()

            dao = Dao(conn)
            await dao.insert_kalshi_edge_snapshot_score(
                {
                    "asof_ts": now,
                    "market_id": "KXBTC-FK",
                    "settled_ts": now + 301,
                    "outcome": 1,
                    "pnl_take_yes": 0.45,
                    "pnl_take_no": -0.55,
                    "brier": 0.2025,
                    "logloss": 0.5978,
                    "error": None,
                    "created_ts": now + 302,
                }
            )
            await conn.commit()

            row = await (
                await conn.execute(
                    "SELECT market_id, asof_ts FROM kalshi_edge_snapshot_scores"
                )
            ).fetchone()
            assert row == ("KXBTC-FK", now)

    asyncio.run(_run())


def test_get_unscored_excludes_scored(tmp_path):
    db_path = tmp_path / "unscored.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status, raw_json) "
                "VALUES (?, ?, ?, ?)",
                ("KXBTC-SCORED", now, "active", "{}"),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, settled_ts, outcome, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "KXBTC-SCORED",
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
                "INSERT INTO kalshi_edge_snapshots (asof_ts, market_id, settlement_ts, spot_ts, spot_price, sigma_annualized, "
                "prob_yes, prob_yes_raw, horizon_seconds, quote_ts, yes_bid, yes_ask, no_bid, no_ask, "
                "yes_mid, no_mid, ev_take_yes, ev_take_no, spot_age_seconds, quote_age_seconds, skip_reason, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    now,
                    "KXBTC-SCORED",
                    now - 10,
                    now - 60,
                    30000.0,
                    0.5,
                    0.6,
                    0.6001,
                    50,
                    now - 10,
                    45.0,
                    55.0,
                    40.0,
                    60.0,
                    50.0,
                    50.0,
                    0.01,
                    -0.02,
                    60,
                    10,
                    None,
                    "{}",
                ),
            )
            await conn.execute(
                "INSERT INTO kalshi_edge_snapshot_scores (asof_ts, market_id, settled_ts, outcome, pnl_take_yes, pnl_take_no, "
                "brier, logloss, error, created_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    now,
                    "KXBTC-SCORED",
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
            snapshots = await dao.get_unscored_edge_snapshots(
                limit=10, now_ts=now
            )
            assert snapshots == []

    asyncio.run(_run())


def test_process_snapshots_counters():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "score_edge_snapshots.py"
    spec = importlib.util.spec_from_file_location("score_edge_snapshots", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    process_snapshots = module.process_snapshots
    now = int(time.time())
    snapshots = [
        {
            "asof_ts": now,
            "market_id": "KXBTC-OK",
            "outcome": 1,
            "prob_yes": 0.6,
            "yes_ask": 30.0,
            "no_ask": 70.0,
            "settled_ts": now,
        },
        {
            "asof_ts": now,
            "market_id": "KXBTC-MISS-YES",
            "outcome": 1,
            "prob_yes": 0.6,
            "yes_ask": None,
            "no_ask": 70.0,
            "settled_ts": now,
        },
        {
            "asof_ts": now,
            "market_id": "KXBTC-MISS-NO",
            "outcome": 1,
            "prob_yes": 0.6,
            "yes_ask": 30.0,
            "no_ask": None,
            "settled_ts": now,
        },
        {
            "asof_ts": now,
            "market_id": "KXBTC-MISS-PROB",
            "outcome": 1,
            "prob_yes": None,
            "yes_ask": 30.0,
            "no_ask": 70.0,
            "settled_ts": now,
        },
    ]
    rows, counters = process_snapshots(snapshots, now_ts=now)
    assert counters["processed_total"] == 4
    assert counters["inserted_total"] == 4
    assert counters["missing_yes_ask"] == 1
    assert counters["missing_no_ask"] == 1
    assert counters["missing_prob"] == 1
    assert counters["errors_total"] == 3
    assert counters["scored_prob"] == 3
    assert counters["scored_pnl_yes"] == 3
    assert counters["scored_pnl_no"] == 3
    assert len(rows) == 4
