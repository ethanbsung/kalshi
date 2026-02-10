import asyncio

import aiosqlite

from kalshi_bot.app.live_stack_health import collect_live_health, format_live_health
from kalshi_bot.data import init_db


def test_collect_live_health_empty(tmp_path):
    db_path = tmp_path / "health_empty.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        async with aiosqlite.connect(db_path) as conn:
            summary = await collect_live_health(
                conn,
                now_ts=1_700_000_000,
                product_id="BTC-USD",
                window_minutes=10,
            )
        assert summary["spot_tick_age_seconds"] is None
        assert summary["quote_age_seconds"] is None
        assert summary["snapshot_age_seconds"] is None
        assert summary["snapshots_last_window"] == 0
        assert summary["opportunities_last_window"] == 0
        assert summary["scores_last_window"] == 0
        line = format_live_health(summary)
        assert "spot_age_s=NA" in line
        assert "quote_age_s=NA" in line
        assert "snapshot_age_s=NA" in line

    asyncio.run(_run())


def test_collect_live_health_counts_and_ages(tmp_path):
    db_path = tmp_path / "health_counts.sqlite"
    now_ts = 1_700_000_000

    async def _run() -> None:
        await init_db(db_path)
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute(
                "INSERT INTO kalshi_markets (market_id, ts_loaded, status, raw_json) VALUES (?, ?, ?, ?)",
                ("KXBTC-H", now_ts - 1000, "active", "{}"),
            )
            await conn.execute(
                "INSERT INTO spot_ticks (ts, product_id, price, best_bid, best_ask, bid_qty, ask_qty, sequence_num, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (now_ts - 5, "BTC-USD", 30000.0, 29999.0, 30001.0, 1.0, 1.0, 1, "{}"),
            )
            await conn.execute(
                "INSERT INTO kalshi_quotes (ts, market_id, yes_bid, yes_ask, no_bid, no_ask, yes_mid, no_mid, p_mid, volume, volume_24h, open_interest, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (now_ts - 11, "KXBTC-H", 49.0, 51.0, 49.0, 51.0, 50.0, 50.0, 0.5, 1, 1, 1, "{}"),
            )
            await conn.execute(
                "INSERT INTO kalshi_edge_snapshots (asof_ts, market_id, settlement_ts, spot_ts, spot_price, sigma_annualized, prob_yes, prob_yes_raw, horizon_seconds, quote_ts, yes_bid, yes_ask, no_bid, no_ask, yes_mid, no_mid, ev_take_yes, ev_take_no, spot_age_seconds, quote_age_seconds, skip_reason, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    now_ts - 17,
                    "KXBTC-H",
                    now_ts + 300,
                    now_ts - 18,
                    30000.0,
                    0.5,
                    0.55,
                    0.5501,
                    317,
                    now_ts - 11,
                    49.0,
                    51.0,
                    49.0,
                    51.0,
                    50.0,
                    50.0,
                    0.0,
                    0.0,
                    1,
                    6,
                    None,
                    "{}",
                ),
            )
            await conn.execute(
                "INSERT INTO kalshi_edge_snapshot_scores (asof_ts, market_id, settled_ts, outcome, pnl_take_yes, pnl_take_no, brier, logloss, error, created_ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    now_ts - 17,
                    "KXBTC-H",
                    now_ts - 1,
                    1,
                    0.49,
                    -0.51,
                    0.2,
                    0.5,
                    None,
                    now_ts - 2,
                ),
            )
            await conn.execute(
                "INSERT INTO opportunities (ts_eval, market_id, settlement_ts, strike, spot_price, sigma, tau, p_model, p_market, best_yes_bid, best_yes_ask, best_no_bid, best_no_ask, spread, eligible, reason_not_eligible, would_trade, side, ev_raw, ev_net, cost_buffer, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    now_ts - 20,
                    "KXBTC-H",
                    now_ts + 300,
                    None,
                    30000.0,
                    0.5,
                    5.0,
                    0.55,
                    0.5,
                    49.0,
                    51.0,
                    49.0,
                    51.0,
                    2.0,
                    1,
                    None,
                    1,
                    "YES",
                    0.04,
                    0.04,
                    None,
                    "{}",
                ),
            )
            await conn.commit()

            summary = await collect_live_health(
                conn,
                now_ts=now_ts,
                product_id="BTC-USD",
                window_minutes=10,
            )

        assert summary["spot_tick_age_seconds"] == 5
        assert summary["quote_age_seconds"] == 11
        assert summary["snapshot_age_seconds"] == 17
        assert summary["snapshots_last_window"] == 1
        assert summary["opportunities_last_window"] == 1
        assert summary["scores_last_window"] == 1

        line = format_live_health(summary)
        assert "spot_age_s=5" in line
        assert "quote_age_s=11" in line
        assert "snapshot_age_s=17" in line
        assert "snapshots=1" in line
        assert "opportunities=1" in line
        assert "scores=1" in line

    asyncio.run(_run())
