from __future__ import annotations

import argparse
import asyncio
import sqlite3
import time
from typing import Any

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.data import init_db
from kalshi_bot.data.dao import Dao
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.strategy.opportunity_engine import (
    OpportunityConfig,
    build_opportunities_from_snapshots,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continuously build opportunity ledger entries."
    )
    parser.add_argument("--interval-seconds", type=int, default=10)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--min-ev", type=float, default=0.01)
    parser.add_argument("--top-n", type=int, default=None)
    parser.add_argument("--emit-passes", action="store_true")
    parser.add_argument("--both-sides", action="store_true")
    parser.add_argument("--min-ask-cents", type=float, default=1.0)
    parser.add_argument("--max-ask-cents", type=float, default=99.0)
    parser.add_argument("--max-spot-age", type=int, default=None)
    parser.add_argument("--max-quote-age", type=int, default=None)
    parser.add_argument("--paper", action="store_true", default=True)
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


async def _load_latest_asof_ts(conn: aiosqlite.Connection) -> int | None:
    cursor = await conn.execute("SELECT MAX(asof_ts) FROM kalshi_edge_snapshots")
    row = await cursor.fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


async def _load_snapshots(
    conn: aiosqlite.Connection, asof_ts: int
) -> list[dict[str, Any]]:
    cursor = await conn.execute(
        "SELECT asof_ts, market_id, settlement_ts, spot_ts, spot_price, "
        "sigma_annualized, prob_yes, prob_yes_raw, horizon_seconds, quote_ts, "
        "yes_bid, yes_ask, no_bid, no_ask, yes_mid, no_mid, ev_take_yes, "
        "ev_take_no, spot_age_seconds, quote_age_seconds, skip_reason, raw_json "
        "FROM kalshi_edge_snapshots WHERE asof_ts = ?",
        (asof_ts,),
    )
    rows = await cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    snapshots = [dict(zip(columns, row)) for row in rows]
    return snapshots


async def run_tick(conn: aiosqlite.Connection, args: argparse.Namespace) -> dict[str, Any]:
    asof_ts = await _load_latest_asof_ts(conn)
    if asof_ts is None:
        return {"error": "no_edge_snapshots"}
    snapshots = await _load_snapshots(conn, asof_ts)
    config = OpportunityConfig(
        min_ev=args.min_ev,
        min_ask_cents=args.min_ask_cents,
        max_ask_cents=args.max_ask_cents,
        max_spot_age=args.max_spot_age,
        max_quote_age=args.max_quote_age,
        top_n=args.top_n,
        emit_passes=args.emit_passes,
        best_side_only=not args.both_sides,
    )
    rows, counters = build_opportunities_from_snapshots(snapshots, config)

    if args.live:
        return {"error": "live_trading_not_supported"}

    inserted = 0
    if rows:
        dao = Dao(conn)
        before = conn.total_changes
        await dao.insert_opportunities(rows)
        inserted = conn.total_changes - before
        await conn.commit()

    summary = {
        "asof_ts": asof_ts,
        "snapshots": len(snapshots),
        "takes": counters["takes"],
        "passes": counters["passes"],
        "inserted": inserted,
        "counters": counters,
    }
    return summary


async def _run_loop() -> int:
    args = _parse_args()
    settings = load_settings()
    logger = setup_logger(settings.log_path)

    print(f"DB path: {settings.db_path}")
    await init_db(settings.db_path)

    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 5000;")
        await conn.commit()

        backoff = [0.2, 0.5, 1.0]
        while True:
            try:
                summary = await run_tick(conn, args)
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower():
                    raise
                for delay in backoff:
                    await asyncio.sleep(delay)
                    try:
                        summary = await run_tick(conn, args)
                        break
                    except sqlite3.OperationalError as retry_exc:
                        if "locked" not in str(retry_exc).lower():
                            raise
                else:
                    summary = {"error": "db_locked"}

            if "error" in summary:
                logger.warning(
                    "opportunity_tick_error",
                    extra={"error": summary["error"]},
                )
                print(f"tick_error={summary['error']}")
            else:
                print(
                    "opportunity_tick ts={asof_ts} snapshots={snapshots} "
                    "takes={takes} passes={passes} inserted={inserted}".format(
                        **summary
                    )
                )
                if args.debug:
                    print(f"counters={summary['counters']}")

            if args.once:
                break
            await asyncio.sleep(args.interval_seconds)

    return 0


def main() -> int:
    return asyncio.run(_run_loop())


if __name__ == "__main__":
    raise SystemExit(main())
