from __future__ import annotations

import argparse
import asyncio
import time
from typing import Any

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.data import init_db
from kalshi_bot.data.dao import Dao
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.strategy.edge_snapshot_scoring import score_snapshot


def process_snapshots(
    snapshots: list[dict[str, Any]], now_ts: int
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, Any]] = []
    counters = {
        "processed_total": 0,
        "inserted_total": 0,
        "missing_outcome": 0,
        "missing_prob": 0,
        "missing_yes_ask": 0,
        "missing_no_ask": 0,
        "errors_total": 0,
        "scored_prob": 0,
        "scored_pnl_yes": 0,
        "scored_pnl_no": 0,
    }
    for snapshot in snapshots:
        counters["processed_total"] += 1
        outcome = snapshot.get("outcome")
        if outcome not in (0, 1):
            counters["missing_outcome"] += 1
            continue
        score = score_snapshot(snapshot, outcome)
        error = score.get("error")
        tokens = error.split(",") if error else []
        if tokens:
            counters["errors_total"] += 1
        if "missing_prob_yes" in tokens:
            counters["missing_prob"] += 1
        if "missing_yes_ask" in tokens:
            counters["missing_yes_ask"] += 1
        if "missing_no_ask" in tokens:
            counters["missing_no_ask"] += 1
        if score.get("brier") is not None and score.get("logloss") is not None:
            counters["scored_prob"] += 1
        if score.get("pnl_take_yes") is not None:
            counters["scored_pnl_yes"] += 1
        if score.get("pnl_take_no") is not None:
            counters["scored_pnl_no"] += 1

        rows.append(
            {
                "asof_ts": snapshot["asof_ts"],
                "market_id": snapshot["market_id"],
                "settled_ts": snapshot.get("settled_ts"),
                "outcome": outcome,
                "pnl_take_yes": score["pnl_take_yes"],
                "pnl_take_no": score["pnl_take_no"],
                "brier": score["brier"],
                "logloss": score["logloss"],
                "error": error,
                "created_ts": now_ts,
            }
        )
        counters["inserted_total"] += 1
    return rows, counters


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Score settled edge snapshots."
    )
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()
    setup_logger(settings.log_path)

    print(f"DB path: {settings.db_path}")
    await init_db(settings.db_path)

    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 5000;")
        await conn.commit()

        dao = Dao(conn)
        now_ts = int(time.time())
        snapshots = await dao.get_unscored_edge_snapshots(
            args.limit, now_ts
        )
        rows, counters = process_snapshots(snapshots, now_ts)

        if args.dry_run:
            print(
                "dry_run_processed_total={processed_total} "
                "inserted_total={inserted_total} "
                "missing_outcome={missing_outcome} "
                "missing_prob={missing_prob} "
                "missing_yes_ask={missing_yes_ask} "
                "missing_no_ask={missing_no_ask} "
                "errors_total={errors_total} "
                "scored_prob={scored_prob} "
                "scored_pnl_yes={scored_pnl_yes} "
                "scored_pnl_no={scored_pnl_no}".format(
                    **counters
                )
            )
            if args.verbose and rows:
                print(f"sample_score={rows[0]}")
            return 0

        if rows:
            await dao.insert_kalshi_edge_snapshot_scores(rows)
            await conn.commit()

        print(
            "processed_total={processed_total} "
            "inserted_total={inserted_total} "
            "missing_outcome={missing_outcome} "
            "missing_prob={missing_prob} "
            "missing_yes_ask={missing_yes_ask} "
            "missing_no_ask={missing_no_ask} "
            "errors_total={errors_total} "
            "scored_prob={scored_prob} "
            "scored_pnl_yes={scored_pnl_yes} "
            "scored_pnl_no={scored_pnl_no}".format(
                **counters
            )
        )
        if args.verbose and rows:
            print(f"sample_score={rows[0]}")

    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
