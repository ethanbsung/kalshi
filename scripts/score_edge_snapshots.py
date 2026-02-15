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
from kalshi_bot.persistence import PostgresEventRepository
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
    parser = argparse.ArgumentParser(description="Score settled edge snapshots.")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--pg-dsn",
        type=str,
        default=None,
        help="Use Postgres source/sink when provided (defaults to PG_DSN).",
    )
    return parser.parse_args()


def _print_counters(counters: dict[str, int]) -> None:
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
        "scored_pnl_no={scored_pnl_no}".format(**counters)
    )


def _load_unscored_edge_snapshots_postgres(
    conn: Any, *, limit: int
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                (e.payload_json->>'asof_ts')::BIGINT AS asof_ts,
                e.payload_json->>'market_id' AS market_id,
                COALESCE(c.settled_ts, c.expiration_ts, c.close_ts) AS settled_ts,
                c.outcome AS outcome,
                NULLIF(e.payload_json->>'prob_yes', '')::DOUBLE PRECISION AS prob_yes,
                NULLIF(e.payload_json->>'yes_ask', '')::DOUBLE PRECISION AS yes_ask,
                NULLIF(e.payload_json->>'no_ask', '')::DOUBLE PRECISION AS no_ask
            FROM event_store.events_raw e
            JOIN event_store.state_contract_latest c
              ON c.ticker = e.payload_json->>'market_id'
            LEFT JOIN event_store.fact_edge_snapshot_scores s
              ON s.market_id = e.payload_json->>'market_id'
             AND s.asof_ts = (e.payload_json->>'asof_ts')::BIGINT
            WHERE e.event_type = 'edge_snapshot'
              AND c.outcome IS NOT NULL
              AND s.market_id IS NULL
            ORDER BY (e.payload_json->>'asof_ts')::BIGINT ASC
            LIMIT %s
            """,
            (max(int(limit), 0),),
        )
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in rows]


def _insert_scores_postgres(conn: Any, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO event_store.fact_edge_snapshot_scores (
                asof_ts,
                market_id,
                settled_ts,
                outcome,
                pnl_take_yes,
                pnl_take_no,
                brier,
                logloss,
                error,
                created_ts
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (asof_ts, market_id) DO UPDATE SET
                settled_ts = EXCLUDED.settled_ts,
                outcome = EXCLUDED.outcome,
                pnl_take_yes = EXCLUDED.pnl_take_yes,
                pnl_take_no = EXCLUDED.pnl_take_no,
                brier = EXCLUDED.brier,
                logloss = EXCLUDED.logloss,
                error = EXCLUDED.error,
                created_ts = EXCLUDED.created_ts,
                updated_at = NOW()
            """,
            [
                (
                    row["asof_ts"],
                    row["market_id"],
                    row.get("settled_ts"),
                    row["outcome"],
                    row.get("pnl_take_yes"),
                    row.get("pnl_take_no"),
                    row.get("brier"),
                    row.get("logloss"),
                    row.get("error"),
                    row["created_ts"],
                )
                for row in rows
            ],
        )
    conn.commit()


async def _run_sqlite(*, args: argparse.Namespace, settings: Any) -> int:
    print(f"backend=sqlite db_path={settings.db_path}")
    await init_db(settings.db_path)
    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 15000;")
        await conn.commit()

        dao = Dao(conn)
        now_ts = int(time.time())
        snapshots = await dao.get_unscored_edge_snapshots(args.limit, now_ts)
        rows, counters = process_snapshots(snapshots, now_ts)
        if args.dry_run:
            _print_counters(counters)
            if args.verbose and rows:
                print(f"sample_score={rows[0]}")
            return 0
        if rows:
            await dao.insert_kalshi_edge_snapshot_scores(rows)
            await conn.commit()
        _print_counters(counters)
        if args.verbose and rows:
            print(f"sample_score={rows[0]}")
    return 0


async def _run_postgres(*, args: argparse.Namespace, pg_dsn: str) -> int:
    print("backend=postgres")
    repo = PostgresEventRepository(pg_dsn)
    repo.ensure_schema()
    repo.close()

    try:
        import psycopg
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg is required for Postgres scoring. "
            "Install with: pip install psycopg[binary]"
        ) from exc

    now_ts = int(time.time())
    with psycopg.connect(pg_dsn) as conn:
        snapshots = _load_unscored_edge_snapshots_postgres(conn, limit=args.limit)
        rows, counters = process_snapshots(snapshots, now_ts)
        if args.dry_run:
            _print_counters(counters)
            if args.verbose and rows:
                print(f"sample_score={rows[0]}")
            return 0
        _insert_scores_postgres(conn, rows)

    _print_counters(counters)
    if args.verbose and rows:
        print(f"sample_score={rows[0]}")
    return 0


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()
    setup_logger(settings.log_path)
    pg_dsn = args.pg_dsn or settings.pg_dsn
    if pg_dsn:
        return await _run_postgres(args=args, pg_dsn=pg_dsn)
    return await _run_sqlite(args=args, settings=settings)


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
