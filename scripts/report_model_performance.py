from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from datetime import datetime, timezone
from typing import Any

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.data import init_db
from kalshi_bot.kalshi.fees import taker_fee_dollars
from kalshi_bot.models.probability import EPS
from kalshi_bot.persistence import PostgresEventRepository


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report model performance on settled opportunities."
    )
    parser.add_argument("--since-seconds", type=int, default=7 * 24 * 3600)
    parser.add_argument(
        "--pg-dsn",
        type=str,
        default=None,
        help="Use Postgres source when provided (defaults to PG_DSN).",
    )
    return parser.parse_args()


def _bucket(prob: float) -> str:
    idx = min(9, max(0, int(prob * 10)))
    lo = idx / 10.0
    hi = lo + 0.1
    return f"{lo:.1f}-{hi:.1f}"


def _parse_price_used(raw_json: str | None) -> float | None:
    if not raw_json:
        return None
    try:
        data = json.loads(raw_json)
    except (TypeError, ValueError):
        return None
    price = data.get("price_used_cents")
    try:
        if price is None:
            return None
        return float(price)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _realized_pnl(outcome: int, side: str, price_cents: float) -> float:
    fee = taker_fee_dollars(price_cents, 1)
    fee_value = fee if fee is not None else 0.0
    price = price_cents / 100.0
    if side == "YES":
        return outcome - price - fee_value
    return (1 - outcome) - price - fee_value


def _score_prob(prob: float | None, outcome: int) -> tuple[float | None, float | None]:
    if prob is None or outcome not in (0, 1):
        return None, None
    p = max(EPS, min(1.0 - EPS, float(prob)))
    brier = (p - outcome) ** 2
    logloss = -(outcome * math.log(p) + (1 - outcome) * math.log(1.0 - p))
    return brier, logloss


def _compute_report_from_rows(rows: list[tuple[Any, ...]]) -> dict[str, Any]:
    total = len(rows)
    settled = 0
    pnl_total = 0.0
    model_brier_total = 0.0
    model_logloss_total = 0.0
    model_score_count = 0
    market_brier_total = 0.0
    market_logloss_total = 0.0
    market_score_count = 0
    snapshot_score_matches_total = 0
    snapshot_score_matches_settled = 0

    by_day: dict[str, dict[str, float]] = {}
    buckets: dict[str, dict[str, float]] = {}

    for (
        ts_eval,
        _market_id,
        side,
        p_model,
        p_market,
        yes_ask,
        no_ask,
        raw_json,
        outcome,
        settled_ts,
        has_snapshot_score,
    ) in rows:
        if int(has_snapshot_score or 0) == 1:
            snapshot_score_matches_total += 1
        if outcome is None:
            continue
        settled += 1
        if int(has_snapshot_score or 0) == 1:
            snapshot_score_matches_settled += 1
        try:
            outcome_val = int(outcome)
        except (TypeError, ValueError):
            continue
        trade_win: int | None = None
        if side == "YES":
            trade_win = outcome_val
        elif side == "NO":
            trade_win = 1 - outcome_val
        model_brier, model_logloss = _score_prob(_safe_float(p_model), outcome_val)
        if model_brier is not None and model_logloss is not None:
            model_brier_total += model_brier
            model_logloss_total += model_logloss
            model_score_count += 1

        market_prob = _safe_float(p_market)
        if market_prob is None:
            if side == "YES":
                yes_price = (
                    _safe_float(yes_ask)
                    if yes_ask is not None
                    else _safe_float(_parse_price_used(raw_json))
                )
                if yes_price is not None:
                    market_prob = yes_price / 100.0
            elif side == "NO":
                no_price = (
                    _safe_float(no_ask)
                    if no_ask is not None
                    else _safe_float(_parse_price_used(raw_json))
                )
                if no_price is not None:
                    market_prob = 1.0 - (no_price / 100.0)
        market_brier, market_logloss = _score_prob(market_prob, outcome_val)
        if market_brier is not None and market_logloss is not None:
            market_brier_total += market_brier
            market_logloss_total += market_logloss
            market_score_count += 1

        price_used = _parse_price_used(raw_json)
        if price_used is None:
            if side == "YES" and yes_ask is not None:
                price_used = float(yes_ask)
            elif side == "NO" and no_ask is not None:
                price_used = float(no_ask)
        if price_used is not None and side in {"YES", "NO"}:
            pnl_total += _realized_pnl(outcome_val, side, price_used)

        ts = settled_ts if settled_ts is not None else ts_eval
        day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        if p_model is not None:
            try:
                model_prob_yes = float(p_model)
                prob_val = model_prob_yes if side == "YES" else 1.0 - model_prob_yes
                bucket = _bucket(prob_val)
                info = buckets.setdefault(bucket, {"count": 0, "prob_sum": 0.0, "wins": 0})
                info["count"] += 1
                info["prob_sum"] += prob_val
                info["wins"] += int(trade_win or 0)
            except (TypeError, ValueError):
                pass

        day_info = by_day.setdefault(day, {"count": 0, "pnl": 0.0, "wins": 0})
        day_info["count"] += 1
        day_info["pnl"] += _realized_pnl(outcome_val, side, price_used or 0.0)
        day_info["wins"] += int(trade_win or 0)

    unsettled = total - settled
    avg_model_brier = model_brier_total / model_score_count if model_score_count else None
    avg_model_logloss = (
        model_logloss_total / model_score_count if model_score_count else None
    )
    avg_market_brier = (
        market_brier_total / market_score_count if market_score_count else None
    )
    avg_market_logloss = (
        market_logloss_total / market_score_count if market_score_count else None
    )
    delta_brier = (
        avg_model_brier - avg_market_brier
        if avg_model_brier is not None and avg_market_brier is not None
        else None
    )
    delta_logloss = (
        avg_model_logloss - avg_market_logloss
        if avg_model_logloss is not None and avg_market_logloss is not None
        else None
    )
    snapshot_score_coverage_total = (
        snapshot_score_matches_total / total if total > 0 else None
    )
    snapshot_score_coverage_settled = (
        snapshot_score_matches_settled / settled if settled > 0 else None
    )

    return {
        "total": total,
        "settled": settled,
        "unsettled": unsettled,
        "avg_model_brier": avg_model_brier,
        "avg_model_logloss": avg_model_logloss,
        "avg_market_brier": avg_market_brier,
        "avg_market_logloss": avg_market_logloss,
        "delta_brier": delta_brier,
        "delta_logloss": delta_logloss,
        "model_scored": model_score_count,
        "market_scored": market_score_count,
        "snapshot_score_matches_total": snapshot_score_matches_total,
        "snapshot_score_matches_settled": snapshot_score_matches_settled,
        "snapshot_score_coverage_total": snapshot_score_coverage_total,
        "snapshot_score_coverage_settled": snapshot_score_coverage_settled,
        # Backward compatible aliases.
        "avg_brier": avg_model_brier,
        "avg_logloss": avg_model_logloss,
        "pnl_total": pnl_total,
        "by_day": by_day,
        "buckets": buckets,
    }


async def compute_report(conn: aiosqlite.Connection, since_ts: int) -> dict[str, Any]:
    cursor = await conn.execute(
        """
        SELECT o.ts_eval, o.market_id, o.side, o.p_model, o.p_market,
               o.best_yes_ask, o.best_no_ask, o.raw_json,
               c.outcome AS outcome,
               COALESCE(c.settled_ts, o.settlement_ts) AS settled_ts,
               CASE WHEN s.market_id IS NULL THEN 0 ELSE 1 END AS has_snapshot_score
        FROM opportunities o
        LEFT JOIN kalshi_contracts c
          ON c.ticker = o.market_id
        LEFT JOIN kalshi_edge_snapshot_scores s
          ON s.market_id = o.market_id AND s.asof_ts = o.ts_eval
        WHERE o.would_trade = 1 AND o.ts_eval >= ?
        ORDER BY o.ts_eval ASC
        """,
        (since_ts,),
    )
    rows = await cursor.fetchall()
    return _compute_report_from_rows([tuple(row) for row in rows])


def compute_report_postgres(conn: Any, since_ts: int) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                (o.payload_json->>'ts_eval')::BIGINT AS ts_eval,
                o.payload_json->>'market_id' AS market_id,
                o.payload_json->>'side' AS side,
                NULLIF(o.payload_json->>'p_model', '')::DOUBLE PRECISION AS p_model,
                NULLIF(o.payload_json->>'p_market', '')::DOUBLE PRECISION AS p_market,
                NULLIF(o.payload_json->>'best_yes_ask', '')::DOUBLE PRECISION AS best_yes_ask,
                NULLIF(o.payload_json->>'best_no_ask', '')::DOUBLE PRECISION AS best_no_ask,
                o.payload_json->>'raw_json' AS raw_json,
                c.outcome AS outcome,
                COALESCE(c.settled_ts, c.expiration_ts, c.close_ts) AS settled_ts,
                CASE WHEN s.market_id IS NULL THEN 0 ELSE 1 END AS has_snapshot_score
            FROM event_store.events_raw o
            LEFT JOIN event_store.state_contract_latest c
              ON c.ticker = o.payload_json->>'market_id'
            LEFT JOIN event_store.fact_edge_snapshot_scores s
              ON s.market_id = o.payload_json->>'market_id'
             AND s.asof_ts = (o.payload_json->>'ts_eval')::BIGINT
            WHERE o.event_type = 'opportunity_decision'
              AND (o.payload_json->>'ts_eval')::BIGINT >= %s
              AND COALESCE((o.payload_json->>'would_trade')::BOOLEAN, FALSE) = TRUE
            ORDER BY (o.payload_json->>'ts_eval')::BIGINT ASC
            """,
            (int(since_ts),),
        )
        rows = cur.fetchall()
    return _compute_report_from_rows(rows)


def _print_report(report: dict[str, Any]) -> None:
    print(
        "opportunities_total={total} settled={settled} unsettled={unsettled}".format(
            total=report["total"],
            settled=report["settled"],
            unsettled=report["unsettled"],
        )
    )
    avg_brier = report["avg_model_brier"]
    avg_logloss = report["avg_model_logloss"]
    avg_market_brier = report["avg_market_brier"]
    avg_market_logloss = report["avg_market_logloss"]
    delta_brier = report["delta_brier"]
    delta_logloss = report["delta_logloss"]
    print(
        "avg_brier={brier} avg_logloss={logloss} "
        "market_avg_brier={mbrier} market_avg_logloss={mlogloss} "
        "delta_brier={dbrier} delta_logloss={dlogloss} "
        "realized_pnl={pnl}".format(
            brier=f"{avg_brier:.6f}" if avg_brier is not None else "NA",
            logloss=f"{avg_logloss:.6f}" if avg_logloss is not None else "NA",
            mbrier=f"{avg_market_brier:.6f}" if avg_market_brier is not None else "NA",
            mlogloss=(
                f"{avg_market_logloss:.6f}" if avg_market_logloss is not None else "NA"
            ),
            dbrier=f"{delta_brier:.6f}" if delta_brier is not None else "NA",
            dlogloss=f"{delta_logloss:.6f}" if delta_logloss is not None else "NA",
            pnl=f"{report['pnl_total']:.4f}",
        )
    )
    print(
        "snapshot_score_matches_total={matches_total} "
        "snapshot_score_matches_settled={matches_settled} "
        "snapshot_score_coverage_total={coverage_total} "
        "snapshot_score_coverage_settled={coverage_settled}".format(
            matches_total=report["snapshot_score_matches_total"],
            matches_settled=report["snapshot_score_matches_settled"],
            coverage_total=(
                f"{report['snapshot_score_coverage_total']:.3f}"
                if report["snapshot_score_coverage_total"] is not None
                else "NA"
            ),
            coverage_settled=(
                f"{report['snapshot_score_coverage_settled']:.3f}"
                if report["snapshot_score_coverage_settled"] is not None
                else "NA"
            ),
        )
    )
    if report["by_day"]:
        print("by_day:")
        for day, info in sorted(report["by_day"].items()):
            print(
                f"  {day} count={int(info['count'])} "
                f"pnl={info['pnl']:.4f} wins={int(info['wins'])}"
            )
    if report["buckets"]:
        print("calibration:")
        for bucket, info in sorted(report["buckets"].items()):
            count = info["count"]
            avg_prob = info["prob_sum"] / count if count else 0.0
            win_rate = info["wins"] / count if count else 0.0
            print(
                f"  {bucket} count={count} avg_prob={avg_prob:.3f} "
                f"win_rate={win_rate:.3f}"
            )


async def _run_sqlite(*, settings: Any, since_ts: int) -> int:
    print(f"backend=sqlite db_path={settings.db_path}")
    await init_db(settings.db_path)
    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA busy_timeout = 5000;")
        report = await compute_report(conn, since_ts)
    _print_report(report)
    return 0


async def _run_postgres(*, pg_dsn: str, since_ts: int) -> int:
    print("backend=postgres")
    repo = PostgresEventRepository(pg_dsn)
    repo.ensure_schema()
    repo.close()
    try:
        import psycopg
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg is required for Postgres reporting. "
            "Install with: pip install psycopg[binary]"
        ) from exc
    with psycopg.connect(pg_dsn) as conn:
        report = compute_report_postgres(conn, since_ts)
    _print_report(report)
    return 0


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()
    since_ts = int(time.time()) - max(args.since_seconds, 0)
    pg_dsn = args.pg_dsn or settings.pg_dsn
    if pg_dsn:
        return await _run_postgres(pg_dsn=pg_dsn, since_ts=since_ts)
    return await _run_sqlite(settings=settings, since_ts=since_ts)


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
