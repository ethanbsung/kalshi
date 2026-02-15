from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from collections import defaultdict
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
        "--fill-quality-limit",
        type=int,
        default=10,
        help="Number of worst slippage fills to print.",
    )
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
    pnl_by_market_side: dict[tuple[str, str], dict[str, float]] = defaultdict(
        lambda: {"count": 0.0, "pnl": 0.0}
    )
    pnl_by_hour_series_side: dict[tuple[str, str, str], dict[str, float]] = defaultdict(
        lambda: {"count": 0.0, "pnl": 0.0}
    )
    pnl_by_entry_band: dict[str, dict[str, float]] = defaultdict(
        lambda: {"count": 0.0, "pnl": 0.0}
    )

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
        realized = None
        if price_used is not None and side in {"YES", "NO"}:
            realized = _realized_pnl(outcome_val, side, price_used)
            pnl_total += realized

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
        if realized is not None:
            day_info["pnl"] += realized
        day_info["wins"] += int(trade_win or 0)

        if realized is not None and isinstance(side, str) and isinstance(_market_id, str):
            market_key = (_market_id, side)
            pnl_by_market_side[market_key]["count"] += 1
            pnl_by_market_side[market_key]["pnl"] += realized

            ts_bucket = settled_ts if settled_ts is not None else ts_eval
            hour_bucket = datetime.fromtimestamp(
                int(ts_bucket), tz=timezone.utc
            ).strftime("%Y-%m-%d %H:00")
            series = _market_id.split("-", 1)[0]
            hour_key = (hour_bucket, series, side)
            pnl_by_hour_series_side[hour_key]["count"] += 1
            pnl_by_hour_series_side[hour_key]["pnl"] += realized

            band_floor = int(max(0, min(90, math.floor(price_used / 10.0) * 10)))
            band = f"{band_floor:02d}-{band_floor + 10:02d}"
            pnl_by_entry_band[band]["count"] += 1
            pnl_by_entry_band[band]["pnl"] += realized

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
        "worst_markets": sorted(
            [
                {
                    "market_id": market_id,
                    "side": side,
                    "count": int(stats["count"]),
                    "pnl": float(stats["pnl"]),
                    "avg_pnl": float(stats["pnl"]) / float(stats["count"]),
                }
                for (market_id, side), stats in pnl_by_market_side.items()
                if stats["count"] > 0
            ],
            key=lambda row: row["pnl"],
        )[:20],
        "worst_hour_buckets": sorted(
            [
                {
                    "hour_utc": hour,
                    "series": series,
                    "side": side,
                    "count": int(stats["count"]),
                    "pnl": float(stats["pnl"]),
                    "avg_pnl": float(stats["pnl"]) / float(stats["count"]),
                }
                for (hour, series, side), stats in pnl_by_hour_series_side.items()
                if stats["count"] > 0
            ],
            key=lambda row: row["pnl"],
        )[:20],
        "entry_band_attribution": sorted(
            [
                {
                    "entry_band_cents": band,
                    "count": int(stats["count"]),
                    "pnl": float(stats["pnl"]),
                    "avg_pnl": float(stats["pnl"]) / float(stats["count"]),
                }
                for band, stats in pnl_by_entry_band.items()
                if stats["count"] > 0
            ],
            key=lambda row: row["pnl"],
        ),
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


def compute_fill_quality_postgres(
    conn: Any, *, since_ts: int, limit: int
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH orders AS (
                SELECT
                    e.idempotency_key,
                    (e.payload_json->>'order_id') AS order_id,
                    (e.payload_json->>'market_id') AS market_id,
                    (e.payload_json->>'side') AS side,
                    (e.payload_json->>'ts_order')::BIGINT AS ts_order,
                    NULLIF(e.payload_json->>'price_cents', '')::DOUBLE PRECISION AS order_price_cents,
                    COALESCE((e.payload_json->>'status')::TEXT, '') AS status
                FROM event_store.events_raw e
                WHERE e.event_type = 'execution_order'
                  AND COALESCE((e.payload_json->>'paper')::BOOLEAN, TRUE) = TRUE
                  AND (e.payload_json->>'ts_order')::BIGINT >= %s
            ),
            fills AS (
                SELECT
                    (e.payload_json->>'order_id') AS order_id,
                    MIN((e.payload_json->>'ts_fill')::BIGINT) AS first_fill_ts,
                    (ARRAY_AGG(NULLIF(e.payload_json->>'price_cents', '')::DOUBLE PRECISION
                        ORDER BY (e.payload_json->>'ts_fill')::BIGINT ASC))[1] AS fill_price_cents
                FROM event_store.events_raw e
                WHERE e.event_type = 'execution_fill'
                  AND COALESCE((e.payload_json->>'paper')::BOOLEAN, TRUE) = TRUE
                  AND (e.payload_json->>'ts_fill')::BIGINT >= %s
                GROUP BY (e.payload_json->>'order_id')
            ),
            joined AS (
                SELECT
                    o.order_id,
                    o.market_id,
                    o.side,
                    o.ts_order,
                    o.order_price_cents,
                    o.status,
                    f.first_fill_ts,
                    f.fill_price_cents,
                    CASE
                        WHEN o.order_price_cents IS NOT NULL AND f.fill_price_cents IS NOT NULL
                        THEN f.fill_price_cents - o.order_price_cents
                        ELSE NULL
                    END AS slippage_cents
                FROM orders o
                LEFT JOIN fills f
                  ON f.order_id = o.order_id
            )
            SELECT
                COUNT(*)::BIGINT AS orders_total,
                COUNT(*) FILTER (WHERE status = 'accepted')::BIGINT AS accepted_total,
                COUNT(*) FILTER (WHERE status = 'rejected')::BIGINT AS rejected_total,
                COUNT(*) FILTER (WHERE first_fill_ts IS NOT NULL)::BIGINT AS filled_total,
                AVG(slippage_cents) AS avg_slippage_cents,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY slippage_cents) AS p50_slippage_cents,
                percentile_cont(0.95) WITHIN GROUP (ORDER BY slippage_cents) AS p95_slippage_cents,
                AVG(CASE WHEN slippage_cents > 0 THEN 1.0 ELSE 0.0 END) AS adverse_fill_rate
            FROM joined
            """,
            (int(since_ts), int(since_ts)),
        )
        summary_row = cur.fetchone()

        cur.execute(
            """
            WITH orders AS (
                SELECT
                    (e.payload_json->>'order_id') AS order_id,
                    (e.payload_json->>'market_id') AS market_id,
                    (e.payload_json->>'side') AS side,
                    (e.payload_json->>'ts_order')::BIGINT AS ts_order,
                    NULLIF(e.payload_json->>'price_cents', '')::DOUBLE PRECISION AS order_price_cents
                FROM event_store.events_raw e
                WHERE e.event_type = 'execution_order'
                  AND COALESCE((e.payload_json->>'paper')::BOOLEAN, TRUE) = TRUE
                  AND (e.payload_json->>'ts_order')::BIGINT >= %s
            ),
            fills AS (
                SELECT
                    (e.payload_json->>'order_id') AS order_id,
                    MIN((e.payload_json->>'ts_fill')::BIGINT) AS first_fill_ts,
                    (ARRAY_AGG(NULLIF(e.payload_json->>'price_cents', '')::DOUBLE PRECISION
                        ORDER BY (e.payload_json->>'ts_fill')::BIGINT ASC))[1] AS fill_price_cents
                FROM event_store.events_raw e
                WHERE e.event_type = 'execution_fill'
                  AND COALESCE((e.payload_json->>'paper')::BOOLEAN, TRUE) = TRUE
                  AND (e.payload_json->>'ts_fill')::BIGINT >= %s
                GROUP BY (e.payload_json->>'order_id')
            )
            SELECT
                o.order_id,
                o.market_id,
                o.side,
                o.ts_order,
                f.first_fill_ts,
                o.order_price_cents,
                f.fill_price_cents,
                (f.fill_price_cents - o.order_price_cents) AS slippage_cents
            FROM orders o
            JOIN fills f
              ON f.order_id = o.order_id
            WHERE o.order_price_cents IS NOT NULL
              AND f.fill_price_cents IS NOT NULL
            ORDER BY (f.fill_price_cents - o.order_price_cents) DESC, o.ts_order DESC
            LIMIT %s
            """,
            (int(since_ts), int(since_ts), max(int(limit), 0)),
        )
        worst_rows = cur.fetchall()

    summary = {
        "orders_total": int(summary_row[0] or 0),
        "accepted_total": int(summary_row[1] or 0),
        "rejected_total": int(summary_row[2] or 0),
        "filled_total": int(summary_row[3] or 0),
        "avg_slippage_cents": _safe_float(summary_row[4]),
        "p50_slippage_cents": _safe_float(summary_row[5]),
        "p95_slippage_cents": _safe_float(summary_row[6]),
        "adverse_fill_rate": _safe_float(summary_row[7]),
    }
    worst = [
        {
            "order_id": row[0],
            "market_id": row[1],
            "side": row[2],
            "ts_order": int(row[3]) if row[3] is not None else None,
            "ts_fill": int(row[4]) if row[4] is not None else None,
            "order_price_cents": _safe_float(row[5]),
            "fill_price_cents": _safe_float(row[6]),
            "slippage_cents": _safe_float(row[7]),
        }
        for row in worst_rows
    ]
    return {"summary": summary, "worst_slippage_fills": worst}


def compute_risk_counters_postgres(conn: Any, *, since_ts: int) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH orders AS (
                SELECT
                    date_trunc(
                        'hour',
                        to_timestamp((e.payload_json->>'ts_order')::BIGINT) AT TIME ZONE 'UTC'
                    ) AS hour_utc,
                    COALESCE((e.payload_json->>'status')::TEXT, '') AS status
                FROM event_store.events_raw e
                WHERE e.event_type = 'execution_order'
                  AND COALESCE((e.payload_json->>'paper')::BOOLEAN, TRUE) = TRUE
                  AND (e.payload_json->>'ts_order')::BIGINT >= %s
            )
            SELECT
                hour_utc,
                COUNT(*)::BIGINT AS orders_total,
                COUNT(*) FILTER (WHERE status = 'rejected')::BIGINT AS rejected_total
            FROM orders
            GROUP BY hour_utc
            ORDER BY hour_utc ASC
            """,
            (int(since_ts),),
        )
        rows = cur.fetchall()

    per_hour = []
    worst_reject_rate = None
    for hour_utc, orders_total, rejected_total in rows:
        orders_int = int(orders_total or 0)
        rejected_int = int(rejected_total or 0)
        reject_rate = (rejected_int / orders_int) if orders_int > 0 else 0.0
        per_hour.append(
            {
                "hour_utc": str(hour_utc),
                "orders_total": orders_int,
                "rejected_total": rejected_int,
                "reject_rate": reject_rate,
            }
        )
        if worst_reject_rate is None or reject_rate > worst_reject_rate:
            worst_reject_rate = reject_rate
    return {
        "hourly_order_reject": per_hour,
        "worst_reject_rate": worst_reject_rate,
    }


def _print_report(
    report: dict[str, Any],
    *,
    fill_quality: dict[str, Any] | None = None,
    risk_counters: dict[str, Any] | None = None,
) -> None:
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
    worst_markets = report.get("worst_markets") or []
    if worst_markets:
        print("worst_markets:")
        for row in worst_markets[:10]:
            print(
                "  {market} side={side} trades={count} pnl={pnl:.4f} avg={avg:.4f}".format(
                    market=row["market_id"],
                    side=row["side"],
                    count=row["count"],
                    pnl=row["pnl"],
                    avg=row["avg_pnl"],
                )
            )
    worst_hour = report.get("worst_hour_buckets") or []
    if worst_hour:
        print("worst_hour_buckets:")
        for row in worst_hour[:10]:
            print(
                "  {hour} series={series} side={side} trades={count} pnl={pnl:.4f} avg={avg:.4f}".format(
                    hour=row["hour_utc"],
                    series=row["series"],
                    side=row["side"],
                    count=row["count"],
                    pnl=row["pnl"],
                    avg=row["avg_pnl"],
                )
            )
    entry_bands = report.get("entry_band_attribution") or []
    if entry_bands:
        print("entry_band_attribution:")
        for row in entry_bands:
            print(
                "  {band} trades={count} pnl={pnl:.4f} avg={avg:.4f}".format(
                    band=row["entry_band_cents"],
                    count=row["count"],
                    pnl=row["pnl"],
                    avg=row["avg_pnl"],
                )
            )
    if fill_quality:
        summary = fill_quality.get("summary", {})
        print(
            "fill_quality orders_total={orders} accepted={accepted} rejected={rejected} "
            "filled={filled} avg_slippage_cents={avg} p50_slippage_cents={p50} "
            "p95_slippage_cents={p95} adverse_fill_rate={adverse}".format(
                orders=summary.get("orders_total", 0),
                accepted=summary.get("accepted_total", 0),
                rejected=summary.get("rejected_total", 0),
                filled=summary.get("filled_total", 0),
                avg=(
                    f"{summary['avg_slippage_cents']:.3f}"
                    if summary.get("avg_slippage_cents") is not None
                    else "NA"
                ),
                p50=(
                    f"{summary['p50_slippage_cents']:.3f}"
                    if summary.get("p50_slippage_cents") is not None
                    else "NA"
                ),
                p95=(
                    f"{summary['p95_slippage_cents']:.3f}"
                    if summary.get("p95_slippage_cents") is not None
                    else "NA"
                ),
                adverse=(
                    f"{summary['adverse_fill_rate']:.3f}"
                    if summary.get("adverse_fill_rate") is not None
                    else "NA"
                ),
            )
        )
        worst_slippage = fill_quality.get("worst_slippage_fills") or []
        if worst_slippage:
            print("worst_slippage_fills:")
            for row in worst_slippage:
                print(
                    "  order_id={order_id} market={market} side={side} "
                    "order_c={order_c} fill_c={fill_c} slippage_c={slip} "
                    "ts_order={ts_order} ts_fill={ts_fill}".format(
                        order_id=row.get("order_id"),
                        market=row.get("market_id"),
                        side=row.get("side"),
                        order_c=(
                            f"{row['order_price_cents']:.2f}"
                            if row.get("order_price_cents") is not None
                            else "NA"
                        ),
                        fill_c=(
                            f"{row['fill_price_cents']:.2f}"
                            if row.get("fill_price_cents") is not None
                            else "NA"
                        ),
                        slip=(
                            f"{row['slippage_cents']:.2f}"
                            if row.get("slippage_cents") is not None
                            else "NA"
                        ),
                        ts_order=row.get("ts_order"),
                        ts_fill=row.get("ts_fill"),
                    )
                )
    if risk_counters:
        worst_reject_rate = risk_counters.get("worst_reject_rate")
        print(
            "risk_counters worst_hourly_reject_rate={rate}".format(
                rate=f"{worst_reject_rate:.3f}" if worst_reject_rate is not None else "NA"
            )
        )
        hourly = risk_counters.get("hourly_order_reject") or []
        if hourly:
            print("hourly_order_reject:")
            for row in hourly[-12:]:
                print(
                    "  {hour} orders={orders} rejected={rejected} reject_rate={rate:.3f}".format(
                        hour=row["hour_utc"],
                        orders=row["orders_total"],
                        rejected=row["rejected_total"],
                        rate=row["reject_rate"],
                    )
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


async def _run_postgres(*, pg_dsn: str, since_ts: int, fill_quality_limit: int) -> int:
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
        fill_quality = compute_fill_quality_postgres(
            conn, since_ts=since_ts, limit=max(fill_quality_limit, 0)
        )
        risk_counters = compute_risk_counters_postgres(conn, since_ts=since_ts)
    _print_report(report, fill_quality=fill_quality, risk_counters=risk_counters)
    return 0


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()
    since_ts = int(time.time()) - max(args.since_seconds, 0)
    pg_dsn = args.pg_dsn or settings.pg_dsn
    if pg_dsn:
        return await _run_postgres(
            pg_dsn=pg_dsn,
            since_ts=since_ts,
            fill_quality_limit=args.fill_quality_limit,
        )
    return await _run_sqlite(settings=settings, since_ts=since_ts)


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
