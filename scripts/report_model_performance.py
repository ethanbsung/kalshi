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
        "--scope",
        choices=["policy", "all"],
        default="policy",
        help="Evaluation scope: policy=would_trade only (default), all=all scored opportunities.",
    )
    parser.add_argument(
        "--fill-quality-limit",
        type=int,
        default=10,
        help="Number of worst slippage fills to print.",
    )
    parser.add_argument(
        "--analytics-details",
        action="store_true",
        help=(
            "Print extended analytics sections "
            "(worst markets/hour buckets, entry bands, fill quality, risk counters)."
        ),
    )
    parser.add_argument(
        "--edge-decay",
        action="store_true",
        help=(
            "Print compact edge decay diagnostics for filled trades "
            "(entry vs +1m/+5m)."
        ),
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
    buckets_model: dict[str, dict[str, float]] = {}
    buckets_market: dict[str, dict[str, float]] = {}
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
                info = buckets_model.setdefault(
                    bucket, {"count": 0, "prob_sum": 0.0, "wins": 0}
                )
                info["count"] += 1
                info["prob_sum"] += prob_val
                info["wins"] += int(trade_win or 0)
            except (TypeError, ValueError):
                pass
        if market_prob is not None:
            try:
                market_prob_yes = float(market_prob)
                prob_val = market_prob_yes if side == "YES" else 1.0 - market_prob_yes
                bucket = _bucket(prob_val)
                info = buckets_market.setdefault(
                    bucket, {"count": 0, "prob_sum": 0.0, "wins": 0}
                )
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
    model_metric_coverage_settled = model_score_count / settled if settled > 0 else None
    market_metric_coverage_settled = (
        market_score_count / settled if settled > 0 else None
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
        "model_metric_coverage_settled": model_metric_coverage_settled,
        "market_metric_coverage_settled": market_metric_coverage_settled,
        "snapshot_score_matches_total": snapshot_score_matches_total,
        "snapshot_score_matches_settled": snapshot_score_matches_settled,
        "snapshot_score_coverage_total": snapshot_score_coverage_total,
        "snapshot_score_coverage_settled": snapshot_score_coverage_settled,
        # Backward compatible aliases.
        "avg_brier": avg_model_brier,
        "avg_logloss": avg_model_logloss,
        "pnl_total": pnl_total,
        "by_day": by_day,
        "buckets": buckets_model,
        "buckets_market": buckets_market,
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


async def compute_report(
    conn: aiosqlite.Connection, since_ts: int, scope: str
) -> dict[str, Any]:
    if scope == "policy":
        where_scope = "AND o.would_trade = 1"
    else:
        where_scope = ""
    query = f"""
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
        WHERE o.ts_eval >= ?
          {where_scope}
        ORDER BY o.ts_eval ASC
    """
    cursor = await conn.execute(query, (since_ts,))
    rows = await cursor.fetchall()
    return _compute_report_from_rows([tuple(row) for row in rows])


def compute_report_postgres(conn: Any, since_ts: int, scope: str) -> dict[str, Any]:
    if scope == "policy":
        where_scope = "AND COALESCE((o.payload_json->>'would_trade')::BOOLEAN, FALSE) = TRUE"
    else:
        where_scope = ""
    with conn.cursor() as cur:
        query = f"""
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
              {where_scope}
            ORDER BY (o.payload_json->>'ts_eval')::BIGINT ASC
        """
        cur.execute(query, (int(since_ts),))
        rows = cur.fetchall()
    return _compute_report_from_rows(rows)


def _edge_value_cents(
    *, prob_yes: float | None, side: str | None, yes_bid: float | None, no_bid: float | None
) -> float | None:
    if prob_yes is None or side is None:
        return None
    side_u = side.upper()
    if side_u == "YES":
        if yes_bid is None:
            return None
        return (float(prob_yes) * 100.0) - float(yes_bid)
    if side_u == "NO":
        if no_bid is None:
            return None
        return ((1.0 - float(prob_yes)) * 100.0) - float(no_bid)
    return None


def compute_edge_decay_postgres(
    conn: Any,
    *,
    since_ts: int,
    scope: str,
    tolerance_seconds: int = 20,
) -> dict[str, Any]:
    if scope == "policy":
        where_scope = (
            "AND COALESCE((d.payload_json->>'would_trade')::BOOLEAN, FALSE) = TRUE"
        )
    else:
        where_scope = ""
    quote_tolerance_seconds = 5
    with conn.cursor() as cur:
        query = f"""
            WITH decisions AS (
                SELECT
                    d.idempotency_key,
                    (d.payload_json->>'ts_eval')::BIGINT AS ts_eval,
                    d.payload_json->>'market_id' AS market_id,
                    d.payload_json->>'side' AS side
                FROM event_store.events_raw d
                WHERE d.event_type = 'opportunity_decision'
                  AND (d.payload_json->>'ts_eval')::BIGINT >= %s
                  {where_scope}
            ),
            filled_orders AS (
                SELECT DISTINCT ON (eo.order_id)
                    eo.order_id,
                    eo.opportunity_idempotency_key
                FROM event_store.execution_order_latest eo
                JOIN event_store.execution_fill_latest f
                  ON f.order_id = eo.order_id
                WHERE eo.paper = TRUE
                  AND f.paper = TRUE
                  AND f.action = 'open'
                ORDER BY eo.order_id, f.ts_fill ASC
            ),
            filled AS (
                SELECT
                    d.market_id,
                    d.side,
                    d.ts_eval
                FROM decisions d
                JOIN filled_orders fo
                  ON fo.opportunity_idempotency_key = d.idempotency_key
            )
            SELECT
                f.market_id,
                f.side,
                f.ts_eval,
                entry.prob_yes AS entry_prob_yes,
                q_entry.yes_bid AS entry_yes_bid,
                q_entry.no_bid AS entry_no_bid,
                m1.prob_yes AS m1_prob_yes,
                q_m1.yes_bid AS m1_yes_bid,
                q_m1.no_bid AS m1_no_bid,
                m5.prob_yes AS m5_prob_yes,
                q_m5.yes_bid AS m5_yes_bid,
                q_m5.no_bid AS m5_no_bid
            FROM filled f
            LEFT JOIN LATERAL (
                SELECT
                    (e.payload_json->>'asof_ts')::BIGINT AS asof_ts,
                    (e.payload_json->>'quote_ts')::BIGINT AS quote_ts,
                    NULLIF(e.payload_json->>'prob_yes', '')::DOUBLE PRECISION AS prob_yes
                FROM event_store.events_raw e
                WHERE e.event_type = 'edge_snapshot'
                  AND e.payload_json->>'market_id' = f.market_id
                  AND (e.payload_json->>'asof_ts')::BIGINT BETWEEN f.ts_eval - %s AND f.ts_eval + %s
                ORDER BY ABS((e.payload_json->>'asof_ts')::BIGINT - f.ts_eval), (e.payload_json->>'asof_ts')::BIGINT DESC
                LIMIT 1
            ) entry ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    NULLIF(q.payload_json->>'yes_bid', '')::DOUBLE PRECISION AS yes_bid,
                    NULLIF(q.payload_json->>'no_bid', '')::DOUBLE PRECISION AS no_bid
                FROM event_store.events_raw q
                WHERE q.event_type = 'quote_update'
                  AND q.payload_json->>'market_id' = f.market_id
                  AND entry.quote_ts IS NOT NULL
                  AND (q.payload_json->>'ts')::BIGINT BETWEEN entry.quote_ts - %s AND entry.quote_ts + %s
                ORDER BY ABS((q.payload_json->>'ts')::BIGINT - entry.quote_ts), (q.payload_json->>'ts')::BIGINT DESC
                LIMIT 1
            ) q_entry ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    (e.payload_json->>'asof_ts')::BIGINT AS asof_ts,
                    (e.payload_json->>'quote_ts')::BIGINT AS quote_ts,
                    NULLIF(e.payload_json->>'prob_yes', '')::DOUBLE PRECISION AS prob_yes
                FROM event_store.events_raw e
                WHERE e.event_type = 'edge_snapshot'
                  AND e.payload_json->>'market_id' = f.market_id
                  AND (e.payload_json->>'asof_ts')::BIGINT BETWEEN (f.ts_eval + 60) - %s AND (f.ts_eval + 60) + %s
                ORDER BY ABS((e.payload_json->>'asof_ts')::BIGINT - (f.ts_eval + 60)), (e.payload_json->>'asof_ts')::BIGINT DESC
                LIMIT 1
            ) m1 ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    NULLIF(q.payload_json->>'yes_bid', '')::DOUBLE PRECISION AS yes_bid,
                    NULLIF(q.payload_json->>'no_bid', '')::DOUBLE PRECISION AS no_bid
                FROM event_store.events_raw q
                WHERE q.event_type = 'quote_update'
                  AND q.payload_json->>'market_id' = f.market_id
                  AND m1.quote_ts IS NOT NULL
                  AND (q.payload_json->>'ts')::BIGINT BETWEEN m1.quote_ts - %s AND m1.quote_ts + %s
                ORDER BY ABS((q.payload_json->>'ts')::BIGINT - m1.quote_ts), (q.payload_json->>'ts')::BIGINT DESC
                LIMIT 1
            ) q_m1 ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    (e.payload_json->>'asof_ts')::BIGINT AS asof_ts,
                    (e.payload_json->>'quote_ts')::BIGINT AS quote_ts,
                    NULLIF(e.payload_json->>'prob_yes', '')::DOUBLE PRECISION AS prob_yes
                FROM event_store.events_raw e
                WHERE e.event_type = 'edge_snapshot'
                  AND e.payload_json->>'market_id' = f.market_id
                  AND (e.payload_json->>'asof_ts')::BIGINT BETWEEN (f.ts_eval + 300) - %s AND (f.ts_eval + 300) + %s
                ORDER BY ABS((e.payload_json->>'asof_ts')::BIGINT - (f.ts_eval + 300)), (e.payload_json->>'asof_ts')::BIGINT DESC
                LIMIT 1
            ) m5 ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    NULLIF(q.payload_json->>'yes_bid', '')::DOUBLE PRECISION AS yes_bid,
                    NULLIF(q.payload_json->>'no_bid', '')::DOUBLE PRECISION AS no_bid
                FROM event_store.events_raw q
                WHERE q.event_type = 'quote_update'
                  AND q.payload_json->>'market_id' = f.market_id
                  AND m5.quote_ts IS NOT NULL
                  AND (q.payload_json->>'ts')::BIGINT BETWEEN m5.quote_ts - %s AND m5.quote_ts + %s
                ORDER BY ABS((q.payload_json->>'ts')::BIGINT - m5.quote_ts), (q.payload_json->>'ts')::BIGINT DESC
                LIMIT 1
            ) q_m5 ON TRUE
        """
        params = (
            int(since_ts),
            int(tolerance_seconds),
            int(tolerance_seconds),
            int(quote_tolerance_seconds),
            int(quote_tolerance_seconds),
            int(tolerance_seconds),
            int(tolerance_seconds),
            int(quote_tolerance_seconds),
            int(quote_tolerance_seconds),
            int(tolerance_seconds),
            int(tolerance_seconds),
            int(quote_tolerance_seconds),
            int(quote_tolerance_seconds),
        )
        cur.execute(query, params)
        rows = cur.fetchall()
    filled_total = len(rows)
    if filled_total == 0:
        return {"status": "no_filled_trades", "reason": "no_filled_trades_in_scope"}

    entry_edges: list[float] = []
    edge_1m_vals: list[float] = []
    edge_5m_vals: list[float] = []
    decay_1m_total = 0
    decay_1m_count = 0
    decay_5m_total = 0
    decay_5m_count = 0
    for row in rows:
        side = row[1]
        edge_entry = _edge_value_cents(
            prob_yes=_safe_float(row[3]),
            side=side,
            yes_bid=_safe_float(row[4]),
            no_bid=_safe_float(row[5]),
        )
        edge_1m = _edge_value_cents(
            prob_yes=_safe_float(row[6]),
            side=side,
            yes_bid=_safe_float(row[7]),
            no_bid=_safe_float(row[8]),
        )
        edge_5m = _edge_value_cents(
            prob_yes=_safe_float(row[9]),
            side=side,
            yes_bid=_safe_float(row[10]),
            no_bid=_safe_float(row[11]),
        )
        if edge_entry is not None:
            entry_edges.append(edge_entry)
        if edge_1m is not None:
            edge_1m_vals.append(edge_1m)
        if edge_5m is not None:
            edge_5m_vals.append(edge_5m)
        if edge_entry is not None and edge_1m is not None:
            decay_1m_count += 1
            if edge_1m < edge_entry:
                decay_1m_total += 1
        if edge_entry is not None and edge_5m is not None:
            decay_5m_count += 1
            if edge_5m < edge_entry:
                decay_5m_total += 1

    def _avg(values: list[float]) -> float | None:
        return (sum(values) / len(values)) if values else None

    return {
        "status": "ok",
        "filled_trades": filled_total,
        "entry_coverage": (len(entry_edges) / filled_total) if filled_total > 0 else None,
        "coverage_1m": (len(edge_1m_vals) / filled_total) if filled_total > 0 else None,
        "coverage_5m": (len(edge_5m_vals) / filled_total) if filled_total > 0 else None,
        "avg_edge_entry_cents": _avg(entry_edges),
        "avg_edge_1m_cents": _avg(edge_1m_vals),
        "avg_edge_5m_cents": _avg(edge_5m_vals),
        "frac_decayed_1m": (decay_1m_total / decay_1m_count) if decay_1m_count > 0 else None,
        "frac_decayed_5m": (decay_5m_total / decay_5m_count) if decay_5m_count > 0 else None,
    }


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
    analytics_details: bool = False,
    fill_quality: dict[str, Any] | None = None,
    risk_counters: dict[str, Any] | None = None,
    edge_decay: dict[str, Any] | None = None,
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
    print(
        "metric_coverage_settled model={model_cov} market={market_cov}".format(
            model_cov=(
                f"{report['model_metric_coverage_settled']:.3f}"
                if report["model_metric_coverage_settled"] is not None
                else "NA"
            ),
            market_cov=(
                f"{report['market_metric_coverage_settled']:.3f}"
                if report["market_metric_coverage_settled"] is not None
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
    market_buckets = report.get("buckets_market") or {}
    if market_buckets:
        print("calibration_market:")
        for bucket, info in sorted(market_buckets.items()):
            count = info["count"]
            avg_prob = info["prob_sum"] / count if count else 0.0
            win_rate = info["wins"] / count if count else 0.0
            print(
                f"  {bucket} count={count} avg_prob={avg_prob:.3f} "
                f"win_rate={win_rate:.3f}"
            )
    extreme_bins = ["0.0-0.1", "0.1-0.2", "0.8-0.9", "0.9-1.0"]
    print("extreme_bins_model:")
    for bucket in extreme_bins:
        info = (report.get("buckets") or {}).get(bucket, {})
        count = int(info.get("count", 0))
        if count > 0:
            avg_prob = float(info["prob_sum"]) / count
            win_rate = float(info["wins"]) / count
            print(
                f"  {bucket} count={count} avg_prob={avg_prob:.3f} "
                f"win_rate={win_rate:.3f}"
            )
        else:
            print(f"  {bucket} count=0 avg_prob=NA win_rate=NA")
    print("extreme_bins_market:")
    for bucket in extreme_bins:
        info = market_buckets.get(bucket, {})
        count = int(info.get("count", 0))
        if count > 0:
            avg_prob = float(info["prob_sum"]) / count
            win_rate = float(info["wins"]) / count
            print(
                f"  {bucket} count={count} avg_prob={avg_prob:.3f} "
                f"win_rate={win_rate:.3f}"
            )
        else:
            print(f"  {bucket} count=0 avg_prob=NA win_rate=NA")
    if analytics_details:
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
                    rate=(
                        f"{worst_reject_rate:.3f}"
                        if worst_reject_rate is not None
                        else "NA"
                    )
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
    if edge_decay is not None:
        if edge_decay.get("status") != "ok":
            print(
                "edge_decay status={status} reason={reason}".format(
                    status=edge_decay.get("status", "NA"),
                    reason=edge_decay.get("reason", "unavailable"),
                )
            )
        else:
            print(
                "edge_decay filled_trades={filled} entry_coverage={entry_cov} "
                "coverage_1m={cov1} coverage_5m={cov5}".format(
                    filled=edge_decay.get("filled_trades", 0),
                    entry_cov=(
                        f"{edge_decay['entry_coverage']:.3f}"
                        if edge_decay.get("entry_coverage") is not None
                        else "NA"
                    ),
                    cov1=(
                        f"{edge_decay['coverage_1m']:.3f}"
                        if edge_decay.get("coverage_1m") is not None
                        else "NA"
                    ),
                    cov5=(
                        f"{edge_decay['coverage_5m']:.3f}"
                        if edge_decay.get("coverage_5m") is not None
                        else "NA"
                    ),
                )
            )
            print(
                "edge_decay avg_edge_entry_cents={entry} avg_edge_1m_cents={m1} "
                "avg_edge_5m_cents={m5}".format(
                    entry=(
                        f"{edge_decay['avg_edge_entry_cents']:.4f}"
                        if edge_decay.get("avg_edge_entry_cents") is not None
                        else "NA"
                    ),
                    m1=(
                        f"{edge_decay['avg_edge_1m_cents']:.4f}"
                        if edge_decay.get("avg_edge_1m_cents") is not None
                        else "NA"
                    ),
                    m5=(
                        f"{edge_decay['avg_edge_5m_cents']:.4f}"
                        if edge_decay.get("avg_edge_5m_cents") is not None
                        else "NA"
                    ),
                )
            )
            print(
                "edge_decay frac_decayed_1m={d1} frac_decayed_5m={d5}".format(
                    d1=(
                        f"{edge_decay['frac_decayed_1m']:.3f}"
                        if edge_decay.get("frac_decayed_1m") is not None
                        else "NA"
                    ),
                    d5=(
                        f"{edge_decay['frac_decayed_5m']:.3f}"
                        if edge_decay.get("frac_decayed_5m") is not None
                        else "NA"
                    ),
                )
            )


async def _run_sqlite(
    *, settings: Any, since_ts: int, scope: str, edge_decay_enabled: bool
) -> int:
    print(f"backend=sqlite db_path={settings.db_path}")
    await init_db(settings.db_path)
    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA busy_timeout = 5000;")
        report = await compute_report(conn, since_ts, scope)
    edge_decay = None
    if edge_decay_enabled:
        edge_decay = {
            "status": "unsupported_backend",
            "reason": "edge_decay_requires_postgres_events_raw",
        }
    _print_report(report, edge_decay=edge_decay)
    return 0


async def _run_postgres(
    *,
    pg_dsn: str,
    since_ts: int,
    fill_quality_limit: int,
    analytics_details: bool,
    scope: str,
    edge_decay_enabled: bool,
) -> int:
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
        report = compute_report_postgres(conn, since_ts, scope)
        fill_quality = None
        risk_counters = None
        edge_decay = None
        if analytics_details:
            fill_quality = compute_fill_quality_postgres(
                conn, since_ts=since_ts, limit=max(fill_quality_limit, 0)
            )
            risk_counters = compute_risk_counters_postgres(conn, since_ts=since_ts)
        if edge_decay_enabled:
            try:
                edge_decay = compute_edge_decay_postgres(
                    conn, since_ts=since_ts, scope=scope
                )
            except Exception as exc:  # pragma: no cover - optional diagnostics
                edge_decay = {
                    "status": "unavailable",
                    "reason": f"edge_decay_query_error:{exc.__class__.__name__}",
                }
    _print_report(
        report,
        analytics_details=analytics_details,
        fill_quality=fill_quality,
        risk_counters=risk_counters,
        edge_decay=edge_decay,
    )
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
            analytics_details=args.analytics_details,
            scope=args.scope,
            edge_decay_enabled=args.edge_decay,
        )
    return await _run_sqlite(
        settings=settings,
        since_ts=since_ts,
        scope=args.scope,
        edge_decay_enabled=args.edge_decay,
    )


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
