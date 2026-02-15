from __future__ import annotations

import asyncio
from typing import Any

import aiosqlite


async def collect_live_health(
    conn: aiosqlite.Connection,
    *,
    now_ts: int,
    product_id: str,
    window_minutes: int,
    max_horizon_seconds: int | None = None,
) -> dict[str, Any]:
    _ = max_horizon_seconds  # Forward-compatible with stack-level horizon config.
    window_seconds = max(window_minutes, 1) * 60
    window_start = now_ts - window_seconds

    latest_spot_ts = await _fetch_scalar(
        conn,
        "SELECT MAX(ts) FROM spot_ticks WHERE product_id = ?",
        (product_id,),
    )
    latest_quote_ts = await _fetch_scalar(
        conn,
        "SELECT MAX(ts) FROM kalshi_quotes",
        (),
    )
    latest_snapshot_asof_ts = await _fetch_scalar(
        conn,
        "SELECT MAX(asof_ts) FROM kalshi_edge_snapshots",
        (),
    )

    snapshots_last_window = await _fetch_scalar(
        conn,
        "SELECT COUNT(*) FROM kalshi_edge_snapshots WHERE asof_ts >= ?",
        (window_start,),
    )
    opportunities_last_window = await _fetch_scalar(
        conn,
        "SELECT COUNT(*) FROM opportunities WHERE ts_eval >= ?",
        (window_start,),
    )
    scores_last_window = await _fetch_scalar(
        conn,
        "SELECT COUNT(*) FROM kalshi_edge_snapshot_scores WHERE created_ts >= ?",
        (window_start,),
    )

    return {
        "now_ts": now_ts,
        "window_minutes": max(window_minutes, 1),
        "spot_tick_age_seconds": _age(now_ts, latest_spot_ts),
        "quote_age_seconds": _age(now_ts, latest_quote_ts),
        "snapshot_age_seconds": _age(now_ts, latest_snapshot_asof_ts),
        "snapshots_last_window": int(snapshots_last_window or 0),
        "opportunities_last_window": int(opportunities_last_window or 0),
        "scores_last_window": int(scores_last_window or 0),
    }


async def collect_live_health_postgres(
    *,
    pg_dsn: str,
    now_ts: int,
    product_id: str,
    window_minutes: int,
    max_horizon_seconds: int | None = None,
) -> dict[str, Any]:
    return await asyncio.to_thread(
        _collect_live_health_postgres_sync,
        pg_dsn=pg_dsn,
        now_ts=now_ts,
        product_id=product_id,
        window_minutes=window_minutes,
        max_horizon_seconds=max_horizon_seconds,
    )


def _collect_live_health_postgres_sync(
    *,
    pg_dsn: str,
    now_ts: int,
    product_id: str,
    window_minutes: int,
    max_horizon_seconds: int | None = None,
) -> dict[str, Any]:
    _ = max_horizon_seconds  # Forward-compatible with stack-level horizon config.
    window_seconds = max(window_minutes, 1) * 60
    window_start = now_ts - window_seconds
    try:
        import psycopg
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg is required for Postgres health checks. "
            "Install with: pip install psycopg[binary]"
        ) from exc

    with psycopg.connect(pg_dsn) as conn:
        latest_spot_ts = _fetch_scalar_postgres(
            conn,
            "SELECT MAX(ts) FROM event_store.state_spot_latest WHERE product_id = %s",
            (product_id,),
        )
        latest_quote_ts = _fetch_scalar_postgres(
            conn,
            "SELECT MAX(ts) FROM event_store.state_quote_latest",
            (),
        )
        latest_snapshot_asof_ts = _fetch_scalar_postgres(
            conn,
            "SELECT MAX(asof_ts) FROM event_store.strategy_edge_latest",
            (),
        )

        snapshots_last_window = _fetch_scalar_postgres(
            conn,
            "SELECT COUNT(*) FROM event_store.events_raw "
            "WHERE event_type = %s AND ts_event >= %s",
            ("edge_snapshot", window_start),
        )
        opportunities_last_window = _fetch_scalar_postgres(
            conn,
            "SELECT COUNT(*) FROM event_store.events_raw "
            "WHERE event_type = %s AND ts_event >= %s",
            ("opportunity_decision", window_start),
        )
        scores_last_window = _fetch_scalar_postgres(
            conn,
            "SELECT COUNT(*) FROM event_store.fact_edge_snapshot_scores WHERE created_ts >= %s",
            (window_start,),
        )
        execution_orders_last_window = _fetch_scalar_postgres(
            conn,
            "SELECT COUNT(*) FROM event_store.events_raw "
            "WHERE event_type = %s AND ts_event >= %s",
            ("execution_order", window_start),
        )
        execution_rejects_last_window = _fetch_scalar_postgres(
            conn,
            "SELECT COUNT(*) FROM event_store.events_raw "
            "WHERE event_type = %s AND ts_event >= %s "
            "AND COALESCE(payload_json->>'status', '') = 'rejected'",
            ("execution_order", window_start),
        )

    orders_count = int(execution_orders_last_window or 0)
    rejects_count = int(execution_rejects_last_window or 0)
    reject_rate = (rejects_count / orders_count) if orders_count > 0 else None

    return {
        "now_ts": now_ts,
        "window_minutes": max(window_minutes, 1),
        "spot_tick_age_seconds": _age(now_ts, latest_spot_ts),
        "quote_age_seconds": _age(now_ts, latest_quote_ts),
        "snapshot_age_seconds": _age(now_ts, latest_snapshot_asof_ts),
        "snapshots_last_window": int(snapshots_last_window or 0),
        "opportunities_last_window": int(opportunities_last_window or 0),
        "scores_last_window": int(scores_last_window or 0),
        "execution_orders_last_window": orders_count,
        "execution_rejects_last_window": rejects_count,
        "execution_reject_rate_last_window": reject_rate,
    }


def format_live_health(summary: dict[str, Any]) -> str:
    return (
        "health "
        f"spot_age_s={_fmt_age(summary.get('spot_tick_age_seconds'))} "
        f"quote_age_s={_fmt_age(summary.get('quote_age_seconds'))} "
        f"snapshot_age_s={_fmt_age(summary.get('snapshot_age_seconds'))} "
        f"window_m={summary.get('window_minutes')} "
        f"snapshots={summary.get('snapshots_last_window')} "
        f"opportunities={summary.get('opportunities_last_window')} "
        f"scores={summary.get('scores_last_window')} "
        f"exec_orders={summary.get('execution_orders_last_window')} "
        f"exec_rejects={summary.get('execution_rejects_last_window')} "
        f"exec_reject_rate={_fmt_ratio(summary.get('execution_reject_rate_last_window'))}"
    )


def _fmt_age(value: Any) -> str:
    if value is None:
        return "NA"
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return "NA"


def _fmt_ratio(value: Any) -> str:
    if value is None:
        return "NA"
    try:
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return "NA"


def _age(now_ts: int, ts: int | None) -> int | None:
    if ts is None:
        return None
    return max(0, now_ts - ts)


async def _fetch_scalar(
    conn: aiosqlite.Connection, sql: str, params: tuple[Any, ...]
) -> int | None:
    cursor = await conn.execute(sql, params)
    row = await cursor.fetchone()
    if not row or row[0] is None:
        return None
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return None


def _fetch_scalar_postgres(conn: Any, sql: str, params: tuple[Any, ...]) -> int | None:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return None
