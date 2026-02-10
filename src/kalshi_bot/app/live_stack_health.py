from __future__ import annotations

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


def format_live_health(summary: dict[str, Any]) -> str:
    return (
        "health "
        f"spot_age_s={_fmt_age(summary.get('spot_tick_age_seconds'))} "
        f"quote_age_s={_fmt_age(summary.get('quote_age_seconds'))} "
        f"snapshot_age_s={_fmt_age(summary.get('snapshot_age_seconds'))} "
        f"window_m={summary.get('window_minutes')} "
        f"snapshots={summary.get('snapshots_last_window')} "
        f"opportunities={summary.get('opportunities_last_window')} "
        f"scores={summary.get('scores_last_window')}"
    )


def _fmt_age(value: Any) -> str:
    if value is None:
        return "NA"
    try:
        return str(int(value))
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
