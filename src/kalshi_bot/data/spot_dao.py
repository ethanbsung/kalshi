"""Spot price accessors for spot_ticks."""

from __future__ import annotations

from typing import Any

import aiosqlite


def _parse_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _filter_positive(values: list[tuple[int, float]]) -> list[tuple[int, float]]:
    return [(ts, price) for ts, price in values if price is not None and price > 0]


def _sort_by_ts(rows: list[tuple[int, float]]) -> list[tuple[int, float]]:
    return sorted(rows, key=lambda item: item[0])


def _normalize_rows(rows: list[tuple[Any, Any]]) -> list[tuple[int, float]]:
    normalized: list[tuple[int, float]] = []
    for ts, price in rows:
        ts_val = _parse_int(ts)
        price_val = _parse_float(price)
        if ts_val is None or price_val is None:
            continue
        normalized.append((ts_val, price_val))
    return _sort_by_ts(_filter_positive(normalized))


async def get_latest_spot(
    conn: aiosqlite.Connection, product_id: str
) -> tuple[int, float] | None:
    cursor = await conn.execute(
        "SELECT ts, price FROM spot_ticks "
        "WHERE product_id = ? AND price IS NOT NULL "
        "ORDER BY ts DESC LIMIT 1",
        (product_id,),
    )
    row = await cursor.fetchone()
    if row is None:
        return None
    ts, price = row
    ts_val = _parse_int(ts)
    price_val = _parse_float(price)
    if ts_val is None or price_val is None or price_val <= 0:
        return None
    return ts_val, price_val


async def get_spot_history(
    conn: aiosqlite.Connection,
    product_id: str,
    lookback_seconds: int,
    max_points: int,
) -> list[tuple[int, float]]:
    cursor = await conn.execute(
        "SELECT ts, price FROM spot_ticks "
        "WHERE product_id = ? AND ts >= (strftime('%s','now') - ?) "
        "AND price IS NOT NULL "
        "ORDER BY ts DESC LIMIT ?",
        (product_id, lookback_seconds, max_points),
    )
    rows = await cursor.fetchall()
    return _normalize_rows(rows)
