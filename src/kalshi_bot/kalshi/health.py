from __future__ import annotations

import time
from typing import Any

import aiosqlite

from kalshi_bot.kalshi.market_filters import build_series_clause, normalize_series


def _market_filters(
    status: str | None, series: list[str] | None, alias: str
) -> tuple[str, list[Any]]:
    where: list[str] = []
    params: list[Any] = []
    if status is not None:
        where.append(f"{alias}.status = ?")
        params.append(status)
    clause, clause_params = build_series_clause(
        normalize_series(series), column=f"{alias}.market_id"
    )
    if clause:
        where.append(clause)
        params.extend(clause_params)
    if not where:
        return "", []
    return " WHERE " + " AND ".join(where), params


async def get_quote_freshness(
    conn: aiosqlite.Connection,
    within_seconds: int,
    status: str | None,
    series: list[str] | None,
) -> dict[str, Any]:
    where_sql, params = _market_filters(status, series, alias="m")
    sql = (
        "SELECT MAX(q.ts) FROM kalshi_quotes q "
        "JOIN kalshi_markets m ON q.market_id = m.market_id"
        f"{where_sql}"
    )
    cursor = await conn.execute(sql, params)
    row = await cursor.fetchone()
    latest_ts = int(row[0]) if row and row[0] is not None else None
    age_seconds = None
    is_fresh = False
    if latest_ts is not None:
        age_seconds = int(time.time()) - latest_ts
        is_fresh = age_seconds <= within_seconds
    return {
        "latest_ts": latest_ts,
        "age_seconds": age_seconds,
        "is_fresh": is_fresh,
    }


async def get_quote_coverage(
    conn: aiosqlite.Connection,
    lookback_seconds: int,
    status: str | None,
    series: list[str] | None,
) -> dict[str, Any]:
    where_sql, params = _market_filters(status, series, alias="m")
    cursor = await conn.execute(
        "SELECT COUNT(*) FROM kalshi_markets m" + where_sql,
        params,
    )
    total_markets = int((await cursor.fetchone())[0])

    lookback_ts = int(time.time()) - lookback_seconds
    quote_where = "WHERE q.ts >= ?"
    if where_sql:
        quote_where += " AND " + where_sql.replace(" WHERE ", "")
    params_with_ts = [lookback_ts, *params]
    cursor = await conn.execute(
        "SELECT COUNT(DISTINCT q.market_id) "
        "FROM kalshi_quotes q JOIN kalshi_markets m "
        "ON q.market_id = m.market_id "
        f"{quote_where}",
        params_with_ts,
    )
    markets_with_quotes = int((await cursor.fetchone())[0])
    coverage_pct = (
        (markets_with_quotes / total_markets * 100.0) if total_markets else 0.0
    )
    return {
        "total_markets": total_markets,
        "markets_with_quotes": markets_with_quotes,
        "coverage_pct": coverage_pct,
    }
