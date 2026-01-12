from __future__ import annotations

import time
from typing import Any

import aiosqlite

from kalshi_bot.kalshi.market_filters import build_series_clause, normalize_series


def _parse_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


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


async def get_latest_spot_price(
    conn: aiosqlite.Connection, product_id: str
) -> dict[str, Any]:
    cursor = await conn.execute(
        """
        SELECT price, ts FROM spot_ticks
        WHERE product_id = ? AND price IS NOT NULL
        ORDER BY ts DESC
        LIMIT 1
        """,
        (product_id,),
    )
    row = await cursor.fetchone()
    if not row:
        return {"price": None, "ts": None}
    return {"price": _parse_float(row[0]), "ts": int(row[1])}


async def get_relevant_universe(
    conn: aiosqlite.Connection,
    pct_band: float,
    top_n: int,
    status: str | None,
    series: list[str] | None,
    product_id: str,
    spot_price: float | None = None,
) -> tuple[list[str], float | None, dict[str, Any]]:
    spot_ts = None
    if spot_price is None:
        spot = await get_latest_spot_price(conn, product_id)
        spot_price = spot["price"]
        spot_ts = spot["ts"]

    if spot_price is None or spot_price <= 0:
        summary = {
            "method": "unavailable",
            "pct_band": pct_band,
            "top_n": top_n,
            "candidate_count": 0,
            "selected_count": 0,
            "spot_ts": spot_ts,
        }
        return [], spot_price, summary

    where_sql, params = _market_filters(status, series, alias="m")
    cursor = await conn.execute(
        """
        SELECT c.ticker, c.lower, c.upper, c.strike_type
        FROM kalshi_contracts c
        JOIN kalshi_markets m ON c.ticker = m.market_id
        """ + where_sql,
        params,
    )
    rows = await cursor.fetchall()

    candidates: list[tuple[str, float]] = []
    for ticker, lower, upper, strike_type in rows:
        lower_val = _parse_float(lower)
        upper_val = _parse_float(upper)
        if lower_val is None and upper_val is None:
            continue
        price_ref = None
        if strike_type == "between":
            if lower_val is None or upper_val is None:
                continue
            price_ref = (lower_val + upper_val) / 2.0
        elif strike_type == "less":
            if upper_val is None:
                continue
            price_ref = upper_val
        elif strike_type == "greater":
            if lower_val is None:
                continue
            price_ref = lower_val
        else:
            continue

        distance_pct = abs(price_ref - spot_price) / spot_price * 100.0
        candidates.append((ticker, distance_pct))

    candidates.sort(key=lambda item: (item[1], item[0]))
    selected = [item for item in candidates if item[1] <= pct_band]
    method = "pct_band"

    if top_n > 0 and len(selected) < min(top_n, len(candidates)):
        selected = candidates[:top_n]
        method = "top_n"

    selected_ids = [ticker for ticker, _ in selected]
    summary = {
        "method": method,
        "pct_band": pct_band,
        "top_n": top_n,
        "candidate_count": len(candidates),
        "selected_count": len(selected_ids),
        "spot_ts": spot_ts,
    }
    return selected_ids, spot_price, summary


async def get_relevant_quote_coverage(
    conn: aiosqlite.Connection,
    market_ids: list[str],
    freshness_seconds: int,
) -> dict[str, Any]:
    if not market_ids:
        return {
            "relevant_total": 0,
            "relevant_with_recent_quotes": 0,
            "relevant_coverage_pct": 0.0,
        }
    placeholders = ",".join("?" for _ in market_ids)
    cursor = await conn.execute(
        f"SELECT market_id, MAX(ts) FROM kalshi_quotes WHERE market_id IN ({placeholders}) GROUP BY market_id",
        market_ids,
    )
    rows = await cursor.fetchall()
    threshold = int(time.time()) - freshness_seconds
    with_recent = sum(1 for _, ts in rows if ts is not None and int(ts) >= threshold)
    total = len(market_ids)
    coverage_pct = (with_recent / total * 100.0) if total else 0.0
    return {
        "relevant_total": total,
        "relevant_with_recent_quotes": with_recent,
        "relevant_coverage_pct": coverage_pct,
    }


def evaluate_smoke_conditions(
    latest_age_seconds: int | None,
    freshness_seconds: int,
    relevant_total: int,
    relevant_coverage_pct: float,
    min_relevant_coverage: float,
) -> tuple[bool, list[str]]:
    failures: list[str] = []
    if latest_age_seconds is None or latest_age_seconds > freshness_seconds * 2:
        failures.append("stale_quotes")
    if relevant_total == 0:
        failures.append("no_relevant_markets")
    if relevant_coverage_pct < min_relevant_coverage:
        failures.append("low_relevant_coverage")
    return bool(failures), failures
