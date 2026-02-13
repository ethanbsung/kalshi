from __future__ import annotations

import time
from typing import Any

import aiosqlite

from kalshi_bot.kalshi.market_filters import (
    build_series_clause,
    normalize_db_status,
    normalize_series,
)


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
    status = normalize_db_status(status)
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
    spot_ts: int | None = None,
    *,
    now_ts: int | None = None,
    freshness_seconds: int = 300,
    max_horizon_seconds: int = 10 * 24 * 3600,
    grace_seconds: int = 3600,
    require_quotes: bool = True,
    exclude_extreme_asks: bool = True,
    min_ask_cents: float = 1.0,
    max_ask_cents: float = 99.0,
    max_spread_cents: float | None = None,
) -> tuple[list[str], float | None, dict[str, Any]]:
    status = normalize_db_status(status)
    now_ts = int(time.time()) if now_ts is None else int(now_ts)
    if spot_price is None:
        spot = await get_latest_spot_price(conn, product_id)
        spot_price = spot["price"]
        spot_ts = spot["ts"]

    if spot_price is None or spot_price <= 0:
        summary = {
            "method": "unavailable",
            "pct_band": pct_band,
            "top_n": top_n,
            "candidate_count_total": 0,
            "excluded_expired": 0,
            "excluded_horizon_out_of_range": 0,
            "excluded_missing_bounds": 0,
            "excluded_missing_recent_quote": 0,
            "excluded_untradable": 0,
            "selected_count": 0,
            "spot_ts": spot_ts,
            "now_ts": now_ts,
            "selection_samples": [],
        }
        return [], spot_price, summary

    normalized_series = normalize_series(series)
    clauses: list[str] = []
    params: list[Any] = []
    if status is not None:
        clauses.append("m.status = ?")
        params.append(status)
    series_clause, series_params = build_series_clause(
        normalized_series, column="m.market_id"
    )
    if series_clause:
        clauses.append(series_clause)
        params.extend(series_params)

    def _where(extra: list[str]) -> tuple[str, list[Any]]:
        all_clauses = clauses + extra
        if not all_clauses:
            return "", params.copy()
        return " WHERE " + " AND ".join(all_clauses), params.copy()

    close_expr = (
        "COALESCE(c.close_ts, m.close_ts, "
        "c.expected_expiration_ts, m.expected_expiration_ts, "
        "c.settlement_ts, m.settlement_ts)"
    )
    allowed_clause = "c.strike_type IN ('between','less','greater')"
    bounds_clause = (
        "("
        "(c.strike_type = 'between' AND c.lower IS NOT NULL AND c.upper IS NOT NULL AND c.upper > c.lower)"
        " OR (c.strike_type = 'less' AND c.upper IS NOT NULL)"
        " OR (c.strike_type = 'greater' AND c.lower IS NOT NULL)"
        ")"
    )
    base_from = "FROM kalshi_contracts c JOIN kalshi_markets m ON c.ticker = m.market_id"

    where_sql, where_params = _where([allowed_clause])
    cursor = await conn.execute(
        f"SELECT COUNT(*) {base_from}{where_sql}", where_params
    )
    candidate_count_total = int((await cursor.fetchone())[0])

    where_sql, where_params = _where(
        [allowed_clause, f"NOT {bounds_clause}"]
    )
    cursor = await conn.execute(
        f"SELECT COUNT(*) {base_from}{where_sql}", where_params
    )
    excluded_missing_bounds = int((await cursor.fetchone())[0])

    where_sql, where_params = _where(
        [
            allowed_clause,
            bounds_clause,
            f"{close_expr} IS NULL",
        ]
    )
    cursor = await conn.execute(
        f"SELECT COUNT(*) {base_from}{where_sql}", where_params
    )
    excluded_missing_close_ts = int((await cursor.fetchone())[0])

    cutoff_min = now_ts - 5
    cutoff_max = now_ts + max_horizon_seconds + grace_seconds

    where_sql, where_params = _where(
        [
            allowed_clause,
            bounds_clause,
            f"{close_expr} IS NOT NULL",
            f"{close_expr} < ?",
        ]
    )
    cursor = await conn.execute(
        f"SELECT COUNT(*) {base_from}{where_sql}",
        [*where_params, cutoff_min],
    )
    excluded_expired = int((await cursor.fetchone())[0])

    where_sql, where_params = _where(
        [
            allowed_clause,
            bounds_clause,
            f"{close_expr} IS NOT NULL",
            f"{close_expr} > ?",
        ]
    )
    cursor = await conn.execute(
        f"SELECT COUNT(*) {base_from}{where_sql}",
        [*where_params, cutoff_max],
    )
    excluded_horizon = int((await cursor.fetchone())[0])

    where_sql, where_params = _where(
        [
            allowed_clause,
            bounds_clause,
            f"{close_expr} IS NOT NULL",
            f"{close_expr} >= ?",
            f"{close_expr} <= ?",
        ]
    )
    cursor = await conn.execute(
        f"SELECT c.ticker, c.lower, c.upper, c.strike_type, {close_expr} AS close_ts "
        f"{base_from}{where_sql}",
        [*where_params, cutoff_min, cutoff_max],
    )
    rows = await cursor.fetchall()

    candidates: list[dict[str, Any]] = []
    for ticker, lower, upper, strike_type, close_ts in rows:
        lower_val = _parse_float(lower)
        upper_val = _parse_float(upper)
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
        horizon_seconds = (
            int(close_ts) - now_ts if close_ts is not None else None
        )
        candidates.append(
            {
                "ticker": ticker,
                "distance_pct": distance_pct,
                "close_ts": close_ts,
                "horizon_seconds": horizon_seconds,
            }
        )

    excluded_missing_recent_quote = 0
    excluded_untradable = 0
    if require_quotes and candidates:
        candidate_ids = [item["ticker"] for item in candidates]
        placeholders = ",".join("?" for _ in candidate_ids)
        threshold = now_ts - freshness_seconds
        quote_rows = []
        if candidate_ids:
            cursor = await conn.execute(
                f"""
                WITH latest AS (
                    SELECT market_id, MAX(ts) AS max_ts
                    FROM kalshi_quotes
                    WHERE ts >= ? AND market_id IN ({placeholders})
                    GROUP BY market_id
                )
                SELECT q.market_id, q.ts, q.yes_bid, q.yes_ask, q.no_bid, q.no_ask
                FROM kalshi_quotes q
                JOIN latest l ON q.market_id = l.market_id AND q.ts = l.max_ts
                """,
                [threshold, *candidate_ids],
            )
            quote_rows = await cursor.fetchall()

        quote_map: dict[str, dict[str, Any]] = {}
        for market_id, ts, yes_bid, yes_ask, no_bid, no_ask in quote_rows:
            quote_map[market_id] = {
                "ts": ts,
                "yes_bid": _parse_float(yes_bid),
                "yes_ask": _parse_float(yes_ask),
                "no_bid": _parse_float(no_bid),
                "no_ask": _parse_float(no_ask),
            }

        filtered: list[dict[str, Any]] = []

        def _ask_tradable(ask: float | None, bid: float | None) -> bool:
            if ask is None:
                return False
            if ask < 0.0 or ask > 100.0:
                return False
            if ask in (0.0, 100.0):
                return not exclude_extreme_asks
            if exclude_extreme_asks and (
                ask < min_ask_cents or ask > max_ask_cents
            ):
                return False
            if max_spread_cents is not None:
                if bid is None:
                    return False
                spread = ask - bid
                if spread < 0 or spread > max_spread_cents:
                    return False
            return True

        for candidate in candidates:
            market_id = candidate["ticker"]
            quote = quote_map.get(market_id)
            if quote is None:
                excluded_missing_recent_quote += 1
                continue
            yes_tradable = _ask_tradable(quote["yes_ask"], quote["yes_bid"])
            no_tradable = _ask_tradable(quote["no_ask"], quote["no_bid"])
            if not yes_tradable and not no_tradable:
                excluded_untradable += 1
                continue
            candidate["quote_ts"] = quote["ts"]
            candidate["quote_age_seconds"] = (
                now_ts - quote["ts"] if quote["ts"] is not None else None
            )
            candidate["yes_ask"] = quote["yes_ask"]
            candidate["no_ask"] = quote["no_ask"]
            filtered.append(candidate)
        candidates = filtered

    candidates.sort(key=lambda item: (item["distance_pct"], item["ticker"]))
    selected = [item for item in candidates if item["distance_pct"] <= pct_band]
    method = "pct_band"
    if top_n > 0 and len(selected) < min(top_n, len(candidates)):
        selected = candidates[:top_n]
        method = "top_n"

    selected_ids = [item["ticker"] for item in selected]
    selection_samples = [
        {
            "ticker": item["ticker"],
            "distance_pct": item["distance_pct"],
            "horizon_seconds": item["horizon_seconds"],
            "close_ts": item["close_ts"],
        }
        for item in selected[:5]
    ]
    summary = {
        "method": method,
        "pct_band": pct_band,
        "top_n": top_n,
        "candidate_count_total": candidate_count_total,
        "excluded_expired": excluded_expired,
        "excluded_horizon_out_of_range": excluded_horizon,
        "excluded_missing_bounds": excluded_missing_bounds,
        "excluded_missing_recent_quote": excluded_missing_recent_quote,
        "excluded_untradable": excluded_untradable,
        "excluded_missing_close_ts": excluded_missing_close_ts,
        "selected_count": len(selected_ids),
        "spot_ts": spot_ts,
        "now_ts": now_ts,
        "selection_samples": selection_samples,
        "series": normalized_series,
        "status": status,
        "require_quotes": require_quotes,
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
