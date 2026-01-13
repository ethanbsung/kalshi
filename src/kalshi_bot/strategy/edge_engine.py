"""Compute EV edges for Kalshi markets from DB inputs."""

from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass
from typing import Any, Callable

import aiosqlite

from kalshi_bot.data.dao import Dao
from kalshi_bot.data.spot_dao import get_latest_spot, get_spot_history
from kalshi_bot.kalshi.health import get_relevant_universe
from kalshi_bot.kalshi.market_filters import normalize_series
from kalshi_bot.kalshi.error_utils import add_failed_sample
from kalshi_bot.kalshi.fees import taker_fee_dollars
from kalshi_bot.models.probability import (
    SECONDS_PER_YEAR,
    prob_between,
    prob_greater_equal,
    prob_less_equal,
)
from kalshi_bot.models.volatility import (
    annualize_vol,
    compute_log_returns,
    ewma_volatility,
)
from kalshi_bot.strategy.edge_math import ev_take_no, ev_take_yes

FeeFn = Callable[[float | None, int], float | None]


@dataclass(frozen=True)
class EdgeInputs:
    spot_price: float
    spot_ts: int
    sigma_annualized: float
    now_ts: int


@dataclass(frozen=True)
class EdgeRow:
    ts: int
    market_id: str
    settlement_ts: int | None
    horizon_seconds: int | None
    spot_price: float
    sigma_annualized: float
    prob_yes: float
    yes_bid: float | None
    yes_ask: float | None
    no_bid: float | None
    no_ask: float | None
    ev_take_yes: float | None
    ev_take_no: float | None
    raw_json: str


@dataclass(frozen=True)
class SigmaParams:
    lookback_seconds: int
    max_points: int
    ewma_lambda: float
    min_points: int
    sigma_floor: float
    sigma_cap: float


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _z_score(
    spot: float, strike: float, horizon_seconds: int, sigma_annualized: float
) -> float | None:
    if spot <= 0 or strike <= 0 or sigma_annualized <= 0:
        return None
    if horizon_seconds <= 0:
        return None
    t = horizon_seconds / SECONDS_PER_YEAR
    if t <= 0:
        return None
    sigma_t = sigma_annualized * math.sqrt(t)
    if sigma_t <= 0:
        return None
    return (math.log(strike / spot) + 0.5 * sigma_t * sigma_t) / sigma_t


def prob_yes_for_contract(
    spot: float,
    sigma: float,
    horizon_seconds: int,
    contract_row: dict[str, Any],
) -> float | None:
    """Return YES probability for Kalshi strike/range contracts.

    Semantics:
      - less:    YES if S_T <= upper
      - greater: YES if S_T >= lower
      - between: YES if lower <= S_T < upper
    """
    strike_type = contract_row.get("strike_type")
    lower = _safe_float(contract_row.get("lower"))
    upper = _safe_float(contract_row.get("upper"))

    if strike_type == "less":
        if upper is None or upper <= 0:
            return None
        return prob_less_equal(spot, upper, horizon_seconds, sigma)
    if strike_type == "greater":
        if lower is None or lower <= 0:
            return None
        return prob_greater_equal(spot, lower, horizon_seconds, sigma)
    if strike_type == "between":
        if lower is None or upper is None:
            return None
        if lower <= 0 or upper <= 0 or upper <= lower:
            return None
        return prob_between(spot, lower, upper, horizon_seconds, sigma)
    return None


def compute_edge_for_market(
    contract: dict[str, Any],
    quote: dict[str, Any],
    inputs: EdgeInputs,
    fee_fn: FeeFn,
    horizon_seconds: int,
    contracts: int = 1,
) -> EdgeRow | None:
    market_id = contract.get("ticker") or contract.get("market_id")
    if not market_id or not isinstance(market_id, str):
        return None

    settlement_ts = _safe_int(contract.get("settlement_ts"))
    if settlement_ts is None:
        return None

    if horizon_seconds < 0:
        horizon_seconds = 0

    prob_yes = prob_yes_for_contract(
        inputs.spot_price,
        inputs.sigma_annualized,
        horizon_seconds,
        contract,
    )
    if prob_yes is None:
        return None

    yes_bid = _safe_float(quote.get("yes_bid"))
    yes_ask = _safe_float(quote.get("yes_ask"))
    no_bid = _safe_float(quote.get("no_bid"))
    no_ask = _safe_float(quote.get("no_ask"))

    ev_yes = ev_take_yes(prob_yes, yes_ask, fee_fn, contracts=contracts)
    ev_no = ev_take_no(prob_yes, no_ask, fee_fn, contracts=contracts)
    if ev_yes is None and ev_no is None:
        return None

    metadata = {
        "quote_ts": quote.get("ts"),
        "strike_type": contract.get("strike_type"),
        "lower": contract.get("lower"),
        "upper": contract.get("upper"),
    }
    return EdgeRow(
        ts=inputs.now_ts,
        market_id=market_id,
        settlement_ts=settlement_ts,
        horizon_seconds=horizon_seconds,
        spot_price=inputs.spot_price,
        sigma_annualized=inputs.sigma_annualized,
        prob_yes=prob_yes,
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        ev_take_yes=ev_yes,
        ev_take_no=ev_no,
        raw_json=json.dumps(metadata),
    )


def _estimate_step_seconds(timestamps: list[int]) -> float | None:
    if len(timestamps) < 2:
        return None
    diffs = [
        b - a
        for a, b in zip(timestamps, timestamps[1:])
        if b > a
    ]
    if not diffs:
        return None
    diffs.sort()
    mid = diffs[len(diffs) // 2]
    return float(mid)


async def _load_contracts(
    conn: aiosqlite.Connection, market_ids: list[str]
) -> dict[str, dict[str, Any]]:
    if not market_ids:
        return {}
    placeholders = ",".join("?" for _ in market_ids)
    cursor = await conn.execute(
        f"SELECT ticker, lower, upper, strike_type, settlement_ts FROM kalshi_contracts WHERE ticker IN ({placeholders})",
        market_ids,
    )
    rows = await cursor.fetchall()
    contracts: dict[str, dict[str, Any]] = {}
    for ticker, lower, upper, strike_type, settlement_ts in rows:
        contracts[ticker] = {
            "ticker": ticker,
            "lower": lower,
            "upper": upper,
            "strike_type": strike_type,
            "settlement_ts": settlement_ts,
        }
    return contracts


async def _load_latest_quotes(
    conn: aiosqlite.Connection,
    market_ids: list[str],
    freshness_seconds: int,
    now_ts: int,
) -> dict[str, dict[str, Any]]:
    if not market_ids:
        return {}
    placeholders = ",".join("?" for _ in market_ids)
    threshold = now_ts - freshness_seconds
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
        [threshold, *market_ids],
    )
    rows = await cursor.fetchall()
    latest: dict[str, dict[str, Any]] = {}
    for market_id, ts, yes_bid, yes_ask, no_bid, no_ask in rows:
        latest[market_id] = {
            "market_id": market_id,
            "ts": ts,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
        }
    return latest


async def _latest_quote_ts(
    conn: aiosqlite.Connection, market_ids: list[str]
) -> int | None:
    if not market_ids:
        return None
    placeholders = ",".join("?" for _ in market_ids)
    cursor = await conn.execute(
        f"SELECT MAX(ts) FROM kalshi_quotes WHERE market_id IN ({placeholders})",
        market_ids,
    )
    row = await cursor.fetchone()
    if row and row[0] is not None:
        return int(row[0])
    return None


async def _count_recent_quote_markets(
    conn: aiosqlite.Connection, market_ids: list[str], threshold: int
) -> int:
    if not market_ids:
        return 0
    placeholders = ",".join("?" for _ in market_ids)
    cursor = await conn.execute(
        f"SELECT COUNT(DISTINCT market_id) FROM kalshi_quotes "
        f"WHERE ts >= ? AND market_id IN ({placeholders})",
        [threshold, *market_ids],
    )
    row = await cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


async def compute_edges(
    conn: aiosqlite.Connection,
    *,
    product_id: str,
    lookback_seconds: int,
    max_spot_points: int,
    ewma_lambda: float,
    min_points: int,
    sigma_floor: float,
    sigma_cap: float,
    status: str | None,
    series: list[str] | None,
    pct_band: float,
    top_n: int,
    freshness_seconds: int,
    max_horizon_seconds: int,
    contracts: int = 1,
    fee_fn: FeeFn | None = None,
    now_ts: int | None = None,
    debug_market_ids: list[str] | None = None,
) -> dict[str, Any]:
    now_ts = now_ts or int(time.time())
    series = normalize_series(series)
    fee_fn = fee_fn or taker_fee_dollars
    grace_seconds = 3600
    logger = logging.getLogger(__name__)
    debug_set = set(debug_market_ids or [])
    debug_logged: set[str] = set()

    latest = await get_latest_spot(conn, product_id)
    if latest is None:
        return {
            "error": "spot_missing",
            "edges_inserted": 0,
            "relevant_total": 0,
        }
    spot_ts, spot_price = latest

    history = await get_spot_history(
        conn, product_id, lookback_seconds, max_spot_points
    )
    timestamps = [row[0] for row in history]
    prices = [row[1] for row in history]
    step_seconds = _estimate_step_seconds(timestamps)
    if step_seconds is None:
        return {
            "error": "spot_step_missing",
            "edges_inserted": 0,
            "relevant_total": 0,
        }

    returns = compute_log_returns(prices)
    if len(returns) < min_points:
        return {
            "error": "sigma_missing",
            "edges_inserted": 0,
            "relevant_total": 0,
        }
    vol_step = ewma_volatility(returns, ewma_lambda)
    if vol_step is None:
        return {
            "error": "sigma_missing",
            "edges_inserted": 0,
            "relevant_total": 0,
        }
    sigma_unclamped = annualize_vol(vol_step, step_seconds)
    if sigma_floor > sigma_cap:
        raise ValueError("sigma_floor must be <= sigma_cap")
    sigma = max(sigma_floor, min(sigma_cap, sigma_unclamped))
    sigma_clamped = abs(sigma - sigma_unclamped) > 1e-12

    relevant_ids, _, selection = await get_relevant_universe(
        conn,
        pct_band=pct_band,
        top_n=top_n,
        status=status,
        series=series,
        product_id=product_id,
        spot_price=spot_price,
    )
    if not relevant_ids:
        return {
            "error": "no_relevant_markets",
            "edges_inserted": 0,
            "relevant_total": 0,
        }

    contract_map = await _load_contracts(conn, relevant_ids)
    quotes = await _load_latest_quotes(
        conn, relevant_ids, freshness_seconds, now_ts
    )
    latest_quote_ts = await _latest_quote_ts(conn, relevant_ids)
    quote_age_seconds = (
        (now_ts - latest_quote_ts) if latest_quote_ts is not None else None
    )
    cutoff = now_ts - freshness_seconds
    quotes_distinct_markets_recent = await _count_recent_quote_markets(
        conn, relevant_ids, cutoff
    )

    inputs = EdgeInputs(
        spot_price=spot_price,
        spot_ts=spot_ts,
        sigma_annualized=sigma,
        now_ts=now_ts,
    )

    dao = Dao(conn)
    inserted = 0
    skipped = 0
    skip_reasons: dict[str, int] = {}
    missing_quote_sample: list[str] = []
    missing_quote_set: set[str] = set()

    for market_id in relevant_ids:
        contract = contract_map.get(market_id)
        if contract is None:
            skip_reasons["missing_contract"] = skip_reasons.get(
                "missing_contract", 0
            ) + 1
            skipped += 1
            continue
        settlement_ts = _safe_int(contract.get("settlement_ts"))  # close time
        if settlement_ts is None:
            skip_reasons["missing_settlement_ts"] = skip_reasons.get(
                "missing_settlement_ts", 0
            ) + 1
            skipped += 1
            continue
        horizon_raw = settlement_ts - now_ts
        if horizon_raw < -5:
            skip_reasons["expired_contract"] = skip_reasons.get(
                "expired_contract", 0
            ) + 1
            skipped += 1
            continue
        if horizon_raw > max_horizon_seconds + grace_seconds:
            skip_reasons["horizon_out_of_range"] = skip_reasons.get(
                "horizon_out_of_range", 0
            ) + 1
            skipped += 1
            continue
        horizon_seconds = max(horizon_raw, 0)
        if market_id in debug_set and market_id not in debug_logged:
            debug_logged.add(market_id)
            strike_type = contract.get("strike_type")
            lower = _safe_float(contract.get("lower"))
            upper = _safe_float(contract.get("upper"))
            debug_info: dict[str, Any] = {
                "market_id": market_id,
                "strike_type": strike_type,
                "lower": lower,
                "upper": upper,
                "spot_price": inputs.spot_price,
                "sigma_annualized": sigma,
                "sigma_unclamped": sigma_unclamped,
                "sigma_clamped": sigma_clamped,
                "now_ts": now_ts,
                "spot_ts": inputs.spot_ts,
                "settlement_ts": settlement_ts,
                "horizon_seconds": horizon_seconds,
            }
            if strike_type == "between":
                z_lower = (
                    _z_score(inputs.spot_price, lower, horizon_seconds, sigma)
                    if lower is not None
                    else None
                )
                z_upper = (
                    _z_score(inputs.spot_price, upper, horizon_seconds, sigma)
                    if upper is not None
                    else None
                )
                cdf_lower = _norm_cdf(z_lower) if z_lower is not None else None
                cdf_upper = _norm_cdf(z_upper) if z_upper is not None else None
                cdf_diff = (
                    (cdf_upper - cdf_lower)
                    if cdf_upper is not None and cdf_lower is not None
                    else None
                )
                debug_info.update(
                    {
                        "z_lower": z_lower,
                        "z_upper": z_upper,
                        "cdf_lower": cdf_lower,
                        "cdf_upper": cdf_upper,
                        "cdf_diff": cdf_diff,
                    }
                )
            elif strike_type == "less":
                z = (
                    _z_score(inputs.spot_price, upper, horizon_seconds, sigma)
                    if upper is not None
                    else None
                )
                debug_info.update(
                    {"z": z, "cdf": _norm_cdf(z) if z is not None else None}
                )
            elif strike_type == "greater":
                z = (
                    _z_score(inputs.spot_price, lower, horizon_seconds, sigma)
                    if lower is not None
                    else None
                )
                debug_info.update(
                    {"z": z, "cdf": _norm_cdf(z) if z is not None else None}
                )
            logger.info("edge_debug_market", extra=debug_info)
        quote = quotes.get(market_id)
        if quote is None:
            skip_reasons["missing_quote"] = skip_reasons.get(
                "missing_quote", 0
            ) + 1
            add_failed_sample(
                missing_quote_sample, missing_quote_set, market_id
            )
            skipped += 1
            continue
        yes_ask = _safe_float(quote.get("yes_ask"))
        no_ask = _safe_float(quote.get("no_ask"))
        yes_missing = yes_ask is None or yes_ask < 0 or yes_ask > 100
        no_missing = no_ask is None or no_ask < 0 or no_ask > 100
        if yes_missing and no_missing:
            skip_reasons["missing_both_sides"] = skip_reasons.get(
                "missing_both_sides", 0
            ) + 1
            skipped += 1
            continue
        if yes_missing:
            skip_reasons["missing_yes_ask"] = skip_reasons.get(
                "missing_yes_ask", 0
            ) + 1
        if no_missing:
            skip_reasons["missing_no_ask"] = skip_reasons.get(
                "missing_no_ask", 0
            ) + 1
        if yes_missing:
            quote["yes_ask"] = None
        if no_missing:
            quote["no_ask"] = None
        edge = compute_edge_for_market(
            contract,
            quote,
            inputs,
            fee_fn,
            horizon_seconds=horizon_seconds,
            contracts=contracts,
        )
        if edge is None:
            skip_reasons["invalid_edge"] = skip_reasons.get(
                "invalid_edge", 0
            ) + 1
            skipped += 1
            continue

        row = {
            "ts": edge.ts,
            "market_id": edge.market_id,
            "settlement_ts": edge.settlement_ts,
            "horizon_seconds": edge.horizon_seconds,
            "spot_price": edge.spot_price,
            "sigma_annualized": edge.sigma_annualized,
            "prob_yes": edge.prob_yes,
            "yes_bid": edge.yes_bid,
            "yes_ask": edge.yes_ask,
            "no_bid": edge.no_bid,
            "no_ask": edge.no_ask,
            "ev_take_yes": edge.ev_take_yes,
            "ev_take_no": edge.ev_take_no,
            "raw_json": edge.raw_json,
        }
        await dao.insert_kalshi_edge(row)
        inserted += 1

    await conn.commit()

    return {
        "now_ts": now_ts,
        "spot_price": spot_price,
        "spot_ts": spot_ts,
        "sigma_annualized": sigma,
        "sigma_unclamped": sigma_unclamped,
        "sigma_clamped": sigma_clamped,
        "step_seconds": step_seconds,
        "selection": selection,
        "relevant_total": len(relevant_ids),
        "edges_inserted": inserted,
        "skipped": skipped,
        "skip_reasons": skip_reasons,
        "latest_quote_ts": latest_quote_ts,
        "quote_age_seconds": quote_age_seconds,
        "quotes_distinct_markets_recent": quotes_distinct_markets_recent,
        "relevant_with_recent_quotes": len(quotes),
        "missing_quote_sample": missing_quote_sample,
        "recent_quote_market_ids_sample": list(quotes.keys())[:5],
        "relevant_ids_sample": relevant_ids[:5],
    }
