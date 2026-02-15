"""Compute EV edges for Kalshi markets from DB inputs."""

from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass
from typing import Any, Callable

import aiosqlite
import sqlite3

from kalshi_bot.data.dao import Dao
from kalshi_bot.data.spot_dao import get_latest_spot, get_spot_history
from kalshi_bot.kalshi.health import get_relevant_universe
from kalshi_bot.kalshi.market_filters import normalize_series
from kalshi_bot.kalshi.error_utils import add_failed_sample
from kalshi_bot.kalshi.fees import taker_fee_dollars
from kalshi_bot.models.probability import (
    SECONDS_PER_YEAR,
    prob_between,
    prob_between_raw,
    prob_greater_equal,
    prob_greater_equal_raw,
    prob_less_equal,
    prob_less_equal_raw,
)
from kalshi_bot.models.volatility import (
    annualize_vol,
    compute_log_returns,
    ewma_volatility,
    resample_last_price_series,
)
from kalshi_bot.strategy.edge_math import ev_take_no, ev_take_yes

FeeFn = Callable[[float | None, int], float | None]
SIGMA_HISTORY_WARNING_INTERVAL_SECONDS = 15 * 60
_SIGMA_THROTTLED_REASONS = {"insufficient_history_span", "insufficient_points"}
_LAST_SIGMA_WARNING_TS_BY_REASON: dict[str, int] = {}
_LAST_SIGMA_PERSIST_WARNING_TS: int | None = None


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
    settlement_ts: int | None  # Market close time used for horizon.
    horizon_seconds: int | None  # Horizon from spot_ts to close_ts.
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
    min_lookback_seconds: int
    sigma_default: float
    sigma_max: float


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


def prob_yes_for_contract_raw(
    spot: float,
    sigma: float,
    horizon_seconds: int,
    contract_row: dict[str, Any],
) -> float | None:
    """Return unclamped YES probability for Kalshi strike/range contracts."""
    strike_type = contract_row.get("strike_type")
    lower = _safe_float(contract_row.get("lower"))
    upper = _safe_float(contract_row.get("upper"))

    if strike_type == "less":
        if upper is None or upper <= 0:
            return None
        return prob_less_equal_raw(spot, upper, horizon_seconds, sigma)
    if strike_type == "greater":
        if lower is None or lower <= 0:
            return None
        return prob_greater_equal_raw(spot, lower, horizon_seconds, sigma)
    if strike_type == "between":
        if lower is None or upper is None:
            return None
        if lower <= 0 or upper <= 0 or upper <= lower:
            return None
        return prob_between_raw(spot, lower, upper, horizon_seconds, sigma)
    return None


def compute_edge_for_market(
    contract: dict[str, Any],
    quote: dict[str, Any],
    inputs: EdgeInputs,
    fee_fn: FeeFn,
    horizon_seconds: int,
    selection_horizon_seconds: int | None = None,
    contracts: int = 1,
) -> EdgeRow | None:
    market_id = contract.get("ticker") or contract.get("market_id")
    if not market_id or not isinstance(market_id, str):
        return None

    settlement_ts = (
        _safe_int(contract.get("close_ts"))
        or _safe_int(contract.get("expected_expiration_ts"))
        or _safe_int(contract.get("settlement_ts"))
    )
    if settlement_ts is None:
        return None

    if horizon_seconds < 0:
        horizon_seconds = 0

    raw_prob_yes = prob_yes_for_contract_raw(
        inputs.spot_price,
        inputs.sigma_annualized,
        horizon_seconds,
        contract,
    )
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

    yes_boundary = yes_ask in (0.0, 100.0)
    no_boundary = no_ask in (0.0, 100.0)
    crossed_market = (
        yes_ask is not None
        and no_ask is not None
        and 0.0 <= yes_ask <= 100.0
        and 0.0 <= no_ask <= 100.0
        and (yes_ask + no_ask) < 100.0
    )
    metadata = {
        "quote_ts": quote.get("ts"),
        "strike_type": contract.get("strike_type"),
        "lower": contract.get("lower"),
        "upper": contract.get("upper"),
        "prob_horizon_seconds": horizon_seconds,
        "selection_horizon_seconds": selection_horizon_seconds,
        "prob_yes_raw": raw_prob_yes,
        "prob_yes_clamped": prob_yes,
        "yes_ask_boundary": yes_boundary,
        "no_ask_boundary": no_boundary,
        "crossed_market": crossed_market,
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


def _sigma_reason_context(
    sigma_reason: str | None,
    *,
    history_span_seconds: int,
    min_sigma_lookback_seconds: int,
    sigma_points_used: int,
    min_points: int,
    sigma_raw: float | None,
    sigma_max: float,
    step_seconds: float,
) -> str | None:
    if sigma_reason == "insufficient_history_span":
        return (
            f"history_span_seconds={history_span_seconds} "
            f"< min_sigma_lookback_seconds={min_sigma_lookback_seconds}"
        )
    if sigma_reason == "insufficient_points":
        return (
            f"sigma_points_used={sigma_points_used} "
            f"< min_points={min_points}"
        )
    if sigma_reason == "out_of_bounds":
        return f"sigma_unclamped={sigma_raw} > sigma_max={sigma_max}"
    if sigma_reason == "nonfinite_sigma":
        return f"sigma_unclamped={sigma_raw} is not finite"
    if sigma_reason == "nonpositive_sigma":
        return f"sigma_unclamped={sigma_raw} <= 0"
    if sigma_reason == "sigma_ewma_missing":
        return "ewma_volatility returned no estimate"
    if sigma_reason == "bad_step_seconds":
        return f"step_seconds={step_seconds} outside [1, 3600]"
    if sigma_reason == "missing_step":
        return "resample step is missing or invalid"
    if sigma_reason == "sigma_missing":
        return "sigma estimate unexpectedly missing"
    return None


def _should_log_sigma_warning(reason: str | None, now_ts: int) -> bool:
    if reason is None:
        return True
    if reason not in _SIGMA_THROTTLED_REASONS:
        return True
    last_ts = _LAST_SIGMA_WARNING_TS_BY_REASON.get(reason)
    if (
        last_ts is None
        or (now_ts - last_ts) >= SIGMA_HISTORY_WARNING_INTERVAL_SECONDS
    ):
        _LAST_SIGMA_WARNING_TS_BY_REASON[reason] = now_ts
        return True
    return False


def _should_log_sigma_persist_warning(now_ts: int) -> bool:
    global _LAST_SIGMA_PERSIST_WARNING_TS
    if (
        _LAST_SIGMA_PERSIST_WARNING_TS is None
        or (now_ts - _LAST_SIGMA_PERSIST_WARNING_TS)
        >= SIGMA_HISTORY_WARNING_INTERVAL_SECONDS
    ):
        _LAST_SIGMA_PERSIST_WARNING_TS = now_ts
        return True
    return False


async def _load_contracts(
    conn: aiosqlite.Connection, market_ids: list[str]
) -> dict[str, dict[str, Any]]:
    if not market_ids:
        return {}
    placeholders = ",".join("?" for _ in market_ids)
    sql = (
        f"SELECT ticker, lower, upper, strike_type, settlement_ts, close_ts, expected_expiration_ts, expiration_ts "
        f"FROM kalshi_contracts WHERE ticker IN ({placeholders})"
    )
    cursor = await conn.execute(
        sql,
        market_ids,
    )
    rows = await cursor.fetchall()
    contracts: dict[str, dict[str, Any]] = {}
    for (
        ticker,
        lower,
        upper,
        strike_type,
        settlement_ts,
        close_ts,
        expected_expiration_ts,
        expiration_ts,
    ) in rows:
        contracts[ticker] = {
            "ticker": ticker,
            "lower": lower,
            "upper": upper,
            "strike_type": strike_type,
            "settlement_ts": settlement_ts,
            "close_ts": close_ts,
            "expected_expiration_ts": expected_expiration_ts,
            "expiration_ts": expiration_ts,
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
    sql = f"""  # nosec B608
        WITH latest AS (
            SELECT market_id, MAX(ts) AS max_ts
            FROM kalshi_quotes
            WHERE ts >= ? AND market_id IN ({placeholders})
            GROUP BY market_id
        )
        SELECT q.market_id, q.ts, q.yes_bid, q.yes_ask, q.no_bid, q.no_ask
        FROM kalshi_quotes q
        JOIN latest l ON q.market_id = l.market_id AND q.ts = l.max_ts
        """
    cursor = await conn.execute(
        sql,
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
        f"SELECT MAX(ts) FROM kalshi_quotes WHERE market_id IN ({placeholders})",  # nosec B608
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
        f"SELECT COUNT(DISTINCT market_id) FROM kalshi_quotes "  # nosec B608
        f"WHERE ts >= ? AND market_id IN ({placeholders})",
        [threshold, *market_ids],
    )
    row = await cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


async def _load_market_titles(
    conn: aiosqlite.Connection, market_ids: list[str]
) -> dict[str, str | None]:
    if not market_ids:
        return {}
    placeholders = ",".join("?" for _ in market_ids)
    cursor = await conn.execute(
        f"SELECT market_id, title FROM kalshi_markets WHERE market_id IN ({placeholders})",  # nosec B608
        market_ids,
    )
    rows = await cursor.fetchall()
    return {market_id: title for market_id, title in rows}


async def _load_sigma_fallback(
    conn: aiosqlite.Connection, product_id: str
) -> float | None:
    try:
        dao = Dao(conn)
        value = await dao.get_latest_sigma(product_id)
    except sqlite3.OperationalError:
        return None
    if value is None:
        return None
    if not math.isfinite(value) or value <= 0:
        return None
    return value


async def _record_sigma(
    conn: aiosqlite.Connection,
    product_id: str,
    ts: int,
    sigma: float,
    method: str,
    lookback_seconds: int | None,
    points: int | None,
) -> bool:
    try:
        dao = Dao(conn)
        await dao.insert_sigma_history(
            {
                "ts": ts,
                "product_id": product_id,
                "sigma": sigma,
                "source": method or "unknown",
                "reason": None,
                "method": method,
                "lookback_seconds": lookback_seconds,
                "points": points,
            }
        )
        return True
    except sqlite3.OperationalError:
        try:
            await conn.rollback()
        except sqlite3.OperationalError:
            pass
        return False


async def _compute_and_store_sigma(
    conn: aiosqlite.Connection,
    *,
    product_id: str,
    lookback_seconds: int,
    max_spot_points: int,
    ewma_lambda: float,
    min_points: int,
    min_sigma_lookback_seconds: int,
    resample_seconds: int,
    sigma_default: float,
    sigma_max: float,
    now_ts: int,
    logger: logging.Logger,
) -> dict[str, Any]:
    if not isinstance(resample_seconds, int) or resample_seconds <= 0:
        raise ValueError("resample_seconds must be a positive int")
    if not math.isfinite(sigma_default) or sigma_default <= 0:
        raise ValueError("sigma_default must be positive and finite")
    if not math.isfinite(sigma_max) or sigma_max <= 0:
        raise ValueError("sigma_max must be positive and finite")

    effective_max_spot_points = max(1, int(max_spot_points))
    max_auto_points = 200_000
    auto_load_attempts = 0
    max_auto_load_attempts = 6

    raw_timestamps: list[int] = []
    raw_prices: list[float] = []
    resampled_timestamps: list[int] = []
    resampled_prices: list[float] = []
    returns: list[float] = []
    history_span_seconds = 0
    sigma_reason: str | None = None
    sigma_raw: float | None = None
    sigma_persisted: bool | None = None

    while True:
        history = await get_spot_history(
            conn, product_id, lookback_seconds, effective_max_spot_points
        )
        raw_timestamps = [row[0] for row in history]
        raw_prices = [row[1] for row in history]
        (
            resampled_timestamps,
            resampled_prices,
        ) = resample_last_price_series(
            raw_timestamps, raw_prices, resample_seconds
        )
        returns = compute_log_returns(resampled_prices)
        history_span_seconds = (
            (resampled_timestamps[-1] - resampled_timestamps[0])
            if len(resampled_timestamps) >= 2
            else 0
        )

        sigma_reason = None
        sigma_raw = None
        step_seconds = float(resample_seconds)

        if step_seconds <= 0:
            sigma_reason = "missing_step"
        elif step_seconds < 1.0 or step_seconds > 3600.0:
            sigma_reason = "bad_step_seconds"
        elif len(resampled_prices) < 2:
            sigma_reason = "insufficient_points"
        elif history_span_seconds < min_sigma_lookback_seconds:
            sigma_reason = "insufficient_history_span"
        elif len(returns) < min_points:
            sigma_reason = "insufficient_points"
        else:
            vol_step = ewma_volatility(returns, ewma_lambda)
            if vol_step is None:
                sigma_reason = "sigma_ewma_missing"
            else:
                sigma_raw = annualize_vol(vol_step, step_seconds)
                if not math.isfinite(sigma_raw):
                    sigma_reason = "nonfinite_sigma"
                elif sigma_raw <= 0:
                    sigma_reason = "nonpositive_sigma"
                elif sigma_raw > sigma_max:
                    sigma_reason = "out_of_bounds"

        # If we only failed because the span is too short and we likely hit a
        # row cap, fetch more history points and try once more.
        if (
            sigma_reason == "insufficient_history_span"
            and auto_load_attempts < max_auto_load_attempts
            and len(raw_timestamps) >= effective_max_spot_points
            and effective_max_spot_points < max_auto_points
        ):
            estimated_step = _estimate_step_seconds(raw_timestamps)
            if estimated_step is None or estimated_step <= 0:
                estimated_step = 1.0
            target_span = max(min_sigma_lookback_seconds, resample_seconds * min_points)
            needed_points = int((target_span / estimated_step) * 1.25) + 5
            needed_points = max(needed_points, effective_max_spot_points * 2)
            new_max_points = min(max_auto_points, needed_points)
            if new_max_points > effective_max_spot_points:
                effective_max_spot_points = new_max_points
                auto_load_attempts += 1
                continue

        break

    raw_points = len(raw_timestamps)
    resampled_points = len(resampled_timestamps)
    step_seconds = float(resample_seconds)
    sigma_points_used = len(returns)
    sigma_lookback_seconds_used = int(history_span_seconds)

    if sigma_reason is None and sigma_raw is None:
        sigma_reason = "sigma_missing"

    if sigma_reason is None and sigma_raw is not None:
        sigma = sigma_raw
        sigma_source = "ewma"
        sigma_ok = True
        persisted = await _record_sigma(
            conn,
            product_id=product_id,
            ts=now_ts,
            sigma=sigma,
            method="ewma",
            lookback_seconds=sigma_lookback_seconds_used,
            points=sigma_points_used,
        )
        sigma_persisted = persisted
        if not persisted and _should_log_sigma_persist_warning(now_ts):
            logger.warning(
                "sigma_persist_failed",
                extra={
                    "product_id": product_id,
                    "sigma_source": sigma_source,
                    "sigma_reason": sigma_reason,
                    "raw_points": raw_points,
                    "resampled_points": resampled_points,
                    "history_span_seconds": history_span_seconds,
                    "sigma_value": sigma,
                },
            )
    else:
        sigma_reason_context = _sigma_reason_context(
            sigma_reason,
            history_span_seconds=history_span_seconds,
            min_sigma_lookback_seconds=min_sigma_lookback_seconds,
            sigma_points_used=sigma_points_used,
            min_points=min_points,
            sigma_raw=sigma_raw,
            sigma_max=sigma_max,
            step_seconds=step_seconds,
        )
        should_log_warning = _should_log_sigma_warning(sigma_reason, now_ts)
        if should_log_warning:
            logger.warning(
                "sigma_compute_failed reason=%s context=%s",
                sigma_reason,
                sigma_reason_context,
                extra={
                    "product_id": product_id,
                    "sigma_reason": sigma_reason,
                    "sigma_reason_context": sigma_reason_context,
                    "raw_points": raw_points,
                    "resampled_points": resampled_points,
                    "history_span_seconds": history_span_seconds,
                    "sigma_points_used": sigma_points_used,
                    "min_points": min_points,
                    "min_sigma_lookback_seconds": min_sigma_lookback_seconds,
                },
            )
        sigma_fallback = await _load_sigma_fallback(conn, product_id)
        if sigma_fallback is not None:
            sigma = sigma_fallback
            sigma_source = "history"
            sigma_ok = False
            if should_log_warning:
                logger.warning(
                    "sigma_fallback_used reason=%s context=%s",
                    sigma_reason,
                    sigma_reason_context,
                    extra={
                        "product_id": product_id,
                        "sigma_reason": sigma_reason,
                        "sigma_reason_context": sigma_reason_context,
                        "sigma_fallback": sigma_fallback,
                        "raw_points": raw_points,
                        "resampled_points": resampled_points,
                        "history_span_seconds": history_span_seconds,
                        "sigma_points_used": sigma_points_used,
                        "min_points": min_points,
                        "min_sigma_lookback_seconds": min_sigma_lookback_seconds,
                    },
                )
            sigma_persisted = False
        else:
            sigma = sigma_default
            sigma_source = "default"
            sigma_ok = False
            if should_log_warning:
                logger.warning(
                    "sigma_default_used reason=%s context=%s",
                    sigma_reason,
                    sigma_reason_context,
                    extra={
                        "product_id": product_id,
                        "sigma_reason": sigma_reason,
                        "sigma_reason_context": sigma_reason_context,
                        "sigma_default": sigma_default,
                        "raw_points": raw_points,
                        "resampled_points": resampled_points,
                        "history_span_seconds": history_span_seconds,
                        "sigma_points_used": sigma_points_used,
                        "min_points": min_points,
                        "min_sigma_lookback_seconds": min_sigma_lookback_seconds,
                        "fallback_history_found": False,
                    },
                )
            sigma_persisted = False

    if sigma_reason is None:
        sigma_reason_context = None

    if sigma_reason is None:
        sigma_quality = "ok"
    elif sigma_source == "history":
        sigma_quality = "fallback_history"
    else:
        sigma_quality = "fallback_default"

    return {
        "sigma": sigma,
        "sigma_unclamped": sigma_raw,
        "sigma_source": sigma_source,
        "sigma_ok": sigma_ok,
        "sigma_reason": sigma_reason,
        "sigma_reason_context": sigma_reason_context,
        "sigma_quality": sigma_quality,
        "sigma_points_used": sigma_points_used,
        "sigma_lookback_seconds_used": sigma_lookback_seconds_used,
        "sigma_persisted": sigma_persisted,
        "raw_points": raw_points,
        "resampled_points": resampled_points,
        "step_seconds": step_seconds,
    }


async def compute_edges(
    conn: aiosqlite.Connection,
    *,
    product_id: str,
    lookback_seconds: int,
    max_spot_points: int,
    ewma_lambda: float,
    min_points: int,
    min_sigma_lookback_seconds: int = 3600,
    resample_seconds: int = 5,
    sigma_default: float,
    sigma_max: float,
    status: str | None,
    series: list[str] | None,
    pct_band: float,
    top_n: int,
    freshness_seconds: int,
    max_horizon_seconds: int,
    contracts: int = 1,
    fee_fn: FeeFn | None = None,
    now_ts: int | None = None,
    require_quotes: bool = True,
    min_ask_cents: float = 1.0,
    max_ask_cents: float = 99.0,
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
            "skip_reasons": {},
        }
    spot_ts, spot_price = latest

    sigma_state = await _compute_and_store_sigma(
        conn,
        product_id=product_id,
        lookback_seconds=lookback_seconds,
        max_spot_points=max_spot_points,
        ewma_lambda=ewma_lambda,
        min_points=min_points,
        min_sigma_lookback_seconds=min_sigma_lookback_seconds,
        resample_seconds=resample_seconds,
        sigma_default=sigma_default,
        sigma_max=sigma_max,
        now_ts=now_ts,
        logger=logger,
    )
    sigma = sigma_state["sigma"]
    sigma_raw = sigma_state["sigma_unclamped"]
    sigma_source = sigma_state["sigma_source"]
    sigma_ok = sigma_state["sigma_ok"]
    sigma_reason = sigma_state["sigma_reason"]
    sigma_quality = sigma_state["sigma_quality"]
    sigma_reason_context = sigma_state["sigma_reason_context"]
    sigma_points_used = sigma_state["sigma_points_used"]
    sigma_lookback_seconds_used = sigma_state["sigma_lookback_seconds_used"]
    sigma_persisted = sigma_state["sigma_persisted"]
    raw_points = sigma_state["raw_points"]
    resampled_points = sigma_state["resampled_points"]
    step_seconds = sigma_state["step_seconds"]

    relevant_ids, _, selection = await get_relevant_universe(
        conn,
        pct_band=pct_band,
        top_n=top_n,
        status=status,
        series=series,
        product_id=product_id,
        spot_price=spot_price,
        spot_ts=spot_ts,
        now_ts=now_ts,
        freshness_seconds=freshness_seconds,
        max_horizon_seconds=max_horizon_seconds,
        grace_seconds=grace_seconds,
        require_quotes=require_quotes,
        min_ask_cents=min_ask_cents,
        max_ask_cents=max_ask_cents,
    )
    if not relevant_ids:
        no_relevant_skip_reasons: dict[str, int] = {}
        if selection:
            expired = selection.get("excluded_expired")
            if expired:
                no_relevant_skip_reasons["expired_contract"] = int(expired)
            horizon = selection.get("excluded_horizon_out_of_range")
            if horizon:
                no_relevant_skip_reasons["horizon_out_of_range"] = int(horizon)
            missing_bounds = selection.get("excluded_missing_bounds")
            if missing_bounds:
                no_relevant_skip_reasons["missing_bounds"] = int(missing_bounds)
            missing_close = selection.get("excluded_missing_close_ts")
            if missing_close:
                no_relevant_skip_reasons["missing_settlement_ts"] = int(missing_close)
            missing_quote = selection.get("excluded_missing_recent_quote")
            if missing_quote:
                no_relevant_skip_reasons["missing_quote"] = int(missing_quote)
            untradable = selection.get("excluded_untradable")
            if untradable:
                no_relevant_skip_reasons["missing_both_sides"] = int(untradable)
        # Ensure we do not leave a pending transaction from sigma persistence
        # when returning early on an empty universe.
        try:
            await conn.commit()
        except sqlite3.OperationalError:
            try:
                await conn.rollback()
            except sqlite3.OperationalError:
                pass
        return {
            "error": "no_relevant_markets",
            "edges_inserted": 0,
            "relevant_total": 0,
            "selection": selection,
            "skip_reasons": no_relevant_skip_reasons,
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
    if len(relevant_ids) <= 200:
        title_ids = relevant_ids
    else:
        title_ids = list(dict.fromkeys(relevant_ids[:5] + list(debug_set)))
    titles_sample = await _load_market_titles(conn, title_ids)

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
    diagnostics: dict[str, int] = {}
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
        settlement_ts = (
            _safe_int(contract.get("close_ts"))
            or _safe_int(contract.get("expected_expiration_ts"))
            or _safe_int(contract.get("settlement_ts"))
        )  # Close/decision timestamp for horizon.
        if settlement_ts is None:
            skip_reasons["missing_settlement_ts"] = skip_reasons.get(
                "missing_settlement_ts", 0
            ) + 1
            skipped += 1
            continue
        selection_horizon_raw = settlement_ts - now_ts
        if selection_horizon_raw < -5:
            skip_reasons["expired_contract"] = skip_reasons.get(
                "expired_contract", 0
            ) + 1
            skipped += 1
            continue
        if selection_horizon_raw > max_horizon_seconds + grace_seconds:
            skip_reasons["horizon_out_of_range"] = skip_reasons.get(
                "horizon_out_of_range", 0
            ) + 1
            skipped += 1
            continue
        horizon_seconds = max(selection_horizon_raw, 0)
        prob_horizon_raw = settlement_ts - inputs.spot_ts
        prob_horizon_seconds = max(prob_horizon_raw, 0)
        if market_id in debug_set and market_id not in debug_logged:
            debug_logged.add(market_id)
            strike_type = contract.get("strike_type")
            lower = _safe_float(contract.get("lower"))
            upper = _safe_float(contract.get("upper"))
            debug_info: dict[str, Any] = {
                "market_id": market_id,
                "title": titles_sample.get(market_id),
                "strike_type": strike_type,
                "lower": lower,
                "upper": upper,
                "spot_price": inputs.spot_price,
                "sigma_annualized": sigma,
                "sigma_unclamped": sigma_raw,
                "sigma_source": sigma_source,
                "sigma_ok": sigma_ok,
                "sigma_reason": sigma_reason,
                "sigma_points_used": sigma_points_used,
                "sigma_lookback_seconds_used": sigma_lookback_seconds_used,
                "resample_seconds": resample_seconds,
                "raw_points": raw_points,
                "resampled_points": resampled_points,
                "step_seconds": step_seconds,
                "now_ts": now_ts,
                "spot_ts": inputs.spot_ts,
                "expected_expiration_ts": contract.get("expected_expiration_ts"),
                "settlement_ts": settlement_ts,
                "prob_horizon_seconds": prob_horizon_seconds,
                "selection_horizon_seconds": horizon_seconds,
            }
            raw_prob = prob_yes_for_contract_raw(
                inputs.spot_price,
                sigma,
                prob_horizon_seconds,
                contract,
            )
            clamped_prob = prob_yes_for_contract(
                inputs.spot_price,
                sigma,
                prob_horizon_seconds,
                contract,
            )
            debug_info["prob_yes_raw"] = raw_prob
            debug_info["prob_yes_clamped"] = clamped_prob
            if strike_type == "between":
                z_lower = (
                    _z_score(
                        inputs.spot_price, lower, prob_horizon_seconds, sigma
                    )
                    if lower is not None
                    else None
                )
                z_upper = (
                    _z_score(
                        inputs.spot_price, upper, prob_horizon_seconds, sigma
                    )
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
                    _z_score(
                        inputs.spot_price, upper, prob_horizon_seconds, sigma
                    )
                    if upper is not None
                    else None
                )
                debug_info.update(
                    {"z": z, "cdf": _norm_cdf(z) if z is not None else None}
                )
            elif strike_type == "greater":
                z = (
                    _z_score(
                        inputs.spot_price, lower, prob_horizon_seconds, sigma
                    )
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

        def _valid_ask(value: float | None) -> bool:
            return value is not None and 0.0 <= value <= 100.0

        def _is_boundary(value: float | None) -> bool:
            return value in (0.0, 100.0)

        def _tradable_ask(value: float | None) -> bool:
            if value is None or not _valid_ask(value):
                return False
            if _is_boundary(value):
                return True
            return min_ask_cents <= value <= max_ask_cents

        yes_tradable = _tradable_ask(yes_ask)
        no_tradable = _tradable_ask(no_ask)
        yes_missing = not yes_tradable
        no_missing = not no_tradable

        crossed_market = False
        if _valid_ask(yes_ask) and _valid_ask(no_ask):
            yes_ask_val = yes_ask
            no_ask_val = no_ask
            if yes_ask_val is not None and no_ask_val is not None:
                crossed_market = (yes_ask_val + no_ask_val) < 100.0
        if crossed_market:
            skip_reasons["crossed_market"] = skip_reasons.get(
                "crossed_market", 0
            ) + 1
            diagnostics["crossed_market"] = diagnostics.get(
                "crossed_market", 0
            ) + 1
            skipped += 1
            continue
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
            horizon_seconds=prob_horizon_seconds,
            selection_horizon_seconds=horizon_seconds,
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
        "sigma_unclamped": sigma_raw,
        "sigma_source": sigma_source,
        "sigma_ok": sigma_ok,
        "sigma_reason": sigma_reason,
        "sigma_reason_context": sigma_reason_context,
        "sigma_quality": sigma_quality,
        "sigma_points_used": sigma_points_used,
        "sigma_lookback_seconds_used": sigma_lookback_seconds_used,
        "sigma_persisted": sigma_persisted,
        "min_sigma_points": min_points,
        "min_sigma_lookback_seconds": min_sigma_lookback_seconds,
        "resample_seconds": resample_seconds,
        "raw_points": raw_points,
        "resampled_points": resampled_points,
        "step_seconds": step_seconds,
        "selection": selection,
        "relevant_total": len(relevant_ids),
        "edges_inserted": inserted,
        "skipped": skipped,
        "skip_reasons": skip_reasons,
        "diagnostics": diagnostics,
        "latest_quote_ts": latest_quote_ts,
        "quote_age_seconds": quote_age_seconds,
        "quotes_distinct_markets_recent": quotes_distinct_markets_recent,
        "relevant_with_recent_quotes": len(quotes),
        "missing_quote_sample": missing_quote_sample,
        "recent_quote_market_ids_sample": list(quotes.keys())[:5],
        "relevant_ids_sample": relevant_ids[:5],
        "relevant_titles_sample": titles_sample,
    }
