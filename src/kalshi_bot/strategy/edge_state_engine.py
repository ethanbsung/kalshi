from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from typing import Any, Callable

from kalshi_bot.kalshi.fees import taker_fee_dollars
from kalshi_bot.models.volatility import (
    annualize_vol,
    compute_log_returns,
    ewma_volatility,
    resample_last_price_series,
)
from kalshi_bot.state import LiveMarketState
from kalshi_bot.strategy.edge_engine import EdgeInputs, compute_edge_for_market

FeeFn = Callable[[float | None, int], float | None]


@dataclass
class SigmaMemory:
    last_good_sigma: float | None = None


def _estimate_step_seconds(timestamps: list[int]) -> float | None:
    if len(timestamps) < 2:
        return None
    diffs = [b - a for a, b in zip(timestamps, timestamps[1:]) if b > a]
    if not diffs:
        return None
    diffs.sort()
    return float(diffs[len(diffs) // 2])


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _compute_mid(bid: float | None, ask: float | None) -> float | None:
    if bid is None or ask is None:
        return None
    return (bid + ask) / 2.0


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
        return f"sigma_points_used={sigma_points_used} < min_points={min_points}"
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


def _compute_sigma(
    *,
    state: LiveMarketState,
    sigma_memory: SigmaMemory,
    product_id: str,
    now_ts: int,
    lookback_seconds: int,
    max_spot_points: int,
    ewma_lambda: float,
    min_points: int,
    min_sigma_lookback_seconds: int,
    resample_seconds: int,
    sigma_default: float,
    sigma_max: float,
) -> dict[str, Any]:
    raw_timestamps, raw_prices = state.spot_history(
        product_id=product_id,
        now_ts=now_ts,
        lookback_seconds=lookback_seconds,
    )
    if max_spot_points > 0 and len(raw_timestamps) > max_spot_points:
        raw_timestamps = raw_timestamps[-max_spot_points:]
        raw_prices = raw_prices[-max_spot_points:]

    history_span_seconds = (
        (raw_timestamps[-1] - raw_timestamps[0]) if len(raw_timestamps) >= 2 else 0
    )
    sigma_reason: str | None = None
    sigma_raw: float | None = None
    sigma_source = "default"
    sigma_ok = False
    sigma = sigma_default
    step_seconds = float(resample_seconds)
    sigma_points_used = 0
    sigma_lookback_seconds_used = int(history_span_seconds)
    raw_points = len(raw_timestamps)
    resampled_points = 0

    if not raw_timestamps or not raw_prices:
        sigma_reason = "insufficient_points"
    else:
        resampled = resample_last_price_series(
            raw_timestamps, raw_prices, bucket_seconds=max(int(resample_seconds), 1)
        )
        resampled_timestamps, resampled_prices = resampled
        resampled_points = len(resampled_timestamps)
        estimated_step = _estimate_step_seconds(resampled_timestamps)
        if estimated_step is None:
            sigma_reason = "missing_step"
        else:
            step_seconds = estimated_step
        if sigma_reason is None and (step_seconds < 1 or step_seconds > 3600):
            sigma_reason = "bad_step_seconds"

        returns = compute_log_returns(resampled_prices) if sigma_reason is None else []
        sigma_points_used = len(returns)
        if sigma_reason is None and sigma_points_used < min_points:
            sigma_reason = "insufficient_points"
        if sigma_reason is None and history_span_seconds < min_sigma_lookback_seconds:
            sigma_reason = "insufficient_history_span"
        if sigma_reason is None:
            vol_step = ewma_volatility(returns, lambda_=ewma_lambda)
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

    if sigma_reason is None and sigma_raw is not None:
        sigma = sigma_raw
        sigma_source = "ewma"
        sigma_ok = True
        sigma_memory.last_good_sigma = sigma
    elif sigma_memory.last_good_sigma is not None:
        sigma = sigma_memory.last_good_sigma
        sigma_source = "history"
        sigma_ok = False
    else:
        sigma = sigma_default
        sigma_source = "default"
        sigma_ok = False

    sigma_quality = "ok"
    if sigma_reason is not None:
        sigma_quality = "fallback_history" if sigma_source == "history" else "fallback_default"

    return {
        "sigma": sigma,
        "sigma_unclamped": sigma_raw,
        "sigma_source": sigma_source,
        "sigma_ok": sigma_ok,
        "sigma_reason": sigma_reason,
        "sigma_reason_context": _sigma_reason_context(
            sigma_reason,
            history_span_seconds=history_span_seconds,
            min_sigma_lookback_seconds=min_sigma_lookback_seconds,
            sigma_points_used=sigma_points_used,
            min_points=min_points,
            sigma_raw=sigma_raw,
            sigma_max=sigma_max,
            step_seconds=step_seconds,
        ),
        "sigma_quality": sigma_quality,
        "sigma_points_used": sigma_points_used,
        "sigma_lookback_seconds_used": sigma_lookback_seconds_used,
        "sigma_persisted": None,
        "min_sigma_points": min_points,
        "min_sigma_lookback_seconds": min_sigma_lookback_seconds,
        "resample_seconds": resample_seconds,
        "raw_points": raw_points,
        "resampled_points": resampled_points,
        "step_seconds": step_seconds,
    }


def compute_edges_from_live_state(
    *,
    state: LiveMarketState,
    sigma_memory: SigmaMemory,
    product_id: str,
    lookback_seconds: int,
    max_spot_points: int,
    ewma_lambda: float,
    min_points: int,
    min_sigma_lookback_seconds: int,
    resample_seconds: int,
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
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    now_ts = now_ts or int(time.time())
    fee_fn = fee_fn or taker_fee_dollars

    latest = state.latest_spot(product_id)
    if latest is None:
        return (
            {
                "error": "spot_missing",
                "edges_inserted": 0,
                "relevant_total": 0,
                "skip_reasons": {},
                "selection": {
                    "method": "unavailable",
                    "pct_band": pct_band,
                    "top_n": top_n,
                    "candidate_count_total": 0,
                    "excluded_expired": 0,
                    "excluded_horizon_out_of_range": 0,
                    "excluded_missing_bounds": 0,
                    "excluded_missing_recent_quote": 0,
                    "excluded_untradable": 0,
                    "excluded_missing_close_ts": 0,
                    "selected_count": 0,
                    "spot_ts": None,
                    "now_ts": now_ts,
                    "selection_samples": [],
                    "series": series or [],
                    "status": status,
                    "require_quotes": require_quotes,
                },
            },
            [],
        )

    spot_ts = int(latest.ts)
    spot_price = float(latest.price)
    sigma_state = _compute_sigma(
        state=state,
        sigma_memory=sigma_memory,
        product_id=product_id,
        now_ts=now_ts,
        lookback_seconds=lookback_seconds,
        max_spot_points=max_spot_points,
        ewma_lambda=ewma_lambda,
        min_points=min_points,
        min_sigma_lookback_seconds=min_sigma_lookback_seconds,
        resample_seconds=resample_seconds,
        sigma_default=sigma_default,
        sigma_max=sigma_max,
    )
    sigma = float(sigma_state["sigma"])

    relevant_ids, selection = state.select_relevant_market_ids(
        spot_price=spot_price,
        spot_ts=spot_ts,
        now_ts=now_ts,
        status=status,
        series=series,
        pct_band=pct_band,
        top_n=top_n,
        freshness_seconds=freshness_seconds,
        max_horizon_seconds=max_horizon_seconds,
        require_quotes=require_quotes,
        min_ask_cents=min_ask_cents,
        max_ask_cents=max_ask_cents,
    )
    if not relevant_ids:
        no_relevant_skip_reasons: dict[str, int] = {}
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
        return (
            {
                "error": "no_relevant_markets",
                "edges_inserted": 0,
                "relevant_total": 0,
                "selection": selection,
                "skip_reasons": no_relevant_skip_reasons,
                "spot_ts": spot_ts,
                "spot_price": spot_price,
                **sigma_state,
            },
            [],
        )

    inputs = EdgeInputs(
        spot_price=spot_price,
        spot_ts=spot_ts,
        sigma_annualized=sigma,
        now_ts=now_ts,
    )
    inserted = 0
    skipped = 0
    skip_reasons: dict[str, int] = {}
    diagnostics: dict[str, int] = {}
    max_spot_age_seconds: int | None = None
    max_quote_age_seconds: int | None = None
    snapshot_rows: list[dict[str, Any]] = []

    for market_id in relevant_ids:
        contract = state.get_contract(market_id)
        if contract is None:
            skip_reasons["missing_contract"] = skip_reasons.get("missing_contract", 0) + 1
            skipped += 1
            continue
        settlement_ts = (
            _safe_int(contract.get("close_ts"))
            or _safe_int(contract.get("expected_expiration_ts"))
            or _safe_int(contract.get("settlement_ts"))
        )
        if settlement_ts is None:
            skip_reasons["missing_settlement_ts"] = skip_reasons.get("missing_settlement_ts", 0) + 1
            skipped += 1
            continue

        selection_horizon_raw = settlement_ts - now_ts
        if selection_horizon_raw < -5:
            skip_reasons["expired_contract"] = skip_reasons.get("expired_contract", 0) + 1
            skipped += 1
            continue
        horizon_seconds = max(settlement_ts - spot_ts, 0)
        selection_horizon_seconds = max(selection_horizon_raw, 0)

        quote = state.get_quote(market_id)
        if quote is None:
            skip_reasons["missing_quote"] = skip_reasons.get("missing_quote", 0) + 1
            skipped += 1
            continue

        yes_ask = _safe_float(quote.get("yes_ask"))
        no_ask = _safe_float(quote.get("no_ask"))
        yes_bid = _safe_float(quote.get("yes_bid"))
        no_bid = _safe_float(quote.get("no_bid"))

        def _valid_ask(value: float | None) -> bool:
            return value is not None and 0.0 <= value <= 100.0

        crossed_market = False
        if _valid_ask(yes_ask) and _valid_ask(no_ask):
            yes_ask_val = yes_ask
            no_ask_val = no_ask
            if yes_ask_val is not None and no_ask_val is not None:
                crossed_market = (yes_ask_val + no_ask_val) < 100.0
        if crossed_market:
            skip_reasons["crossed_market"] = skip_reasons.get("crossed_market", 0) + 1
            diagnostics["crossed_market"] = diagnostics.get("crossed_market", 0) + 1
            skipped += 1
            continue

        def _tradable_ask(value: float | None, bid: float | None) -> bool:
            if value is None:
                return False
            if value < 0.0 or value > 100.0:
                return False
            if value in (0.0, 100.0):
                return True
            if value < min_ask_cents or value > max_ask_cents:
                return False
            if bid is None:
                return False
            spread = value - bid
            return spread >= 0.0

        yes_tradable = _tradable_ask(yes_ask, yes_bid)
        no_tradable = _tradable_ask(no_ask, no_bid)
        if not yes_tradable and not no_tradable:
            skip_reasons["missing_both_sides"] = skip_reasons.get("missing_both_sides", 0) + 1
            skipped += 1
            continue
        if not yes_tradable:
            quote["yes_ask"] = None
            skip_reasons["missing_yes_ask"] = skip_reasons.get("missing_yes_ask", 0) + 1
        if not no_tradable:
            quote["no_ask"] = None
            skip_reasons["missing_no_ask"] = skip_reasons.get("missing_no_ask", 0) + 1

        edge = compute_edge_for_market(
            contract,
            quote,
            inputs,
            fee_fn,
            horizon_seconds=horizon_seconds,
            selection_horizon_seconds=selection_horizon_seconds,
            contracts=contracts,
        )
        if edge is None:
            skip_reasons["invalid_edge"] = skip_reasons.get("invalid_edge", 0) + 1
            skipped += 1
            continue

        quote_ts = _safe_int(quote.get("ts"))
        spot_age_seconds = now_ts - spot_ts
        quote_age_seconds = (now_ts - quote_ts) if quote_ts is not None else None
        max_spot_age_seconds = (
            spot_age_seconds
            if max_spot_age_seconds is None
            else max(max_spot_age_seconds, spot_age_seconds)
        )
        if quote_age_seconds is not None:
            max_quote_age_seconds = (
                quote_age_seconds
                if max_quote_age_seconds is None
                else max(max_quote_age_seconds, quote_age_seconds)
            )

        raw_meta: dict[str, Any] = {}
        try:
            raw_meta = json.loads(edge.raw_json) if edge.raw_json else {}
            if not isinstance(raw_meta, dict):
                raw_meta = {}
        except json.JSONDecodeError:
            raw_meta = {}
        raw_meta.update(
            {
                "snapshot_version": 1,
                "sigma_source": sigma_state["sigma_source"],
                "sigma_ok": sigma_state["sigma_ok"],
                "sigma_reason": sigma_state["sigma_reason"],
                "sigma_reason_context": sigma_state["sigma_reason_context"],
                "sigma_points_used": sigma_state["sigma_points_used"],
                "min_sigma_points": sigma_state["min_sigma_points"],
                "sigma_lookback_seconds_used": sigma_state["sigma_lookback_seconds_used"],
                "min_sigma_lookback_seconds": sigma_state["min_sigma_lookback_seconds"],
            }
        )
        snapshot_rows.append(
            {
                "asof_ts": now_ts,
                "market_id": edge.market_id,
                "settlement_ts": edge.settlement_ts,
                "spot_ts": spot_ts,
                "spot_price": edge.spot_price,
                "sigma_annualized": edge.sigma_annualized,
                "prob_yes": edge.prob_yes,
                "horizon_seconds": edge.horizon_seconds,
                "quote_ts": quote_ts,
                "yes_bid": _safe_float(quote.get("yes_bid")),
                "yes_ask": _safe_float(quote.get("yes_ask")),
                "no_bid": _safe_float(quote.get("no_bid")),
                "no_ask": _safe_float(quote.get("no_ask")),
                "yes_mid": _compute_mid(
                    _safe_float(quote.get("yes_bid")),
                    _safe_float(quote.get("yes_ask")),
                ),
                "no_mid": _compute_mid(
                    _safe_float(quote.get("no_bid")),
                    _safe_float(quote.get("no_ask")),
                ),
                "ev_take_yes": edge.ev_take_yes,
                "ev_take_no": edge.ev_take_no,
                "spot_age_seconds": spot_age_seconds,
                "quote_age_seconds": quote_age_seconds,
                "raw_json": json.dumps(raw_meta),
            }
        )
        inserted += 1

    summary = {
        "now_ts": now_ts,
        "spot_price": spot_price,
        "spot_ts": spot_ts,
        "selection": selection,
        "relevant_total": len(relevant_ids),
        "edges_inserted": inserted,
        "snapshots_inserted": inserted,
        "snapshots_total": inserted,
        "skipped": skipped,
        "skip_reasons": skip_reasons,
        "diagnostics": diagnostics,
        "max_spot_age_seconds": max_spot_age_seconds,
        "max_quote_age_seconds": max_quote_age_seconds,
        "latest_quote_ts": max(
            [row["quote_ts"] for row in snapshot_rows if row.get("quote_ts") is not None],
            default=None,
        ),
        "quote_age_seconds": max_quote_age_seconds,
        "quotes_distinct_markets_recent": len(
            {row["market_id"] for row in snapshot_rows if row.get("quote_ts") is not None}
        ),
        "relevant_with_recent_quotes": len(snapshot_rows),
        "missing_quote_sample": [],
        "recent_quote_market_ids_sample": [row["market_id"] for row in snapshot_rows[:5]],
        "relevant_ids_sample": relevant_ids[:5],
        "relevant_titles_sample": {},
        **sigma_state,
    }
    return summary, snapshot_rows
