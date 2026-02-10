"""Scoring for edge snapshots once outcomes are known."""

from __future__ import annotations

import math
from typing import Any

from kalshi_bot.kalshi.fees import taker_fee_dollars
from kalshi_bot.models.probability import EPS


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _clamp_prob(prob: float) -> float:
    return max(EPS, min(1.0 - EPS, prob))


def score_snapshot(snapshot_row: dict[str, Any], outcome: int) -> dict[str, Any]:
    """Return scores for a snapshot given a 0/1 outcome."""
    errors: set[str] = set()
    if outcome not in (0, 1):
        errors.add("invalid_outcome")

    prob_yes = _safe_float(snapshot_row.get("prob_yes"))
    brier = None
    logloss = None
    if prob_yes is None:
        errors.add("missing_prob_yes")
    elif outcome in (0, 1):
        prob_clamped = _clamp_prob(prob_yes)
        brier = (prob_clamped - outcome) ** 2
        logloss = -(
            outcome * math.log(prob_clamped)
            + (1 - outcome) * math.log(1.0 - prob_clamped)
        )

    yes_ask = _safe_float(snapshot_row.get("yes_ask"))
    no_ask = _safe_float(snapshot_row.get("no_ask"))
    pnl_take_yes = None
    pnl_take_no = None
    if yes_ask is None:
        errors.add("missing_yes_ask")
    elif outcome in (0, 1):
        yes_fee = taker_fee_dollars(yes_ask, 1)
        if yes_fee is None:
            errors.add("invalid_yes_fee")
        else:
            pnl_take_yes = outcome - (yes_ask / 100.0) - yes_fee

    if no_ask is None:
        errors.add("missing_no_ask")
    elif outcome in (0, 1):
        no_fee = taker_fee_dollars(no_ask, 1)
        if no_fee is None:
            errors.add("invalid_no_fee")
        else:
            pnl_take_no = (1 - outcome) - (no_ask / 100.0) - no_fee

    return {
        "pnl_take_yes": pnl_take_yes,
        "pnl_take_no": pnl_take_no,
        "brier": brier,
        "logloss": logloss,
        "error": ",".join(sorted(errors)) if errors else None,
    }
