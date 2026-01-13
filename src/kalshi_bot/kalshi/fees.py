"""Kalshi fee utilities (taker fees only)."""

from __future__ import annotations

import math
from typing import Any


def taker_fee_dollars(price_cents: float | None, contracts: int) -> float | None:
    if price_cents is None:
        return None
    if contracts <= 0:
        return None
    try:
        price_value = float(price_cents)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(price_value):
        return None
    if price_value < 0.0 or price_value > 100.0:
        return None
    if price_value == 0.0 or price_value == 100.0:
        return 0.0
    price = price_value / 100.0
    raw = 0.07 * contracts * price * (1.0 - price)
    return math.ceil(raw * 100.0) / 100.0


def max_contracts_for_budget(max_budget_dollars: float, price_cents: float | None) -> int:
    """Conservative sizing helper based on notional cost only."""
    if max_budget_dollars <= 0:
        return 0
    if price_cents is None:
        return 0
    try:
        price = float(price_cents) / 100.0
    except (TypeError, ValueError):
        return 0
    if price <= 0.0:
        return 0
    return int(max_budget_dollars // price)
