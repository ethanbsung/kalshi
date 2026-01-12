"""Volatility estimation utilities for spot price history."""

from __future__ import annotations

import math
from typing import Iterable

SECONDS_PER_YEAR = 365.0 * 24.0 * 60.0 * 60.0


def compute_log_returns(prices: Iterable[float]) -> list[float]:
    """Compute log returns, skipping non-positive prices."""
    cleaned: list[float] = []
    for price in prices:
        if price is None:
            continue
        if price <= 0:
            continue
        cleaned.append(price)

    returns: list[float] = []
    if len(cleaned) < 2:
        return returns
    for prev, curr in zip(cleaned, cleaned[1:]):
        returns.append(math.log(curr / prev))
    return returns


def ewma_volatility(returns: list[float], lambda_: float) -> float | None:
    """EWMA volatility per sqrt(step) using variance recursion."""
    if not returns:
        return None
    if not (0.0 < lambda_ < 1.0):
        raise ValueError("lambda_ must be in (0, 1)")
    var = returns[0] ** 2
    for value in returns[1:]:
        var = lambda_ * var + (1.0 - lambda_) * (value**2)
    return math.sqrt(var)


def annualize_vol(vol_per_sqrt_step: float, step_seconds: float) -> float:
    if step_seconds <= 0:
        raise ValueError("step_seconds must be positive")
    return vol_per_sqrt_step * math.sqrt(SECONDS_PER_YEAR / step_seconds)


def estimate_sigma_annualized(
    prices: list[float],
    step_seconds: float,
    lambda_: float,
    min_points: int,
    sigma_floor: float,
    sigma_cap: float,
) -> float | None:
    """Estimate annualized sigma using EWMA log-returns with clamping."""
    returns = compute_log_returns(prices)
    if len(returns) < min_points:
        return None
    vol_step = ewma_volatility(returns, lambda_)
    if vol_step is None:
        return None
    sigma = annualize_vol(vol_step, step_seconds)
    if sigma_floor > sigma_cap:
        raise ValueError("sigma_floor must be <= sigma_cap")
    return max(sigma_floor, min(sigma_cap, sigma))
