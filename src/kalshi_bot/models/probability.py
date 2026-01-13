"""Lognormal probability model for BTC price.

Uses geometric Brownian motion with mu=0 (E[S_T] = S_0):
    ln(S_T / S_0) ~ Normal(-0.5*sigma^2*t, sigma^2*t)
    S_T = S_0 * exp((-0.5*sigma^2*t) + sigma*sqrt(t)*Z)

Time is converted to year fractions using a 365-day year.
"""

from __future__ import annotations

import math
from typing import Final

SECONDS_PER_YEAR: Final[float] = 365.0 * 24.0 * 60.0 * 60.0
EPS: Final[float] = 1e-12


def _clamp_prob(prob: float) -> float:
    return max(EPS, min(1.0 - EPS, prob))


def _year_fraction(horizon_seconds: float) -> float:
    return horizon_seconds / SECONDS_PER_YEAR


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _step_prob(spot: float, threshold: float, *, greater: bool) -> float | None:
    if spot <= 0:
        return None
    if greater:
        return 1.0 if spot >= threshold else 0.0
    return 1.0 if spot <= threshold else 0.0


def prob_less_equal(
    spot: float, K: float, horizon_seconds: float, sigma_annualized: float
) -> float | None:
    """P(S_T <= K) under GBM with expected price preserved.

    For horizon_seconds <= 0, returns a deterministic step function at spot.
    For horizon_seconds > 0, invalid inputs (spot<=0, K<=0, sigma<=0) return None.
    """
    if horizon_seconds <= 0:
        step = _step_prob(spot, K, greater=False)
        return _clamp_prob(step) if step is not None else None
    if spot <= 0 or sigma_annualized <= 0 or K <= 0:
        return None

    t = _year_fraction(horizon_seconds)
    if t <= 0:
        step = _step_prob(spot, K, greater=False)
        return _clamp_prob(step) if step is not None else None

    sigma_t = sigma_annualized * math.sqrt(t)
    if sigma_t <= 0:
        return None

    z = (math.log(K / spot) + 0.5 * sigma_t * sigma_t) / sigma_t
    return _clamp_prob(_norm_cdf(z))


def prob_greater_equal(
    spot: float, K: float, horizon_seconds: float, sigma_annualized: float
) -> float | None:
    """P(S_T >= K) under mu=0 GBM (None for invalid inputs)."""
    if horizon_seconds <= 0:
        step = _step_prob(spot, K, greater=True)
        return _clamp_prob(step) if step is not None else None
    prob = prob_less_equal_raw(spot, K, horizon_seconds, sigma_annualized)
    if prob is None:
        return None
    return _clamp_prob(1.0 - prob)


def prob_between(
    spot: float,
    lower: float,
    upper: float,
    horizon_seconds: float,
    sigma_annualized: float,
) -> float | None:
    """P(lower <= S_T < upper) using a continuous lognormal distribution.

    Boundary convention is [lower, upper). For horizon_seconds <= 0, returns a
    deterministic step outcome (or None if spot<=0).
    """
    if upper <= lower:
        return None
    if horizon_seconds <= 0:
        if spot <= 0:
            return None
        return _clamp_prob(1.0 if lower <= spot < upper else 0.0)

    upper_raw = prob_less_equal_raw(
        spot, upper, horizon_seconds, sigma_annualized
    )
    lower_raw = prob_less_equal_raw(
        spot, lower, horizon_seconds, sigma_annualized
    )
    if upper_raw is None or lower_raw is None:
        return None
    upper_prob = _clamp_prob(upper_raw)
    lower_prob = _clamp_prob(lower_raw)
    return _clamp_prob(upper_prob - lower_prob)


def prob_less_equal_raw(
    spot: float, K: float, horizon_seconds: float, sigma_annualized: float
) -> float | None:
    """Unclamped P(S_T <= K) under mu=0 GBM."""
    if horizon_seconds <= 0:
        return _step_prob(spot, K, greater=False)
    if spot <= 0 or sigma_annualized <= 0 or K <= 0:
        return None

    t = _year_fraction(horizon_seconds)
    if t <= 0:
        return _step_prob(spot, K, greater=False)

    sigma_t = sigma_annualized * math.sqrt(t)
    if sigma_t <= 0:
        return None

    z = (math.log(K / spot) + 0.5 * sigma_t * sigma_t) / sigma_t
    return _norm_cdf(z)


def prob_greater_equal_raw(
    spot: float, K: float, horizon_seconds: float, sigma_annualized: float
) -> float | None:
    """Unclamped P(S_T >= K) under mu=0 GBM."""
    if horizon_seconds <= 0:
        return _step_prob(spot, K, greater=True)
    prob = prob_less_equal_raw(spot, K, horizon_seconds, sigma_annualized)
    if prob is None:
        return None
    return 1.0 - prob


def prob_between_raw(
    spot: float,
    lower: float,
    upper: float,
    horizon_seconds: float,
    sigma_annualized: float,
) -> float | None:
    """Unclamped P(lower <= S_T < upper)."""
    if upper <= lower:
        return None
    if horizon_seconds <= 0:
        if spot <= 0:
            return None
        return 1.0 if lower <= spot < upper else 0.0
    upper_prob = prob_less_equal_raw(
        spot, upper, horizon_seconds, sigma_annualized
    )
    lower_prob = prob_less_equal_raw(
        spot, lower, horizon_seconds, sigma_annualized
    )
    if upper_prob is None or lower_prob is None:
        return None
    return upper_prob - lower_prob
