import math

from kalshi_bot.models.probability import (
    EPS,
    prob_between,
    prob_greater_equal,
    prob_less_equal,
)


def test_prob_less_equal_monotonic():
    spot = 100.0
    sigma = 0.5
    horizon = 3600
    assert prob_less_equal(spot, 90.0, horizon, sigma) < prob_less_equal(
        spot, 110.0, horizon, sigma
    )


def test_prob_between_consistency():
    spot = 100.0
    sigma = 0.5
    horizon = 3600
    lower = 90.0
    upper = 110.0
    between = prob_between(spot, lower, upper, horizon, sigma)
    expected = prob_less_equal(spot, upper, horizon, sigma) - prob_less_equal(
        spot, lower, horizon, sigma
    )
    assert between is not None
    assert 0.0 <= between <= 1.0
    expected = max(EPS, min(1.0 - EPS, expected))
    assert abs(between - expected) < 1e-12


def test_horizon_zero_step_behavior_quotes():
    spot = 100.0
    sigma = 0.5
    assert prob_less_equal(spot, 100.0, 0.0, sigma) == 1.0 - EPS
    assert prob_less_equal(spot, 99.0, 0.0, sigma) == EPS
    assert prob_greater_equal(spot, 100.0, 0.0, sigma) == 1.0 - EPS
    assert prob_greater_equal(spot, 101.0, 0.0, sigma) == EPS
    assert prob_between(spot, 90.0, 100.0, 0.0, sigma) == EPS
    assert prob_between(spot, 90.0, 101.0, 0.0, sigma) == 1.0 - EPS


def test_median_shift_gt_half():
    spot = 100.0
    sigma = 0.8
    horizon = 86400
    prob = prob_less_equal(spot, spot, horizon, sigma)
    assert prob is not None
    assert 0.50 < prob < 0.60


def test_known_z_regression():
    spot = 100.0
    K = 105.0
    sigma = 0.6
    horizon = 86400
    t = horizon / (365.0 * 24.0 * 60.0 * 60.0)
    sigma_t = sigma * math.sqrt(t)
    z = (math.log(K / spot) + 0.5 * sigma_t * sigma_t) / sigma_t
    expected = 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))
    prob = prob_less_equal(spot, K, horizon, sigma)
    assert prob is not None
    assert abs(prob - expected) < 1e-9


def test_invalid_inputs_return_none():
    assert prob_less_equal(0.0, 100.0, 3600, 0.5) is None
    assert prob_less_equal(100.0, 0.0, 3600, 0.5) is None
    assert prob_less_equal(100.0, 100.0, 3600, 0.0) is None
    assert prob_greater_equal(100.0, 100.0, 3600, 0.0) is None
    assert prob_between(100.0, 90.0, 110.0, 3600, 0.0) is None


def test_greater_equals_complement():
    spot = 100.0
    K = 105.0
    sigma = 0.7
    horizon = 3600
    less = prob_less_equal(spot, K, horizon, sigma)
    greater = prob_greater_equal(spot, K, horizon, sigma)
    assert less is not None
    assert greater is not None
    assert abs(greater - (1.0 - less)) < 1e-12


def test_prob_between_near_spot_plausible():
    spot = 91100.0
    lower = 91000.0
    upper = 91249.0
    sigma = 0.2
    horizon = 7 * 24 * 3600
    prob = prob_between(spot, lower, upper, horizon, sigma)
    assert prob is not None
    assert 0.01 < prob < 0.2


def test_prob_between_horizon_scaling():
    spot = 91100.0
    lower = 91000.0
    upper = 91249.0
    sigma = 0.2
    prob_1d = prob_between(spot, lower, upper, 24 * 3600, sigma)
    prob_7d = prob_between(spot, lower, upper, 7 * 24 * 3600, sigma)
    assert prob_1d is not None and prob_7d is not None
    assert prob_7d < prob_1d


def test_extreme_otm_probs_are_clamped():
    spot = 100.0
    sigma = 0.4
    horizon = 3600
    low = prob_less_equal(spot, 1e-6, horizon, sigma)
    high = prob_less_equal(spot, 1e6, horizon, sigma)
    assert low is not None and high is not None
    assert EPS <= low < 1.0 - EPS
    assert EPS < high <= 1.0 - EPS


def test_between_tiny_prob_clamped():
    spot = 100.0
    sigma = 0.4
    horizon = 3600
    prob = prob_between(spot, 1e-6, 1e-5, horizon, sigma)
    assert prob is not None
    assert EPS <= prob < 1.0 - EPS
