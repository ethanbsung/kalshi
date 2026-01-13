import math

from kalshi_bot.models.volatility import (
    compute_log_returns,
    estimate_sigma_annualized,
    ewma_volatility,
    resample_last_price_series,
)


def test_compute_log_returns_skips_invalid():
    prices = [100.0, 0.0, -1.0, 105.0, 110.0]
    returns = compute_log_returns(prices)
    assert len(returns) == 2
    assert math.isclose(returns[0], math.log(105.0 / 100.0))


def test_ewma_increases_with_magnitude():
    low = ewma_volatility([0.01, 0.01, 0.01], lambda_=0.9)
    high = ewma_volatility([0.05, 0.05, 0.05], lambda_=0.9)
    assert low is not None and high is not None
    assert high > low


def test_clamping_and_min_points():
    prices = [100.0, 101.0, 102.0]
    sigma = estimate_sigma_annualized(
        prices=prices,
        step_seconds=60.0,
        lambda_=0.9,
        min_points=2,
        sigma_floor=0.1,
        sigma_cap=0.2,
    )
    assert sigma is not None
    assert 0.1 <= sigma <= 0.2

    too_few = estimate_sigma_annualized(
        prices=prices,
        step_seconds=60.0,
        lambda_=0.9,
        min_points=10,
        sigma_floor=0.1,
        sigma_cap=0.2,
    )
    assert too_few is None


def test_resample_last_price_series_last_tick():
    timestamps = [2, 1, 2, 7, 1, 7]
    prices = [20.0, 10.0, 21.0, 70.0, 11.0, 71.0]
    resampled_ts, resampled_prices = resample_last_price_series(
        timestamps, prices, bucket_seconds=5
    )
    assert resampled_ts == [2, 7]
    assert resampled_prices == [21.0, 71.0]
