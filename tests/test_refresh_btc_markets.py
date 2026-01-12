from kalshi_bot.kalshi.btc_markets import (
    empty_series_tickers,
    extract_settlement_ts,
    extract_strike_basic,
)


def test_extract_strike_numeric():
    market = {
        "custom_strike": 44500,
        "price_ranges": [{"start": 0, "end": 100, "step": 1}],
    }
    assert extract_strike_basic(market) == 44500.0


def test_extract_strike_numeric_string():
    market = {"custom_strike": "44,500"}
    assert extract_strike_basic(market) == 44500.0


def test_extract_strike_dict_none():
    market = {
        "custom_strike": {"Associated Markets": ["A", "B"]},
        "price_ranges": [{"start": 0, "end": 100, "step": 1}],
    }
    assert extract_strike_basic(market) is None


def test_extract_strike_missing_none():
    market = {"custom_strike": None}
    assert extract_strike_basic(market) is None


def test_extract_settlement_ts():
    market = {"close_time": "2024-12-31T23:59:00Z"}
    assert extract_settlement_ts(market) == 1735689540


def test_empty_series_tickers():
    per_series = {
        "KXBTC": {"markets": []},
        "KXBTC15M": {"markets": [{"ticker": "A"}]},
    }
    assert empty_series_tickers(per_series) == ["KXBTC"]
