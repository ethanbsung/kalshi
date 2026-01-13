from kalshi_bot.kalshi.fees import taker_fee_dollars
from kalshi_bot.strategy.edge_math import ev_take_yes


def test_taker_fee_rounding():
    assert taker_fee_dollars(50.0, 1) == 0.02
    assert taker_fee_dollars(1.0, 1) == 0.01
    assert taker_fee_dollars(0.0, 1) == 0.0
    assert taker_fee_dollars(100.0, 1) == 0.0


def test_taker_fee_invalid_inputs():
    assert taker_fee_dollars(None, 1) is None
    assert taker_fee_dollars(-1.0, 1) is None
    assert taker_fee_dollars(101.0, 1) is None
    assert taker_fee_dollars(50.0, 0) is None


def test_ev_includes_taker_fee():
    ev = ev_take_yes(0.6, 50.0)
    assert ev is not None
    assert abs(ev - 0.08) < 1e-9


def test_ev_returns_none_when_fee_invalid():
    assert ev_take_yes(0.6, 120.0) is None
