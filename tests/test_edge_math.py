from kalshi_bot.strategy.edge_math import (
    Quote,
    ev_make_no,
    ev_make_yes,
    ev_take_no,
    ev_take_yes,
    implied_prob,
)


def test_ev_yes_increases_with_prob():
    def fee_fn(cost: float, contracts: int) -> float:
        return 0.0

    low = ev_take_yes(0.4, 50.0, fee_fn)
    high = ev_take_yes(0.6, 50.0, fee_fn)
    assert low is not None and high is not None
    assert high > low


def test_fees_reduce_ev_and_are_called():
    calls = []

    def fee_fn(cost: float, contracts: int) -> float:
        calls.append((cost, contracts))
        return 0.02

    ev = ev_take_yes(0.6, 50.0, fee_fn)
    assert ev is not None
    assert calls, "fee_fn should be called"

    def zero_fee(cost: float, contracts: int) -> float:
        return 0.0

    ev_no_fee = ev_take_yes(0.6, 50.0, zero_fee)
    assert ev_no_fee is not None
    assert ev_no_fee > ev


def test_missing_price_returns_none():
    def fee_fn(cost: float, contracts: int) -> float:
        return 0.0

    assert ev_take_yes(0.5, None, fee_fn) is None
    assert ev_take_no(0.5, None, fee_fn) is None
    assert ev_make_yes(0.5, None, fee_fn) is None
    assert ev_make_no(0.5, None, fee_fn) is None


def test_prob_none_returns_none_without_fees():
    calls = []

    def fee_fn(cost: float, contracts: int) -> float:
        calls.append((cost, contracts))
        return 0.0

    assert ev_take_yes(None, 50.0, fee_fn) is None
    assert ev_take_no(None, 50.0, fee_fn) is None
    assert ev_make_yes(None, 50.0, fee_fn) is None
    assert ev_make_no(None, 50.0, fee_fn) is None
    assert calls == []


def test_implied_prob_from_quote():
    quote = Quote(yes_bid=45.0, yes_ask=55.0, no_bid=40.0, no_ask=60.0)
    probs = quote.implied_probs()
    assert probs["yes_bid"] == 0.45
    assert probs["no_ask"] == 0.6
    assert implied_prob(None) is None
