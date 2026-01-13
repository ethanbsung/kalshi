"""Expected value math for Kalshi contracts (prices in cents)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from kalshi_bot.kalshi.fees import taker_fee_dollars

FeeFn = Callable[[float | None, int], float | None]


@dataclass(frozen=True)
class Quote:
    yes_bid: float | None
    yes_ask: float | None
    no_bid: float | None
    no_ask: float | None

    def implied_probs(self) -> dict[str, float | None]:
        return {
            "yes_bid": implied_prob(self.yes_bid),
            "yes_ask": implied_prob(self.yes_ask),
            "no_bid": implied_prob(self.no_bid),
            "no_ask": implied_prob(self.no_ask),
        }


def implied_prob(price_cents: float | None) -> float | None:
    if price_cents is None:
        return None
    return price_cents / 100.0


def _ev_buy(
    prob_yes: float | None,
    price_cents: float | None,
    fee_fn: FeeFn,
    contracts: int,
) -> float | None:
    if prob_yes is None:
        return None
    if price_cents is None:
        return None
    cost = (price_cents / 100.0) * contracts
    payout = prob_yes * contracts
    fees = fee_fn(price_cents, contracts)
    if fees is None:
        return None
    return payout - cost - fees


def ev_take_yes(
    prob_yes: float | None,
    yes_ask_cents: float | None,
    fee_fn: FeeFn = taker_fee_dollars,
    contracts: int = 1,
) -> float | None:
    """EV of buying YES at the ask."""
    return _ev_buy(prob_yes, yes_ask_cents, fee_fn, contracts)


def ev_take_no(
    prob_yes: float | None,
    no_ask_cents: float | None,
    fee_fn: FeeFn = taker_fee_dollars,
    contracts: int = 1,
) -> float | None:
    """EV of buying NO at the ask."""
    if prob_yes is None:
        return None
    return _ev_buy(1.0 - prob_yes, no_ask_cents, fee_fn, contracts)


def ev_make_yes(
    prob_yes: float | None,
    limit_price_cents: float | None,
    fee_fn: FeeFn = taker_fee_dollars,
    contracts: int = 1,
) -> float | None:
    """EV of placing a YES bid at a limit price."""
    return _ev_buy(prob_yes, limit_price_cents, fee_fn, contracts)


def ev_make_no(
    prob_yes: float | None,
    limit_price_cents: float | None,
    fee_fn: FeeFn = taker_fee_dollars,
    contracts: int = 1,
) -> float | None:
    """EV of placing a NO bid at a limit price."""
    if prob_yes is None:
        return None
    return _ev_buy(1.0 - prob_yes, limit_price_cents, fee_fn, contracts)
