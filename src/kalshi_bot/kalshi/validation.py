from __future__ import annotations

from typing import Any, Mapping


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def validate_quote_row(row: Mapping[str, Any]) -> tuple[bool, str | None]:
    market_id = row.get("market_id")
    if not isinstance(market_id, str) or not market_id.strip():
        return False, "missing_market_id"

    yes_bid = _as_float(row.get("yes_bid"))
    yes_ask = _as_float(row.get("yes_ask"))
    no_bid = _as_float(row.get("no_bid"))
    no_ask = _as_float(row.get("no_ask"))

    if yes_bid is not None and yes_ask is not None and yes_bid > yes_ask:
        return False, "yes_bid_gt_yes_ask"
    if no_bid is not None and no_ask is not None and no_bid > no_ask:
        return False, "no_bid_gt_no_ask"

    p_mid = _as_float(row.get("p_mid"))
    if p_mid is not None and not (0.0 <= p_mid <= 1.0):
        return False, "p_mid_out_of_bounds"

    return True, None
