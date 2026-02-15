from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any

from kalshi_bot.events.models import (
    ContractUpdateEvent,
    EventBase,
    MarketLifecycleEvent,
    QuoteUpdateEvent,
    SpotTickEvent,
)
from kalshi_bot.kalshi.market_filters import normalize_db_status, normalize_series


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_market_status(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    if normalized == "active":
        return "open"
    return normalized


def _market_matches_series(market_id: str, series: list[str]) -> bool:
    if not series:
        return True
    for item in series:
        if market_id == item or market_id.startswith(f"{item}-"):
            return True
    return False


@dataclass(frozen=True)
class SpotValue:
    ts: int
    price: float


class LiveMarketState:
    """In-memory state built from market event streams."""

    def __init__(self, *, max_spot_points: int = 20_000) -> None:
        self._max_spot_points = max(max_spot_points, 1)
        self._spot_history: dict[str, deque[tuple[int, float]]] = {}
        self._spot_latest: dict[str, SpotValue] = {}
        self._quotes: dict[str, dict[str, Any]] = {}
        self._contracts: dict[str, dict[str, Any]] = {}
        self._markets: dict[str, dict[str, Any]] = {}
        self._market_event_ts: dict[str, int] = {}
        self._contract_event_ts: dict[str, int] = {}
        self._event_counts: dict[str, int] = {}

    @property
    def event_counts(self) -> dict[str, int]:
        return dict(self._event_counts)

    def apply_event(self, event: EventBase) -> None:
        self._event_counts[event.event_type] = self._event_counts.get(event.event_type, 0) + 1

        if isinstance(event, SpotTickEvent):
            ts = int(event.payload.ts)
            product_id = str(event.payload.product_id)
            price = float(event.payload.price)
            history = self._spot_history.get(product_id)
            if history is None:
                history = deque(maxlen=self._max_spot_points)
                self._spot_history[product_id] = history
            history.append((ts, price))
            latest = self._spot_latest.get(product_id)
            if latest is None or ts >= latest.ts:
                self._spot_latest[product_id] = SpotValue(ts=ts, price=price)
            return

        if isinstance(event, QuoteUpdateEvent):
            quote_payload = event.payload
            market_id = str(quote_payload.market_id)
            quote = self._quotes.get(market_id) or {}
            ts = int(quote_payload.ts)
            prev_ts = _safe_int(quote.get("ts"))
            if prev_ts is not None and ts < prev_ts:
                return
            self._quotes[market_id] = {
                "ts": ts,
                "yes_bid": _safe_float(quote_payload.yes_bid),
                "yes_ask": _safe_float(quote_payload.yes_ask),
                "no_bid": _safe_float(quote_payload.no_bid),
                "no_ask": _safe_float(quote_payload.no_ask),
            }
            return

        if isinstance(event, MarketLifecycleEvent):
            lifecycle_payload = event.payload
            market_id = str(lifecycle_payload.market_id)
            event_ts = int(event.ts_event)
            prev_ts = self._market_event_ts.get(market_id)
            if prev_ts is not None and event_ts < prev_ts:
                return
            current = self._markets.get(market_id) or {}
            next_status = _normalize_market_status(str(lifecycle_payload.status))
            if next_status is not None:
                current["status"] = next_status
            close_ts = _safe_int(lifecycle_payload.close_ts)
            expected_expiration_ts = _safe_int(lifecycle_payload.expected_expiration_ts)
            expiration_ts = _safe_int(lifecycle_payload.expiration_ts)
            settlement_ts = _safe_int(lifecycle_payload.settlement_ts)
            if close_ts is not None:
                current["close_ts"] = close_ts
            if expected_expiration_ts is not None:
                current["expected_expiration_ts"] = expected_expiration_ts
            if expiration_ts is not None:
                current["expiration_ts"] = expiration_ts
            if settlement_ts is not None:
                current["settlement_ts"] = settlement_ts
            self._markets[market_id] = current
            self._market_event_ts[market_id] = event_ts
            return

        if isinstance(event, ContractUpdateEvent):
            contract_payload = event.payload
            ticker = str(contract_payload.ticker)
            event_ts = int(event.ts_event)
            prev_ts = self._contract_event_ts.get(ticker)
            if prev_ts is not None and event_ts < prev_ts:
                return
            current = self._contracts.get(ticker) or {}
            current["ticker"] = ticker
            current["lower"] = _safe_float(contract_payload.lower)
            current["upper"] = _safe_float(contract_payload.upper)
            current["strike_type"] = (
                str(contract_payload.strike_type)
                if contract_payload.strike_type is not None
                else None
            )
            close_ts = _safe_int(contract_payload.close_ts)
            expected_expiration_ts = _safe_int(contract_payload.expected_expiration_ts)
            expiration_ts = _safe_int(contract_payload.expiration_ts)
            settled_ts = _safe_int(contract_payload.settled_ts)
            outcome = _safe_int(contract_payload.outcome)
            if close_ts is not None:
                current["close_ts"] = close_ts
            if expected_expiration_ts is not None:
                current["expected_expiration_ts"] = expected_expiration_ts
            if expiration_ts is not None:
                current["expiration_ts"] = expiration_ts
            if settled_ts is not None:
                current["settled_ts"] = settled_ts
            if outcome is not None:
                current["outcome"] = outcome
            self._contracts[ticker] = current
            self._contract_event_ts[ticker] = event_ts
            return

    def latest_spot(self, product_id: str) -> SpotValue | None:
        return self._spot_latest.get(product_id)

    def spot_history(
        self,
        *,
        product_id: str,
        now_ts: int,
        lookback_seconds: int,
    ) -> tuple[list[int], list[float]]:
        history = self._spot_history.get(product_id)
        if history is None:
            return [], []
        threshold = now_ts - max(lookback_seconds, 0)
        timestamps: list[int] = []
        prices: list[float] = []
        for ts, price in history:
            if ts >= threshold:
                timestamps.append(int(ts))
                prices.append(float(price))
        return timestamps, prices

    def get_contract(self, market_id: str) -> dict[str, Any] | None:
        contract = self._contracts.get(market_id)
        if contract is None:
            return None
        merged = dict(contract)
        market = self._markets.get(market_id)
        if market:
            for key in (
                "status",
                "close_ts",
                "expected_expiration_ts",
                "expiration_ts",
                "settlement_ts",
            ):
                value = market.get(key)
                if value is not None and merged.get(key) is None:
                    merged[key] = value
        return merged

    def get_quote(self, market_id: str) -> dict[str, Any] | None:
        quote = self._quotes.get(market_id)
        if quote is None:
            return None
        return dict(quote)

    def select_relevant_market_ids(
        self,
        *,
        spot_price: float,
        spot_ts: int,
        now_ts: int,
        status: str | None,
        series: list[str] | None,
        pct_band: float,
        top_n: int,
        freshness_seconds: int,
        max_horizon_seconds: int,
        grace_seconds: int = 3600,
        require_quotes: bool = True,
        min_ask_cents: float = 1.0,
        max_ask_cents: float = 99.0,
    ) -> tuple[list[str], dict[str, Any]]:
        requested_status = _normalize_market_status(normalize_db_status(status))
        normalized_series = normalize_series(series)
        cutoff_min = now_ts - 5
        cutoff_max = now_ts + max_horizon_seconds + grace_seconds

        candidate_count_total = 0
        excluded_missing_bounds = 0
        excluded_missing_close_ts = 0
        excluded_expired = 0
        excluded_horizon_out_of_range = 0
        excluded_missing_recent_quote = 0
        excluded_untradable = 0

        candidates: list[dict[str, Any]] = []

        for market_id, contract in self._contracts.items():
            if requested_status is not None:
                market = self._markets.get(market_id)
                market_status = (
                    _normalize_market_status(str(market.get("status") or ""))
                    if market is not None
                    else None
                )
                # In bus replay mode we can have fresh contract updates before
                # lifecycle status for a market. Only enforce status when known.
                if market_status is not None and market_status != requested_status:
                    continue
            if not _market_matches_series(market_id, normalized_series):
                continue

            strike_type = str(contract.get("strike_type") or "")
            if strike_type not in {"between", "less", "greater"}:
                continue
            candidate_count_total += 1

            lower = _safe_float(contract.get("lower"))
            upper = _safe_float(contract.get("upper"))
            has_bounds = (
                (strike_type == "between" and lower is not None and upper is not None and upper > lower)
                or (strike_type == "less" and upper is not None)
                or (strike_type == "greater" and lower is not None)
            )
            if not has_bounds:
                excluded_missing_bounds += 1
                continue

            close_ts = (
                _safe_int(contract.get("close_ts"))
                or _safe_int(contract.get("expected_expiration_ts"))
                or _safe_int(contract.get("settlement_ts"))
                or _safe_int((self._markets.get(market_id) or {}).get("close_ts"))
                or _safe_int((self._markets.get(market_id) or {}).get("expected_expiration_ts"))
                or _safe_int((self._markets.get(market_id) or {}).get("settlement_ts"))
            )
            if close_ts is None:
                excluded_missing_close_ts += 1
                continue
            if close_ts < cutoff_min:
                excluded_expired += 1
                continue
            if close_ts > cutoff_max:
                excluded_horizon_out_of_range += 1
                continue

            if strike_type == "between":
                if lower is None or upper is None:
                    excluded_missing_bounds += 1
                    continue
                price_ref = (lower + upper) / 2.0
            elif strike_type == "less":
                if upper is None:
                    excluded_missing_bounds += 1
                    continue
                price_ref = upper
            else:
                if lower is None:
                    excluded_missing_bounds += 1
                    continue
                price_ref = lower
            distance_pct = abs(price_ref - spot_price) / spot_price * 100.0
            horizon_seconds = close_ts - now_ts

            candidate: dict[str, Any] = {
                "ticker": market_id,
                "distance_pct": distance_pct,
                "close_ts": close_ts,
                "horizon_seconds": horizon_seconds,
            }
            if require_quotes:
                quote = self._quotes.get(market_id)
                if quote is None:
                    excluded_missing_recent_quote += 1
                    continue
                quote_ts = _safe_int(quote.get("ts"))
                if quote_ts is None or quote_ts < (now_ts - freshness_seconds):
                    excluded_missing_recent_quote += 1
                    continue
                yes_bid = _safe_float(quote.get("yes_bid"))
                yes_ask = _safe_float(quote.get("yes_ask"))
                no_bid = _safe_float(quote.get("no_bid"))
                no_ask = _safe_float(quote.get("no_ask"))

                def _ask_tradable(ask: float | None, bid: float | None) -> bool:
                    if ask is None:
                        return False
                    if ask < 0.0 or ask > 100.0:
                        return False
                    if ask in (0.0, 100.0):
                        return True
                    if ask < min_ask_cents or ask > max_ask_cents:
                        return False
                    if bid is None:
                        return False
                    spread = ask - bid
                    return spread >= 0.0

                yes_tradable = _ask_tradable(yes_ask, yes_bid)
                no_tradable = _ask_tradable(no_ask, no_bid)
                if not yes_tradable and not no_tradable:
                    excluded_untradable += 1
                    continue
                candidate["quote_ts"] = quote_ts
                candidate["quote_age_seconds"] = now_ts - quote_ts
                candidate["yes_ask"] = yes_ask
                candidate["no_ask"] = no_ask
            candidates.append(candidate)

        candidates.sort(key=lambda item: (item["distance_pct"], item["ticker"]))
        selected = [item for item in candidates if item["distance_pct"] <= pct_band]
        method = "pct_band"
        if top_n > 0 and len(selected) < min(top_n, len(candidates)):
            selected = candidates[:top_n]
            method = "top_n"

        selected_ids = [item["ticker"] for item in selected]
        summary = {
            "method": method,
            "pct_band": pct_band,
            "top_n": top_n,
            "candidate_count_total": candidate_count_total,
            "excluded_expired": excluded_expired,
            "excluded_horizon_out_of_range": excluded_horizon_out_of_range,
            "excluded_missing_bounds": excluded_missing_bounds,
            "excluded_missing_recent_quote": excluded_missing_recent_quote,
            "excluded_untradable": excluded_untradable,
            "excluded_missing_close_ts": excluded_missing_close_ts,
            "selected_count": len(selected_ids),
            "spot_ts": spot_ts,
            "now_ts": now_ts,
            "selection_samples": [
                {
                    "ticker": item["ticker"],
                    "distance_pct": item["distance_pct"],
                    "horizon_seconds": item["horizon_seconds"],
                    "close_ts": item["close_ts"],
                }
                for item in selected[:5]
            ],
            "series": normalized_series,
            "status": requested_status,
            "require_quotes": require_quotes,
        }
        return selected_ids, summary
