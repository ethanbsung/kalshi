import time

from kalshi_bot.events import (
    ContractUpdateEvent,
    MarketLifecycleEvent,
    QuoteUpdateEvent,
    SpotTickEvent,
)
from kalshi_bot.state import LiveMarketState
from kalshi_bot.strategy.edge_state_engine import (
    SigmaMemory,
    compute_edges_from_live_state,
)


def _seed_basic_market_state(now: int) -> LiveMarketState:
    state = LiveMarketState(max_spot_points=100)
    state.apply_event(
        SpotTickEvent(
            source="test",
            payload={
                "ts": now - 60,
                "product_id": "BTC-USD",
                "price": 30000.0,
            },
        )
    )
    state.apply_event(
        SpotTickEvent(
            source="test",
            payload={
                "ts": now,
                "product_id": "BTC-USD",
                "price": 30100.0,
            },
        )
    )
    state.apply_event(
        MarketLifecycleEvent(
            source="test",
            payload={
                "market_id": "KXBTC-STATE",
                "status": "active",
                "close_ts": now + 3600,
                "expected_expiration_ts": now + 3600,
            },
        )
    )
    state.apply_event(
        ContractUpdateEvent(
            source="test",
            payload={
                "ticker": "KXBTC-STATE",
                "lower": 30000.0,
                "upper": None,
                "strike_type": "greater",
                "close_ts": now + 3600,
                "expected_expiration_ts": now + 3600,
            },
        )
    )
    return state


def test_live_market_state_selection_accepts_active_alias() -> None:
    now = int(time.time())
    state = _seed_basic_market_state(now)
    state.apply_event(
        QuoteUpdateEvent(
            source="test",
            payload={
                "ts": now,
                "market_id": "KXBTC-STATE",
                "yes_bid": 45.0,
                "yes_ask": 55.0,
                "no_bid": 45.0,
                "no_ask": 55.0,
            },
        )
    )

    selected, summary = state.select_relevant_market_ids(
        spot_price=30100.0,
        spot_ts=now,
        now_ts=now,
        status="active",
        series=["KXBTC"],
        pct_band=5.0,
        top_n=10,
        freshness_seconds=300,
        max_horizon_seconds=3600 * 6,
        require_quotes=True,
    )
    assert selected == ["KXBTC-STATE"]
    assert summary["selected_count"] == 1
    assert summary["status"] == "open"


def test_compute_edges_from_live_state_generates_snapshot() -> None:
    now = int(time.time())
    state = _seed_basic_market_state(now)
    state.apply_event(
        QuoteUpdateEvent(
            source="test",
            payload={
                "ts": now,
                "market_id": "KXBTC-STATE",
                "yes_bid": 45.0,
                "yes_ask": 55.0,
                "no_bid": 45.0,
                "no_ask": 55.0,
            },
        )
    )

    summary, snapshots = compute_edges_from_live_state(
        state=state,
        sigma_memory=SigmaMemory(),
        product_id="BTC-USD",
        lookback_seconds=3600,
        max_spot_points=100,
        ewma_lambda=0.94,
        min_points=1,
        min_sigma_lookback_seconds=0,
        resample_seconds=5,
        sigma_default=0.6,
        sigma_max=5.0,
        status="active",
        series=["KXBTC"],
        pct_band=5.0,
        top_n=10,
        freshness_seconds=300,
        max_horizon_seconds=3600 * 6,
        now_ts=now,
    )
    assert "error" not in summary
    assert summary["edges_inserted"] == 1
    assert len(snapshots) == 1
    assert snapshots[0]["market_id"] == "KXBTC-STATE"
    assert snapshots[0]["prob_yes"] is not None
    assert snapshots[0]["ev_take_yes"] is not None


def test_compute_edges_from_live_state_reports_missing_quote() -> None:
    now = int(time.time())
    state = _seed_basic_market_state(now)

    summary, snapshots = compute_edges_from_live_state(
        state=state,
        sigma_memory=SigmaMemory(),
        product_id="BTC-USD",
        lookback_seconds=3600,
        max_spot_points=100,
        ewma_lambda=0.94,
        min_points=1,
        min_sigma_lookback_seconds=0,
        resample_seconds=5,
        sigma_default=0.6,
        sigma_max=5.0,
        status="active",
        series=["KXBTC"],
        pct_band=5.0,
        top_n=10,
        freshness_seconds=300,
        max_horizon_seconds=3600 * 6,
        now_ts=now,
    )
    assert summary["error"] == "no_relevant_markets"
    assert snapshots == []
    assert summary["skip_reasons"].get("missing_quote") == 1
