from kalshi_bot.strategy.opportunity_engine import (
    OpportunityConfig,
    build_opportunities_from_snapshots,
)


def test_build_opportunities_take_yes():
    snapshots = [
        {
            "asof_ts": 100,
            "market_id": "KXBTC-TEST",
            "settlement_ts": 200,
            "spot_ts": 90,
            "spot_price": 30000.0,
            "sigma_annualized": 0.5,
            "prob_yes": 0.6,
            "prob_yes_raw": 0.6001,
            "horizon_seconds": 110,
            "quote_ts": 95,
            "yes_bid": 45.0,
            "yes_ask": 50.0,
            "no_bid": 45.0,
            "no_ask": 50.0,
            "yes_mid": 47.5,
            "no_mid": 52.5,
            "spot_age_seconds": 10,
            "quote_age_seconds": 5,
        }
    ]
    config = OpportunityConfig(min_ev=0.05, emit_passes=False)
    rows, counters = build_opportunities_from_snapshots(snapshots, config)
    assert counters["takes"] == 1
    assert len(rows) == 1
    row = rows[0]
    assert row["side"] == "YES"
    assert row["would_trade"] == 1
    assert row["ev_raw"] == 0.6 - 0.50


def test_build_opportunities_missing_yes_ask_pass():
    snapshots = [
        {
            "asof_ts": 100,
            "market_id": "KXBTC-TEST",
            "settlement_ts": 200,
            "spot_ts": 90,
            "spot_price": 30000.0,
            "sigma_annualized": 0.5,
            "prob_yes": 0.6,
            "horizon_seconds": 110,
            "quote_ts": 95,
            "yes_bid": None,
            "yes_ask": None,
            "no_bid": 45.0,
            "no_ask": 50.0,
            "spot_age_seconds": 10,
            "quote_age_seconds": 5,
        }
    ]
    config = OpportunityConfig(
        min_ev=0.05, emit_passes=True, best_side_only=False
    )
    rows, counters = build_opportunities_from_snapshots(snapshots, config)
    assert counters["passes"] >= 1
    assert any(row["reason_not_eligible"] == "missing_yes_ask" for row in rows)
