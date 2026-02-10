import json

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


def test_build_opportunities_sigma_not_ready_blocks_take():
    snapshots = [
        {
            "asof_ts": 100,
            "market_id": "KXBTC-TEST",
            "settlement_ts": 200,
            "spot_ts": 90,
            "spot_price": 30000.0,
            "sigma_annualized": 0.6,
            "prob_yes": 0.62,
            "horizon_seconds": 110,
            "quote_ts": 95,
            "yes_bid": 45.0,
            "yes_ask": 50.0,
            "no_bid": 45.0,
            "no_ask": 50.0,
            "spot_age_seconds": 5,
            "quote_age_seconds": 5,
            "raw_json": json.dumps(
                {
                    "sigma_source": "default",
                    "sigma_ok": False,
                    "sigma_reason": "insufficient_history_span",
                    "sigma_points_used": 12,
                    "min_sigma_points": 10,
                    "sigma_lookback_seconds_used": 600,
                    "min_sigma_lookback_seconds": 3600,
                }
            ),
        }
    ]
    config = OpportunityConfig(min_ev=0.01, emit_passes=True)
    rows, counters = build_opportunities_from_snapshots(snapshots, config)
    assert counters["takes"] == 0
    assert counters["passes"] == 1
    assert any("sigma_not_ready" in (row["reason_not_eligible"] or "") for row in rows)


def test_build_opportunities_sigma_span_points_gating():
    snapshots = [
        {
            "asof_ts": 100,
            "market_id": "KXBTC-TEST",
            "settlement_ts": 200,
            "spot_ts": 90,
            "spot_price": 30000.0,
            "sigma_annualized": 0.6,
            "prob_yes": 0.62,
            "horizon_seconds": 110,
            "quote_ts": 95,
            "yes_bid": 45.0,
            "yes_ask": 50.0,
            "no_bid": 45.0,
            "no_ask": 50.0,
            "spot_age_seconds": 5,
            "quote_age_seconds": 5,
            "raw_json": json.dumps(
                {
                    "sigma_source": "ewma",
                    "sigma_ok": True,
                    "sigma_points_used": 5,
                    "min_sigma_points": 10,
                    "sigma_lookback_seconds_used": 600,
                    "min_sigma_lookback_seconds": 3600,
                }
            ),
        }
    ]
    config = OpportunityConfig(min_ev=0.01, emit_passes=True)
    rows, counters = build_opportunities_from_snapshots(snapshots, config)
    assert counters["takes"] == 0
    assert counters["passes"] == 1
    reasons = [row["reason_not_eligible"] or "" for row in rows]
    assert any("sigma_points_short" in reason for reason in reasons)
    assert any("sigma_history_short" in reason for reason in reasons)


def test_build_opportunities_freshness_gate_when_age_missing():
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
            "yes_bid": 45.0,
            "yes_ask": 50.0,
            "no_bid": 45.0,
            "no_ask": 50.0,
            "spot_age_seconds": None,
            "quote_age_seconds": None,
        }
    ]
    config = OpportunityConfig(
        min_ev=0.01,
        emit_passes=True,
        max_spot_age=10,
        max_quote_age=10,
    )
    rows, counters = build_opportunities_from_snapshots(snapshots, config)
    assert counters["takes"] == 0
    assert counters["passes"] == 1
    assert any("spot_stale" in (row["reason_not_eligible"] or "") for row in rows)
    assert any("quote_stale" in (row["reason_not_eligible"] or "") for row in rows)


def test_build_opportunities_prefers_fee_aware_ev_from_snapshot():
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
            "yes_bid": 45.0,
            "yes_ask": 50.0,
            "no_bid": 45.0,
            "no_ask": 50.0,
            # Fee-aware EV supplied by edge snapshot path.
            "ev_take_yes": 0.02,
            "ev_take_no": -0.12,
            "spot_age_seconds": 5,
            "quote_age_seconds": 5,
            "raw_json": json.dumps(
                {
                    "sigma_source": "ewma",
                    "sigma_ok": True,
                    "sigma_points_used": 20,
                    "min_sigma_points": 10,
                    "sigma_lookback_seconds_used": 4000,
                    "min_sigma_lookback_seconds": 3600,
                }
            ),
        }
    ]
    config = OpportunityConfig(min_ev=0.03, emit_passes=True)
    rows, counters = build_opportunities_from_snapshots(snapshots, config)
    assert counters["takes"] == 0
    assert counters["passes"] == 1
    assert rows[0]["ev_raw"] == 0.02
    assert rows[0]["reason_not_eligible"] == "ev_below_threshold"
