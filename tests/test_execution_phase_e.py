from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from kalshi_bot.events import (
    ExecutionFillEvent,
    ExecutionOrderEvent,
    parse_event_dict,
)


def _load_paper_execution_module() -> object:
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_paper_execution.py"
    spec = importlib.util.spec_from_file_location("run_paper_execution", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_parse_execution_events_roundtrip() -> None:
    order = ExecutionOrderEvent(
        source="test",
        payload={
            "ts_order": 1_700_000_000,
            "order_id": "paper:abc",
            "market_id": "KXBTC-TEST",
            "side": "YES",
            "status": "filled",
        },
    )
    fill = ExecutionFillEvent(
        source="test",
        payload={
            "ts_fill": 1_700_000_001,
            "fill_id": "paper:abc:open",
            "order_id": "paper:abc",
            "market_id": "KXBTC-TEST",
            "side": "YES",
        },
    )

    parsed_order = parse_event_dict(order.model_dump(mode="python"))
    parsed_fill = parse_event_dict(fill.model_dump(mode="python"))

    assert isinstance(parsed_order, ExecutionOrderEvent)
    assert isinstance(parsed_fill, ExecutionFillEvent)
    assert parsed_order.payload.order_id == "paper:abc"
    assert parsed_fill.payload.fill_id == "paper:abc:open"


def test_paper_execution_state_risk_gates() -> None:
    module = _load_paper_execution_module()
    state = module._PaperExecutionState()

    state.accept(market_id="KXBTC-TEST", side="YES", ts_open=100)
    assert (
        state.reject_reason(
            market_id="KXBTC-TEST",
            side="NO",
            now_ts=110,
            cooldown_seconds=120,
            max_open_positions=10,
            kill_switch_active=False,
        )
        == "position_open_opposite_side"
    )

    state.close_market("KXBTC-TEST")
    assert (
        state.reject_reason(
            market_id="KXBTC-TEST",
            side="YES",
            now_ts=150,
            cooldown_seconds=120,
            max_open_positions=10,
            kill_switch_active=False,
        )
        == "cooldown_active"
    )
    assert (
        state.reject_reason(
            market_id="KXBTC-TEST",
            side="YES",
            now_ts=260,
            cooldown_seconds=120,
            max_open_positions=10,
            kill_switch_active=False,
        )
        is None
    )


def test_paper_execution_state_max_open_positions() -> None:
    module = _load_paper_execution_module()
    state = module._PaperExecutionState()
    state.accept(market_id="KXBTC-A", side="YES", ts_open=100)

    reason = state.reject_reason(
        market_id="KXBTC-B",
        side="YES",
        now_ts=101,
        cooldown_seconds=0,
        max_open_positions=1,
        kill_switch_active=False,
    )
    assert reason == "max_open_positions"
