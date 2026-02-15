from __future__ import annotations

import time
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

from kalshi_bot.events.event_contracts import (
    build_idempotency_key,
    schema_version_for_event,
)


class EventBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_type: str
    schema_version: int = Field(default=1, ge=1)
    ts_event: int = Field(default_factory=lambda: int(time.time()))
    source: str
    idempotency_key: str | None = None
    payload: Any

    @model_validator(mode="after")
    def _enforce_contract(self) -> "EventBase":
        expected = schema_version_for_event(self.event_type)
        if self.schema_version != expected:
            raise ValueError(
                "schema_version mismatch for "
                f"{self.event_type}: got={self.schema_version} expected={expected}"
            )
        if not self.idempotency_key:
            self.idempotency_key = build_idempotency_key(
                self.event_type, _payload_dict(self.payload), self.schema_version
            )
        return self


class SpotTickPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ts: int
    product_id: str
    price: float
    best_bid: float | None = None
    best_ask: float | None = None
    bid_qty: float | None = None
    ask_qty: float | None = None
    sequence_num: int | None = None


class SpotTickEvent(EventBase):
    event_type: Literal["spot_tick"] = "spot_tick"
    payload: SpotTickPayload


class QuoteUpdatePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ts: int
    market_id: str
    source_msg_id: str | None = None
    yes_bid: float | None = None
    yes_ask: float | None = None
    no_bid: float | None = None
    no_ask: float | None = None
    yes_mid: float | None = None
    no_mid: float | None = None
    p_mid: float | None = None


class QuoteUpdateEvent(EventBase):
    event_type: Literal["quote_update"] = "quote_update"
    payload: QuoteUpdatePayload


class MarketLifecyclePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market_id: str
    status: str
    close_ts: int | None = None
    expected_expiration_ts: int | None = None
    expiration_ts: int | None = None
    settlement_ts: int | None = None


class MarketLifecycleEvent(EventBase):
    event_type: Literal["market_lifecycle"] = "market_lifecycle"
    payload: MarketLifecyclePayload


class ContractUpdatePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticker: str
    lower: float | None = None
    upper: float | None = None
    strike_type: str | None = None
    close_ts: int | None = None
    expected_expiration_ts: int | None = None
    expiration_ts: int | None = None
    settled_ts: int | None = None
    outcome: int | None = None


class ContractUpdateEvent(EventBase):
    event_type: Literal["contract_update"] = "contract_update"
    payload: ContractUpdatePayload


class EdgeSnapshotPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    asof_ts: int
    market_id: str
    prob_yes: float
    ev_take_yes: float
    ev_take_no: float
    sigma_annualized: float
    spot_price: float
    quote_ts: int | None = None
    spot_ts: int | None = None
    settlement_ts: int | None = None
    horizon_seconds: int | None = None
    strike: str | None = None
    prob_yes_raw: float | None = None
    yes_bid: float | None = None
    yes_ask: float | None = None
    no_bid: float | None = None
    no_ask: float | None = None
    yes_mid: float | None = None
    no_mid: float | None = None
    spot_age_seconds: int | None = None
    quote_age_seconds: int | None = None
    raw_json: str | None = None


class EdgeSnapshotEvent(EventBase):
    event_type: Literal["edge_snapshot"] = "edge_snapshot"
    payload: EdgeSnapshotPayload


class OpportunityDecisionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ts_eval: int
    market_id: str
    eligible: bool
    would_trade: bool
    side: str | None = None
    reason_not_eligible: str | None = None
    ev_raw: float | None = None
    ev_net: float | None = None
    settlement_ts: int | None = None
    strike: str | None = None
    spot_price: float | None = None
    sigma: float | None = None
    tau: float | None = None
    p_model: float | None = None
    p_market: float | None = None
    best_yes_bid: float | None = None
    best_yes_ask: float | None = None
    best_no_bid: float | None = None
    best_no_ask: float | None = None
    spread: float | None = None
    cost_buffer: float | None = None
    raw_json: str | None = None
    strategy_version: int | None = None


class OpportunityDecisionEvent(EventBase):
    event_type: Literal["opportunity_decision"] = "opportunity_decision"
    payload: OpportunityDecisionPayload


class ExecutionOrderPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ts_order: int
    order_id: str
    market_id: str
    side: str
    action: str = "open"
    quantity: int = Field(default=1, ge=1)
    price_cents: float | None = None
    status: str
    reason: str | None = None
    opportunity_idempotency_key: str | None = None
    paper: bool = True


class ExecutionOrderEvent(EventBase):
    event_type: Literal["execution_order"] = "execution_order"
    payload: ExecutionOrderPayload


class ExecutionFillPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ts_fill: int
    fill_id: str
    order_id: str
    market_id: str
    side: str
    action: str = "open"
    quantity: int = Field(default=1, ge=1)
    price_cents: float | None = None
    outcome: int | None = None
    reason: str | None = None
    paper: bool = True


class ExecutionFillEvent(EventBase):
    event_type: Literal["execution_fill"] = "execution_fill"
    payload: ExecutionFillPayload


Event = (
    SpotTickEvent
    | QuoteUpdateEvent
    | MarketLifecycleEvent
    | ContractUpdateEvent
    | EdgeSnapshotEvent
    | OpportunityDecisionEvent
    | ExecutionOrderEvent
    | ExecutionFillEvent
)

EVENT_MODEL_BY_TYPE: dict[str, type[EventBase]] = {
    "spot_tick": SpotTickEvent,
    "quote_update": QuoteUpdateEvent,
    "market_lifecycle": MarketLifecycleEvent,
    "contract_update": ContractUpdateEvent,
    "edge_snapshot": EdgeSnapshotEvent,
    "opportunity_decision": OpportunityDecisionEvent,
    "execution_order": ExecutionOrderEvent,
    "execution_fill": ExecutionFillEvent,
}

EVENT_ADAPTER = TypeAdapter(Event)


def _payload_dict(payload: Any) -> dict[str, Any]:
    if isinstance(payload, BaseModel):
        return payload.model_dump()
    if isinstance(payload, dict):
        return payload
    raise TypeError(f"Unsupported payload type: {type(payload)!r}")


def parse_event_dict(raw: dict[str, Any]) -> Event:
    event_type = str(raw.get("event_type") or "")
    model = EVENT_MODEL_BY_TYPE.get(event_type)
    if model is None:
        raise ValueError(f"Unknown event_type: {event_type!r}")
    return cast(Event, model.model_validate(raw))
