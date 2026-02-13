from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

EVENT_SCHEMA_VERSIONS: dict[str, int] = {
    "spot_tick": 1,
    "quote_update": 1,
    "market_lifecycle": 1,
    "contract_update": 1,
    "edge_snapshot": 1,
    "opportunity_decision": 1,
}

EVENT_SUBJECTS: dict[str, str] = {
    "spot_tick": "market.spot_ticks",
    "quote_update": "market.quote_updates",
    "market_lifecycle": "market.lifecycle",
    "contract_update": "market.contract_updates",
    "edge_snapshot": "strategy.edge_snapshots",
    "opportunity_decision": "strategy.opportunity_decisions",
}

DLQ_SUBJECT_PREFIX = "dlq"


@dataclass(frozen=True)
class JetStreamStreamSpec:
    name: str
    subjects: tuple[str, ...]
    description: str


def schema_version_for_event(event_type: str) -> int:
    if event_type not in EVENT_SCHEMA_VERSIONS:
        raise KeyError(f"Unknown event type: {event_type}")
    return EVENT_SCHEMA_VERSIONS[event_type]


def subject_for_event(event_type: str) -> str:
    if event_type not in EVENT_SUBJECTS:
        raise KeyError(f"Unknown event type: {event_type}")
    return EVENT_SUBJECTS[event_type]


def dlq_subject_for_event(event_type: str) -> str:
    subject = subject_for_event(event_type)
    return f"{DLQ_SUBJECT_PREFIX}.{subject}"


def _stable_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _coerce_part(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def _all_present(parts: list[str | None]) -> bool:
    return all(part is not None for part in parts)


def _parts_for_idempotency(event_type: str, payload: dict[str, Any]) -> list[str]:
    if event_type == "spot_tick":
        parts = [
            _coerce_part(payload.get("product_id")),
            _coerce_part(payload.get("ts")),
            _coerce_part(payload.get("sequence_num")),
        ]
        if _all_present(parts):
            return [part for part in parts if part is not None]
        return [_stable_json(payload)]
    if event_type == "quote_update":
        parts = [
            _coerce_part(payload.get("market_id")),
            _coerce_part(payload.get("ts")),
            _coerce_part(payload.get("source_msg_id")),
        ]
        if _all_present(parts):
            return [part for part in parts if part is not None]
        return [_stable_json(payload)]
    if event_type == "market_lifecycle":
        parts = [
            _coerce_part(payload.get("market_id")),
            _coerce_part(payload.get("status")),
            _coerce_part(payload.get("close_ts")),
            _coerce_part(payload.get("expected_expiration_ts")),
            _coerce_part(payload.get("expiration_ts")),
            _coerce_part(payload.get("settlement_ts")),
        ]
        if parts[0] is not None and parts[1] is not None:
            return [part or "" for part in parts]
        return [_stable_json(payload)]
    if event_type == "contract_update":
        parts = [
            _coerce_part(payload.get("ticker")),
            _coerce_part(payload.get("close_ts")),
            _coerce_part(payload.get("expected_expiration_ts")),
            _coerce_part(payload.get("expiration_ts")),
            _coerce_part(payload.get("settled_ts")),
            _coerce_part(payload.get("outcome")),
        ]
        if parts[0] is not None:
            return [part or "" for part in parts]
        return [_stable_json(payload)]
    if event_type == "edge_snapshot":
        parts = [
            _coerce_part(payload.get("asof_ts")),
            _coerce_part(payload.get("market_id")),
            _coerce_part(payload.get("strategy_version")) or "v1",
        ]
        if parts[0] is not None and parts[1] is not None:
            return [part for part in parts if part is not None]
        return [_stable_json(payload)]
    if event_type == "opportunity_decision":
        parts = [
            _coerce_part(payload.get("ts_eval")),
            _coerce_part(payload.get("market_id")),
            _coerce_part(payload.get("side")),
            _coerce_part(payload.get("strategy_version")) or "v1",
        ]
        if parts[0] is not None and parts[1] is not None and parts[2] is not None:
            return [part for part in parts if part is not None]
        return [_stable_json(payload)]
    return [_stable_json(payload)]


def build_idempotency_key(
    event_type: str, payload: dict[str, Any], schema_version: int
) -> str:
    parts = _parts_for_idempotency(event_type, payload)
    digest_source = "|".join(parts).encode("utf-8")
    digest = hashlib.sha256(digest_source).hexdigest()[:24]
    return f"{event_type}:v{schema_version}:{digest}"


def default_stream_specs() -> list[JetStreamStreamSpec]:
    return [
        JetStreamStreamSpec(
            name="MARKET_EVENTS",
            subjects=(
                "market.spot_ticks",
                "market.quote_updates",
                "market.lifecycle",
                "market.contract_updates",
            ),
            description="Spot, quote, and market lifecycle inputs",
        ),
        JetStreamStreamSpec(
            name="STRATEGY_EVENTS",
            subjects=(
                "strategy.edge_snapshots",
                "strategy.opportunity_decisions",
            ),
            description="Strategy outputs",
        ),
        JetStreamStreamSpec(
            name="EXECUTION_EVENTS",
            subjects=("execution.orders", "execution.fills"),
            description="Execution events",
        ),
        JetStreamStreamSpec(
            name="DEAD_LETTER",
            subjects=("dlq.>",),
            description="Dead-letter events",
        ),
    ]
