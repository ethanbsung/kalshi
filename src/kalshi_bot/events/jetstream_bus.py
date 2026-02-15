from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from kalshi_bot.events.event_contracts import subject_for_event
from kalshi_bot.events.models import EventBase, parse_event_dict


@dataclass(slots=True)
class JetStreamMessageEvent:
    msg: Any
    event: EventBase


class JetStreamEventPublisher:
    def __init__(self, js: Any) -> None:
        self._js = js

    async def publish(self, event: EventBase) -> None:
        subject = subject_for_event(event.event_type)
        headers = {
            "Nats-Msg-Id": str(event.idempotency_key),
            "event_type": event.event_type,
            "schema_version": str(event.schema_version),
        }
        await self._js.publish(
            subject=subject,
            payload=event.model_dump_json().encode("utf-8"),
            headers=headers,
        )


async def subscribe_persistence_consumers(js: Any, durable_prefix: str) -> list[Any]:
    return [
        await js.pull_subscribe(
            subject="market.>",
            durable=f"{durable_prefix}_market",
            stream="MARKET_EVENTS",
        ),
        await js.pull_subscribe(
            subject="strategy.>",
            durable=f"{durable_prefix}_strategy",
            stream="STRATEGY_EVENTS",
        ),
        await js.pull_subscribe(
            subject="execution.>",
            durable=f"{durable_prefix}_execution",
            stream="EXECUTION_EVENTS",
        ),
    ]


async def fetch_message_events(
    subscription: Any,
    *,
    batch: int,
    timeout_seconds: float,
) -> list[JetStreamMessageEvent]:
    msgs = await subscription.fetch(batch=batch, timeout=timeout_seconds)
    parsed: list[JetStreamMessageEvent] = []
    for msg in msgs:
        raw = json.loads(msg.data.decode("utf-8"))
        event = parse_event_dict(raw)
        parsed.append(JetStreamMessageEvent(msg=msg, event=event))
    return parsed
