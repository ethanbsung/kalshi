from __future__ import annotations

from pathlib import Path
from typing import Any

from kalshi_bot.events.jetstream import connect_jetstream
from kalshi_bot.events.jetstream_bus import JetStreamEventPublisher
from kalshi_bot.events.jsonl_sink import JsonlEventSink
from kalshi_bot.events.models import EventBase


class EventPublisher:
    """Publish events to zero, one, or multiple sinks."""

    def __init__(
        self,
        *,
        jsonl_sink: JsonlEventSink | None = None,
        jetstream_publisher: JetStreamEventPublisher | None = None,
        jetstream_connection: Any | None = None,
    ) -> None:
        self._jsonl_sink = jsonl_sink
        self._jetstream_publisher = jetstream_publisher
        self._jetstream_connection = jetstream_connection

    @classmethod
    async def create(
        cls,
        *,
        jsonl_path: str | Path | None = None,
        bus_url: str | None = None,
    ) -> "EventPublisher":
        jsonl_sink = JsonlEventSink(jsonl_path) if jsonl_path else None
        jetstream_publisher: JetStreamEventPublisher | None = None
        jetstream_connection: Any | None = None
        if bus_url:
            nc, js = await connect_jetstream(bus_url)
            jetstream_connection = nc
            jetstream_publisher = JetStreamEventPublisher(js)
        return cls(
            jsonl_sink=jsonl_sink,
            jetstream_publisher=jetstream_publisher,
            jetstream_connection=jetstream_connection,
        )

    @property
    def enabled(self) -> bool:
        return (
            (self._jsonl_sink is not None and self._jsonl_sink.enabled)
            or self._jetstream_publisher is not None
        )

    async def publish(self, event: EventBase) -> None:
        errors: list[str] = []
        if self._jsonl_sink is not None and self._jsonl_sink.enabled:
            try:
                await self._jsonl_sink.publish(event)
            except Exception as exc:
                errors.append(f"jsonl:{exc}")
        if self._jetstream_publisher is not None:
            try:
                await self._jetstream_publisher.publish(event)
            except Exception as exc:
                errors.append(f"jetstream:{exc}")
        if errors:
            raise RuntimeError("; ".join(errors))

    async def close(self) -> None:
        if self._jetstream_connection is None:
            return
        drain = getattr(self._jetstream_connection, "drain", None)
        if callable(drain):
            await drain()
        self._jetstream_connection = None
