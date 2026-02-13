from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol

from kalshi_bot.events.models import Event


class EventBus(Protocol):
    async def publish(self, event: Event) -> None:
        ...

    def subscribe(
        self, *, event_types: set[str] | None = None, max_queue_size: int = 0
    ) -> asyncio.Queue[Event]:
        ...

    def unsubscribe(self, queue: asyncio.Queue[Event]) -> None:
        ...


@dataclass
class _Subscriber:
    queue: asyncio.Queue[Event]
    event_types: set[str] | None


class InMemoryEventBus:
    def __init__(self) -> None:
        self._subs: dict[int, _Subscriber] = {}
        self._lock = asyncio.Lock()

    def subscribe(
        self, *, event_types: set[str] | None = None, max_queue_size: int = 0
    ) -> asyncio.Queue[Event]:
        queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=max_queue_size)
        sub = _Subscriber(queue=queue, event_types=set(event_types or []))
        # subscribe is sync for caller ergonomics; internal map write is cheap.
        self._subs[id(queue)] = sub
        return queue

    def unsubscribe(self, queue: asyncio.Queue[Event]) -> None:
        self._subs.pop(id(queue), None)

    async def publish(self, event: Event) -> None:
        async with self._lock:
            subs = list(self._subs.values())
        for sub in subs:
            if sub.event_types and event.event_type not in sub.event_types:
                continue
            await sub.queue.put(event)
