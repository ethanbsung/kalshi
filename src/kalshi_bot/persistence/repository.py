from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from kalshi_bot.events.models import EventBase


@dataclass(slots=True)
class PersistResult:
    inserted: bool


class EventRepository(Protocol):
    def ensure_schema(self) -> None:
        ...

    def upsert_event(self, event: EventBase) -> PersistResult:
        ...

    def close(self) -> None:
        ...


class InMemoryEventRepository:
    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], dict[str, object]] = {}

    def ensure_schema(self) -> None:
        return

    def upsert_event(self, event: EventBase) -> PersistResult:
        key = (event.event_type, str(event.idempotency_key or ""))
        if key in self._rows:
            return PersistResult(inserted=False)
        self._rows[key] = event.model_dump(mode="python")
        return PersistResult(inserted=True)

    def count(self) -> int:
        return len(self._rows)

    def close(self) -> None:
        return
