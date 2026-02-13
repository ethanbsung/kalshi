from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from kalshi_bot.events.models import EventBase
from kalshi_bot.persistence.repository import EventRepository


@dataclass(slots=True)
class PersistenceStats:
    processed: int = 0
    inserted: int = 0
    duplicates: int = 0
    errors: int = 0


class PersistenceService:
    def __init__(
        self,
        repository: EventRepository,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._repo = repository
        self._log = logger or logging.getLogger(__name__)

    def ensure_schema(self) -> None:
        self._repo.ensure_schema()

    def persist_event(self, event: EventBase) -> bool:
        result = self._repo.upsert_event(event)
        return result.inserted

    async def run_queue_consumer(
        self,
        queue: asyncio.Queue[EventBase],
        *,
        stop_event: asyncio.Event,
        poll_timeout_seconds: float = 1.0,
    ) -> PersistenceStats:
        stats = PersistenceStats()
        while not stop_event.is_set():
            try:
                event = await asyncio.wait_for(
                    queue.get(), timeout=poll_timeout_seconds
                )
            except asyncio.TimeoutError:
                continue
            stats.processed += 1
            try:
                inserted = self.persist_event(event)
                if inserted:
                    stats.inserted += 1
                else:
                    stats.duplicates += 1
            except Exception:
                stats.errors += 1
                self._log.exception(
                    "persistence_event_failed",
                    extra={
                        "event_type": event.event_type,
                        "idempotency_key": event.idempotency_key,
                    },
                )
        return stats

    def close(self) -> None:
        self._repo.close()
