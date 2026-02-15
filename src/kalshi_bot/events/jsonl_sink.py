from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from kalshi_bot.events.models import EventBase


class JsonlEventSink:
    """Best-effort JSONL sink for shadow event publishing."""

    def __init__(self, path: str | Path | None) -> None:
        self._path = Path(path) if path else None
        self._lock = asyncio.Lock()
        if self._path is not None:
            self._path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def enabled(self) -> bool:
        return self._path is not None

    async def publish(self, event: EventBase) -> None:
        if self._path is None:
            return
        line = event.model_dump_json()
        await self._write_line(line)

    async def publish_dict(self, event: dict[str, Any]) -> None:
        if self._path is None:
            return
        import json

        line = json.dumps(event, separators=(",", ":"), sort_keys=True)
        await self._write_line(line)

    async def _write_line(self, line: str) -> None:
        if self._path is None:
            return
        async with self._lock:
            await asyncio.to_thread(self._append_line_sync, line)

    def _append_line_sync(self, line: str) -> None:
        path = self._path
        if path is None:
            raise RuntimeError("JSONL sink path is not configured")
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.write("\n")
