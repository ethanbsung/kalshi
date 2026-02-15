from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from kalshi_bot.events.event_contracts import JetStreamStreamSpec, default_stream_specs


@dataclass(slots=True)
class JetStreamSetupResult:
    name: str
    created: bool


async def connect_jetstream(bus_url: str) -> tuple[Any, Any]:
    try:
        import nats
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "nats-py is required for JetStream setup. Install with: pip install nats-py"
        ) from exc
    async def _error_cb(_exc: Exception) -> None:
        return

    try:
        nc = await nats.connect(
            servers=[bus_url],
            connect_timeout=2.0,
            allow_reconnect=False,
            max_reconnect_attempts=0,
            error_cb=_error_cb,
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to connect to JetStream at {bus_url}. "
            "Ensure NATS is running and BUS_URL is correct."
        ) from exc
    return nc, nc.jetstream()


def _stream_config(spec: JetStreamStreamSpec, retention_hours: int) -> dict[str, Any]:
    # nats-py StreamConfig expects duration fields in seconds and internally
    # serializes them to nanoseconds.
    max_age_seconds = float(max(int(retention_hours), 1) * 3600)
    return {
        "name": spec.name,
        "subjects": list(spec.subjects),
        "description": spec.description,
        "retention": "limits",
        "storage": "file",
        "discard": "old",
        "max_age": max_age_seconds,
        "duplicate_window": 120.0,
    }


def _stream_config_object(cfg: dict[str, Any]) -> Any | None:
    """Build a typed StreamConfig when available for nats-py compatibility."""
    try:
        from nats.js.api import (
            DiscardPolicy,
            RetentionPolicy,
            StorageType,
            StreamConfig,
        )
    except Exception:
        return None
    return StreamConfig(
        name=cfg["name"],
        subjects=cfg["subjects"],
        description=cfg.get("description"),
        retention=RetentionPolicy.LIMITS,
        storage=StorageType.FILE,
        discard=DiscardPolicy.OLD,
        max_age=cfg["max_age"],
        duplicate_window=cfg["duplicate_window"],
    )


async def ensure_streams(
    js: Any,
    *,
    retention_hours: int,
    stream_specs: list[JetStreamStreamSpec] | None = None,
) -> list[JetStreamSetupResult]:
    specs = stream_specs or default_stream_specs()
    results: list[JetStreamSetupResult] = []
    for spec in specs:
        cfg = _stream_config(spec, retention_hours)
        typed_cfg = _stream_config_object(cfg)
        created = False
        try:
            await js.stream_info(spec.name)
            if typed_cfg is not None:
                await js.update_stream(config=typed_cfg)
            else:
                await js.update_stream(**cfg)
        except Exception:
            if typed_cfg is not None:
                await js.add_stream(config=typed_cfg)
            else:
                await js.add_stream(**cfg)
            created = True
        results.append(JetStreamSetupResult(name=spec.name, created=created))
    return results
