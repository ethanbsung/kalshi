from __future__ import annotations

import argparse
import asyncio
import json
import signal
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

from kalshi_bot.config import load_settings
from kalshi_bot.events import dlq_subject_for_event, parse_event_dict
from kalshi_bot.events.jetstream import connect_jetstream
from kalshi_bot.persistence import PersistenceService, PostgresEventRepository


@dataclass(slots=True)
class Counters:
    processed: int = 0
    inserted: int = 0
    duplicates: int = 0
    parse_errors: int = 0
    persist_errors: int = 0
    dlq_published: int = 0


def _coerce_headers(headers: object | None) -> dict[str, str]:
    if headers is None:
        return {}
    if isinstance(headers, dict):
        return {str(k): str(v) for k, v in headers.items()}
    try:
        items = cast(Any, headers).items()
    except Exception:
        return {}
    return {str(k): str(v) for k, v in items}


def _msg_meta(msg: object) -> dict[str, Any]:
    metadata = getattr(msg, "metadata", None)
    if metadata is None:
        return {}
    out: dict[str, Any] = {}
    for key in (
        "num_pending",
        "num_delivered",
        "timestamp",
        "stream",
        "consumer",
        "sequence",
    ):
        value = getattr(metadata, key, None)
        if value is not None:
            out[key] = value
    sequence = out.get("sequence")
    if sequence is not None:
        out["sequence"] = {
            "stream": getattr(sequence, "stream", None),
            "consumer": getattr(sequence, "consumer", None),
        }
    timestamp = out.get("timestamp")
    if timestamp is not None:
        out["timestamp"] = str(timestamp)
    return out


def _append_jsonl(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(obj, separators=(",", ":"), sort_keys=True))
        handle.write("\n")


async def _consumer_lag_snapshot(subs: list[object]) -> dict[str, int | None]:
    snapshot: dict[str, int | None] = {}
    for idx, sub in enumerate(subs):
        sub_any = cast(Any, sub)
        key = ""
        pending: int | None = None
        consumer_info: object | None = None
        try:
            consumer_info = await sub_any.consumer_info()
            durable_name = getattr(consumer_info, "name", None)
            if durable_name is None and isinstance(consumer_info, dict):
                durable_name = consumer_info.get("name")
            if durable_name:
                key = str(durable_name)
            pending_value = getattr(consumer_info, "num_pending", None)
            if pending_value is None and isinstance(consumer_info, dict):
                pending_value = consumer_info.get("num_pending")
            if pending_value is not None:
                pending = int(pending_value)
        except Exception:
            pending = None
        if not key:
            key = str(getattr(sub, "_durable", "") or "")
        if not key:
            key = str(
                getattr(sub, "subject", "")
                or getattr(sub, "_subject", "")
                or f"consumer_{idx}"
            )
        key = key.replace(" ", "_")
        if key in snapshot:
            key = f"{key}_{idx}"
        snapshot[key] = pending
    return snapshot


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume JetStream events and persist idempotently to Postgres."
    )
    parser.add_argument("--bus-url", type=str, default=None)
    parser.add_argument("--pg-dsn", type=str, default=None)
    parser.add_argument("--durable-prefix", type=str, default="svc_persistence")
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--fetch-timeout-seconds", type=float, default=1.0)
    parser.add_argument("--log-every-seconds", type=float, default=30.0)
    parser.add_argument(
        "--metrics-log-path",
        type=str,
        default=None,
        help="Optional JSONL path for periodic metric snapshots.",
    )
    parser.add_argument(
        "--error-log-path",
        type=str,
        default=None,
        help="Optional JSONL path for parse/persist error details.",
    )
    parser.add_argument(
        "--error-payload-preview-chars",
        type=int,
        default=2000,
        help="Max payload characters captured per parse/persist error log row.",
    )
    parser.add_argument(
        "--lag-alert-threshold",
        type=int,
        default=None,
        help=(
            "Alert when market consumer lag exceeds this value "
            "(default BUS_CONSUMER_LAG_ALERT_THRESHOLD)."
        ),
    )
    parser.add_argument(
        "--lag-alert-cooldown-seconds",
        type=float,
        default=60.0,
        help="Minimum seconds between repeated lag alerts.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process one fetch cycle and exit.",
    )
    return parser.parse_args()


async def _publish_dlq(js: object, subject: str, payload: bytes, error: str) -> None:
    headers = {"error": error[:200]}
    await cast(Any, js).publish(subject=subject, payload=payload, headers=headers)


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()

    bus_url = args.bus_url or settings.bus_url
    pg_dsn = args.pg_dsn or settings.pg_dsn
    if not pg_dsn:
        raise RuntimeError("PG_DSN is required for persistence service")
    metrics_log_path = Path(args.metrics_log_path) if args.metrics_log_path else None
    error_log_path = Path(args.error_log_path) if args.error_log_path else None
    payload_preview_chars = max(int(args.error_payload_preview_chars or 0), 0)
    lag_alert_threshold = (
        int(args.lag_alert_threshold)
        if args.lag_alert_threshold is not None
        else int(settings.bus_consumer_lag_alert_threshold)
    )
    lag_alert_cooldown = max(float(args.lag_alert_cooldown_seconds), 1.0)

    repo = PostgresEventRepository(pg_dsn)
    service = PersistenceService(repo)
    try:
        service.ensure_schema()
    except Exception as exc:
        raise RuntimeError(
            "Failed to connect to Postgres for persistence schema setup. "
            "Ensure PG_DSN is valid and Postgres is running."
        ) from exc

    nc, js = await connect_jetstream(bus_url)
    subs = [
        await js.pull_subscribe(
            subject="market.>",
            durable=f"{args.durable_prefix}_market",
            stream="MARKET_EVENTS",
        ),
        await js.pull_subscribe(
            subject="strategy.>",
            durable=f"{args.durable_prefix}_strategy",
            stream="STRATEGY_EVENTS",
        ),
        await js.pull_subscribe(
            subject="execution.>",
            durable=f"{args.durable_prefix}_execution",
            stream="EXECUTION_EVENTS",
        ),
    ]

    stop_event = asyncio.Event()

    def _stop() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _stop)

    counters = Counters()
    last_log = time.monotonic()
    last_fetch_error_log = 0.0
    last_lag_alert_log = 0.0

    try:
        while not stop_event.is_set():
            for sub in subs:
                try:
                    msgs = await sub.fetch(
                        batch=args.batch_size,
                        timeout=args.fetch_timeout_seconds,
                    )
                except TimeoutError:
                    continue
                except Exception as exc:
                    now = time.monotonic()
                    if (now - last_fetch_error_log) >= 10.0:
                        print(
                            "fetch_error type={type_name} err={err}".format(
                                type_name=type(exc).__name__,
                                err=exc,
                            )
                        )
                        last_fetch_error_log = now
                    continue

                for msg in msgs:
                    counters.processed += 1
                    try:
                        raw = json.loads(msg.data.decode("utf-8"))
                        event = parse_event_dict(raw)
                    except Exception as exc:
                        counters.parse_errors += 1
                        await _publish_dlq(
                            js,
                            subject="dlq.invalid_event",
                            payload=msg.data,
                            error=f"parse_error:{exc}",
                        )
                        counters.dlq_published += 1
                        if error_log_path is not None:
                            row = {
                                "ts": int(time.time()),
                                "kind": "parse_error",
                                "subject": str(getattr(msg, "subject", "")),
                                "error_type": type(exc).__name__,
                                "error": str(exc),
                                "payload_len": len(msg.data),
                                "payload_preview": msg.data.decode(
                                    "utf-8", errors="replace"
                                )[:payload_preview_chars],
                                "headers": _coerce_headers(getattr(msg, "headers", None)),
                                "meta": _msg_meta(msg),
                            }
                            await asyncio.to_thread(_append_jsonl, error_log_path, row)
                        await msg.ack()
                        continue

                    try:
                        inserted = service.persist_event(event)
                        if inserted:
                            counters.inserted += 1
                        else:
                            counters.duplicates += 1
                        await msg.ack()
                    except Exception as exc:
                        counters.persist_errors += 1
                        await _publish_dlq(
                            js,
                            subject=dlq_subject_for_event(event.event_type),
                            payload=msg.data,
                            error=f"persist_error:{exc}",
                        )
                        counters.dlq_published += 1
                        if error_log_path is not None:
                            row = {
                                "ts": int(time.time()),
                                "kind": "persist_error",
                                "event_type": event.event_type,
                                "idempotency_key": str(event.idempotency_key or ""),
                                "subject": str(getattr(msg, "subject", "")),
                                "error_type": type(exc).__name__,
                                "error": str(exc),
                                "payload_preview": msg.data.decode(
                                    "utf-8", errors="replace"
                                )[:payload_preview_chars],
                                "headers": _coerce_headers(getattr(msg, "headers", None)),
                                "meta": _msg_meta(msg),
                            }
                            await asyncio.to_thread(_append_jsonl, error_log_path, row)
                        await msg.ack()

            now = time.monotonic()
            if (now - last_log) >= args.log_every_seconds:
                lag = await _consumer_lag_snapshot(subs)
                market_lag = lag.get(f"{args.durable_prefix}_market")
                if isinstance(market_lag, int) and lag_alert_threshold > 0:
                    if market_lag > lag_alert_threshold and (
                        (now - last_lag_alert_log) >= lag_alert_cooldown
                    ):
                        print(
                            "ALERT market_consumer_lag lag={lag} threshold={threshold}".format(
                                lag=market_lag,
                                threshold=lag_alert_threshold,
                            )
                        )
                        last_lag_alert_log = now
                line = (
                    "processed={processed} inserted={inserted} duplicates={duplicates} "
                    "parse_errors={parse_errors} persist_errors={persist_errors} "
                    "dlq_published={dlq_published} consumer_lag={lag}".format(
                        lag=lag, **asdict(counters)
                    )
                )
                print(line)
                if metrics_log_path is not None:
                    metric_row = {
                        "ts": int(time.time()),
                        "kind": "persistence_metrics",
                        **asdict(counters),
                        "consumer_lag": lag,
                    }
                    await asyncio.to_thread(_append_jsonl, metrics_log_path, metric_row)
                last_log = now

            if args.once:
                break
    finally:
        service.close()
        await nc.drain()

    final_line = (
        "final processed={processed} inserted={inserted} duplicates={duplicates} "
        "parse_errors={parse_errors} persist_errors={persist_errors} "
        "dlq_published={dlq_published}".format(**asdict(counters))
    )
    print(final_line)
    if metrics_log_path is not None:
        final_row = {
            "ts": int(time.time()),
            "kind": "persistence_final",
            **asdict(counters),
        }
        await asyncio.to_thread(_append_jsonl, metrics_log_path, final_row)
    return 0


def main() -> int:
    try:
        return asyncio.run(_run())
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
