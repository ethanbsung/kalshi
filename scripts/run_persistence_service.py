from __future__ import annotations

import argparse
import asyncio
import json
import signal
import time
from dataclasses import asdict, dataclass

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


async def _consumer_lag_snapshot(subs: list[object]) -> dict[str, int | None]:
    snapshot: dict[str, int | None] = {}
    for idx, sub in enumerate(subs):
        key = ""
        pending: int | None = None
        consumer_info: object | None = None
        try:
            consumer_info = await sub.consumer_info()
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
        "--once",
        action="store_true",
        help="Process one fetch cycle and exit.",
    )
    return parser.parse_args()


async def _publish_dlq(js: object, subject: str, payload: bytes, error: str) -> None:
    headers = {"error": error[:200]}
    await js.publish(subject=subject, payload=payload, headers=headers)


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()

    bus_url = args.bus_url or settings.bus_url
    pg_dsn = args.pg_dsn or settings.pg_dsn
    if not pg_dsn:
        raise RuntimeError("PG_DSN is required for persistence service")

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
                except Exception:
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
                        await msg.ack()

            now = time.monotonic()
            if (now - last_log) >= args.log_every_seconds:
                lag = await _consumer_lag_snapshot(subs)
                print(
                    "processed={processed} inserted={inserted} duplicates={duplicates} "
                    "parse_errors={parse_errors} persist_errors={persist_errors} "
                    "dlq_published={dlq_published} consumer_lag={lag}".format(
                        lag=lag, **asdict(counters)
                    )
                )
                last_log = now

            if args.once:
                break
    finally:
        service.close()
        await nc.drain()

    print(
        "final processed={processed} inserted={inserted} duplicates={duplicates} "
        "parse_errors={parse_errors} persist_errors={persist_errors} "
        "dlq_published={dlq_published}".format(**asdict(counters))
    )
    return 0


def main() -> int:
    try:
        return asyncio.run(_run())
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
