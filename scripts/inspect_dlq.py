from __future__ import annotations

import argparse
import json

from kalshi_bot.config import load_settings
from kalshi_bot.events import parse_event_dict, subject_for_event
from kalshi_bot.events.jetstream import connect_jetstream


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect and optionally replay dead-letter JetStream events."
    )
    parser.add_argument("--bus-url", type=str, default=None)
    parser.add_argument("--durable", type=str, default="dlq_inspector")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--fetch-timeout-seconds", type=float, default=1.0)
    parser.add_argument("--max-messages", type=int, default=200)
    parser.add_argument(
        "--ack",
        action="store_true",
        help="Ack consumed DLQ messages.",
    )
    parser.add_argument(
        "--replay",
        action="store_true",
        help="Replay valid event payloads back to their canonical subjects.",
    )
    parser.add_argument(
        "--replay-only-type",
        action="append",
        default=None,
        help="Optional event_type filter for replay (repeatable).",
    )
    return parser.parse_args()


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()
    bus_url = args.bus_url or settings.bus_url

    nc, js = await connect_jetstream(bus_url)
    try:
        sub = await js.pull_subscribe(
            subject="dlq.>",
            durable=args.durable,
            stream="DEAD_LETTER",
        )

        replay_filter = set(args.replay_only_type or [])
        processed = 0
        replayed = 0
        parse_failed = 0
        while processed < args.max_messages:
            remaining = args.max_messages - processed
            batch = min(max(int(args.batch_size), 1), remaining)
            try:
                msgs = await sub.fetch(batch=batch, timeout=args.fetch_timeout_seconds)
            except TimeoutError:
                break
            if not msgs:
                break

            for msg in msgs:
                processed += 1
                error_header = ""
                if getattr(msg, "headers", None):
                    error_header = str(msg.headers.get("error") or "")
                payload_text = msg.data.decode("utf-8", errors="replace")
                event_type = "unknown"
                try:
                    raw = json.loads(payload_text)
                    event = parse_event_dict(raw)
                    event_type = event.event_type
                except Exception:
                    parse_failed += 1

                print(
                    "idx={idx} subject={subject} event_type={event_type} "
                    "error={error}".format(
                        idx=processed,
                        subject=msg.subject,
                        event_type=event_type,
                        error=error_header or "none",
                    )
                )

                if (
                    args.replay
                    and event_type != "unknown"
                    and (not replay_filter or event_type in replay_filter)
                ):
                    target_subject = subject_for_event(event_type)
                    await js.publish(
                        subject=target_subject,
                        payload=msg.data,
                        headers={"replayed_from": msg.subject},
                    )
                    replayed += 1

                if args.ack or args.replay:
                    await msg.ack()

        print(
            "processed={processed} replayed={replayed} parse_failed={parse_failed}".format(
                processed=processed,
                replayed=replayed,
                parse_failed=parse_failed,
            )
        )
    finally:
        await nc.drain()
    return 0


def main() -> int:
    import asyncio

    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
