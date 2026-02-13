from __future__ import annotations

import argparse
import asyncio

from kalshi_bot.config import load_settings
from kalshi_bot.events.jetstream import connect_jetstream, ensure_streams


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create/update JetStream streams used by live trading services."
    )
    parser.add_argument(
        "--bus-url",
        type=str,
        default=None,
        help="NATS URL override (default from BUS_URL).",
    )
    parser.add_argument(
        "--retention-hours",
        type=int,
        default=None,
        help="Stream retention in hours (default from BUS_STREAM_RETENTION_HOURS).",
    )
    return parser.parse_args()


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()

    bus_url = args.bus_url or settings.bus_url
    retention_hours = (
        args.retention_hours
        if args.retention_hours is not None
        else settings.bus_stream_retention_hours
    )

    try:
        nc, js = await connect_jetstream(bus_url)
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 2
    try:
        results = await ensure_streams(
            js,
            retention_hours=retention_hours,
        )
        print(f"bus_url={bus_url} retention_hours={retention_hours}")
        for result in results:
            status = "created" if result.created else "updated"
            print(f"stream={result.name} status={status}")
    finally:
        await nc.drain()
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
