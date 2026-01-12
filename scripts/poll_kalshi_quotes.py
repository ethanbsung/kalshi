from __future__ import annotations

import argparse
import asyncio

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.data.db import init_db
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.kalshi.btc_markets import BTC_SERIES_TICKERS
from kalshi_bot.kalshi.market_filters import (
    fetch_status_counts,
    no_markets_message,
    normalize_series,
)
from kalshi_bot.kalshi.quotes import KalshiQuotePoller, load_market_tickers
from kalshi_bot.kalshi.rest_client import KalshiRestClient


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Poll Kalshi quote snapshots")
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--status", type=str, default=None)
    parser.add_argument(
        "--series",
        action="append",
        default=None,
        help="Repeatable series ticker filter (defaults to BTC series)",
    )
    return parser.parse_args()


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()
    logger = setup_logger(settings.log_path)

    print(f"DB path: {settings.db_path}")

    await init_db(settings.db_path)

    rest_client = KalshiRestClient(
        base_url=settings.kalshi_rest_url,
        api_key_id=settings.kalshi_api_key_id,
        private_key_path=settings.kalshi_private_key_path,
        logger=logger,
    )
    poller = KalshiQuotePoller(
        rest_client=rest_client,
        logger=logger,
        max_concurrency=args.concurrency,
    )

    series = (
        normalize_series(args.series)
        if args.series is not None
        else list(BTC_SERIES_TICKERS)
    )

    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 5000;")
        await conn.commit()

        tickers = await load_market_tickers(conn, args.status, series)
        if not tickers:
            counts = await fetch_status_counts(conn)
            message = no_markets_message(args.status, counts)
            logger.error(
                "kalshi_quotes_no_markets",
                extra={"status": args.status, "series": series, "status_counts": counts},
            )
            print(f"ERROR: {message}")
            return 1

        summary = await poller.run(
            conn,
            run_seconds=args.seconds,
            poll_interval=args.interval,
            status=args.status,
            series=series,
            tickers=tickers,
        )

    print(
        "Quote poll summary: successes={successes} failures={failures} inserted={inserted}".format(
            **summary
        )
    )
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
