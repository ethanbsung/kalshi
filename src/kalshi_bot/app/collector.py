from __future__ import annotations

import argparse
import asyncio
import logging
import time
from typing import Any, AsyncIterator

import aiosqlite

from kalshi_bot.config import Settings, load_settings
from kalshi_bot.data import Dao, init_db
from kalshi_bot.feeds.coinbase_ws import CoinbaseWsClient
from kalshi_bot.infra import setup_logger


def _build_parser(default_seconds: int) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Kalshi BTC collector")
    parser.add_argument(
        "--coinbase",
        action="store_true",
        help="Enable Coinbase Advanced Trade ticker ingestion",
    )
    parser.add_argument(
        "--seconds",
        type=int,
        default=default_seconds,
        help="Collector runtime in seconds",
    )
    return parser


async def _run_coinbase(
    settings: Settings,
    logger: logging.Logger,
    seconds: int,
    message_source: AsyncIterator[dict[str, Any]] | None = None,
) -> None:
    client = CoinbaseWsClient(
        ws_url=settings.coinbase_ws_url,
        product_id=settings.coinbase_product_id,
        stale_seconds=settings.coinbase_stale_seconds,
        logger=logger,
        message_source=message_source,
    )

    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        dao = Dao(conn)
        rows_since_commit = 0
        last_commit = time.monotonic()

        async def handle_row(row: dict[str, Any]) -> None:
            nonlocal rows_since_commit, last_commit
            await dao.insert_spot_tick(row)
            rows_since_commit += 1
            now = time.monotonic()
            if rows_since_commit >= 50 or now - last_commit >= 1.0:
                await conn.commit()
                rows_since_commit = 0
                last_commit = now

        await client.run(handle_row, run_seconds=seconds)
        if rows_since_commit:
            await conn.commit()


async def run_collector(
    settings: Settings,
    coinbase: bool,
    seconds: int,
    message_source: AsyncIterator[dict[str, Any]] | None = None,
) -> int:
    logger = setup_logger(settings.log_path)
    await init_db(settings.db_path)
    logger.info(
        "startup",
        extra={
            "db_path": str(settings.db_path),
            "log_path": str(settings.log_path),
            "trading_enabled": settings.trading_enabled,
            "kalshi_env": settings.kalshi_env,
            "coinbase_enabled": coinbase,
            "collector_seconds": seconds,
        },
    )

    if coinbase:
        await _run_coinbase(
            settings, logger, seconds, message_source=message_source
        )
    return 0


def main() -> int:
    settings = load_settings()
    parser = _build_parser(settings.collector_seconds)
    args = parser.parse_args()
    return asyncio.run(
        run_collector(
            settings=settings,
            coinbase=args.coinbase,
            seconds=args.seconds,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
