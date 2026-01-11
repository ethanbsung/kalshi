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
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Log to stdout in addition to JSONL file",
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
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 5000;")
        await conn.commit()

        dao = Dao(conn)

        rows_since_commit = 0
        last_commit = time.monotonic()
        total_rows = 0
        total_commits = 0

        async def handle_row(row: dict[str, Any]) -> None:
            nonlocal rows_since_commit, last_commit, total_rows, total_commits
            await dao.insert_spot_tick(row)
            rows_since_commit += 1
            total_rows += 1

            now = time.monotonic()
            if rows_since_commit >= 50 or (now - last_commit) >= 1.0:
                await conn.commit()
                rows_since_commit = 0
                last_commit = now
                total_commits += 1

        try:
            await client.run(handle_row, run_seconds=seconds)
        finally:
            if rows_since_commit:
                await conn.commit()
                total_commits += 1
            logger.info(
                "coinbase_run_summary",
                extra={
                    "connect_attempts": client.connect_attempts,
                    "connect_successes": client.connect_successes,
                    "recv_count": client.recv_count,
                    "parsed_row_count": client.parsed_row_count,
                    "error_message_count": client.error_message_count,
                    "close_count": client.close_count,
                    "total_inserted_rows": total_rows,
                    "total_commits": total_commits,
                },
            )


async def run_collector(
    settings: Settings,
    coinbase: bool,
    seconds: int,
    message_source: AsyncIterator[dict[str, Any]] | None = None,
    debug: bool = False,
) -> int:
    logger = setup_logger(settings.log_path, also_stdout=debug)
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
            debug=args.debug,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
