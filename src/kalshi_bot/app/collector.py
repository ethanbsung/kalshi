from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sqlite3
import time
from typing import Any, AsyncIterator

import aiosqlite

from kalshi_bot.config import Settings, load_settings
from kalshi_bot.data import Dao, init_db
from kalshi_bot.events import EventPublisher, SpotTickEvent
from kalshi_bot.feeds.coinbase_ws import CoinbaseWsClient
from kalshi_bot.kalshi import KalshiRestClient, KalshiWsClient
from kalshi_bot.infra import setup_logger


def _build_parser(default_seconds: int) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Kalshi BTC collector")
    parser.add_argument(
        "--coinbase",
        action="store_true",
        help="Enable Coinbase Advanced Trade ticker ingestion",
    )
    parser.add_argument(
        "--kalshi",
        action="store_true",
        help="Enable Kalshi market data ingestion",
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
    parser.add_argument(
        "--events-jsonl-path",
        type=str,
        default=None,
        help="Optional JSONL file path for shadow event publishing.",
    )
    parser.add_argument(
        "--events-bus",
        action="store_true",
        help="Publish spot_tick events to JetStream.",
    )
    parser.add_argument(
        "--events-bus-url",
        type=str,
        default=None,
        help="JetStream bus URL override (default BUS_URL).",
    )
    parser.add_argument(
        "--coinbase-publish-only",
        action="store_true",
        help="Coinbase ingest publishes events only and skips SQLite writes.",
    )
    return parser


async def _run_coinbase(
    settings: Settings,
    logger: logging.Logger,
    seconds: int,
    message_source: AsyncIterator[dict[str, Any]] | None = None,
    events_jsonl_path: str | None = None,
    events_bus_url: str | None = None,
    publish_only: bool = False,
) -> None:
    client = CoinbaseWsClient(
        ws_url=settings.coinbase_ws_url,
        product_id=settings.coinbase_product_id,
        stale_seconds=settings.coinbase_stale_seconds,
        logger=logger,
        message_source=message_source,
    )

    event_sink = await EventPublisher.create(
        jsonl_path=events_jsonl_path,
        bus_url=events_bus_url,
    )
    if publish_only:
        if not event_sink.enabled:
            await event_sink.close()
            raise RuntimeError(
                "coinbase publish-only mode requires an event sink "
                "(--events-jsonl-path and/or --events-bus)."
            )
        total_rows = 0
        event_publish_failures = 0

        async def handle_row(row: dict[str, Any]) -> None:
            nonlocal total_rows, event_publish_failures
            total_rows += 1
            try:
                await event_sink.publish(
                    SpotTickEvent(
                        source="collector.coinbase",
                        payload={
                            "ts": int(row["ts"]),
                            "product_id": str(row["product_id"]),
                            "price": float(row["price"]),
                            "best_bid": (
                                float(row["best_bid"])
                                if row["best_bid"] is not None
                                else None
                            ),
                            "best_ask": (
                                float(row["best_ask"])
                                if row["best_ask"] is not None
                                else None
                            ),
                            "bid_qty": (
                                float(row["bid_qty"])
                                if row["bid_qty"] is not None
                                else None
                            ),
                            "ask_qty": (
                                float(row["ask_qty"])
                                if row["ask_qty"] is not None
                                else None
                            ),
                            "sequence_num": (
                                int(row["sequence_num"])
                                if row["sequence_num"] is not None
                                else None
                            ),
                        },
                    )
                )
            except Exception:
                event_publish_failures += 1

        try:
            await client.run(handle_row, run_seconds=seconds)
        finally:
            logger.info(
                "coinbase_run_summary",
                extra={
                    "connect_attempts": client.connect_attempts,
                    "connect_successes": client.connect_successes,
                    "recv_count": client.recv_count,
                    "parsed_row_count": client.parsed_row_count,
                    "error_message_count": client.error_message_count,
                    "close_count": client.close_count,
                    "publish_only": True,
                    "rows_seen": total_rows,
                    "event_publish_failures": event_publish_failures,
                },
            )
            await event_sink.close()
        return

    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 15000;")
        await conn.commit()

        spot_columns = Dao.SPOT_TICK_COLUMNS
        spot_insert_sql = (
            "INSERT INTO spot_ticks ("
            + ", ".join(spot_columns)
            + ") VALUES ("
            + ", ".join("?" for _ in spot_columns)
            + ")"
        )

        rows_since_commit = 0
        last_commit = time.monotonic()
        total_rows = 0
        total_commits = 0
        event_publish_failures = 0
        lock_insert_retries = 0
        dropped_rows_lock = 0
        commit_failures = 0
        last_lock_log_ts = 0.0
        def _is_locked(exc: sqlite3.OperationalError) -> bool:
            return "locked" in str(exc).lower()

        async def _commit_with_retry() -> bool:
            nonlocal total_commits, commit_failures, last_commit
            backoffs = (0.05, 0.1, 0.2, 0.5, 1.0, 2.0)
            for attempt in range(len(backoffs) + 1):
                try:
                    await conn.commit()
                    total_commits += 1
                    last_commit = time.monotonic()
                    return True
                except sqlite3.OperationalError as exc:
                    if not _is_locked(exc):
                        raise
                    commit_failures += 1
                    if attempt < len(backoffs):
                        await asyncio.sleep(backoffs[attempt])
            return False

        async def handle_row(row: dict[str, Any]) -> None:
            nonlocal rows_since_commit, total_rows, lock_insert_retries, dropped_rows_lock, last_lock_log_ts
            inserted = False
            backoffs = (0.05, 0.1, 0.2, 0.5, 1.0)
            for attempt in range(len(backoffs) + 1):
                try:
                    await conn.execute(
                        spot_insert_sql,
                        tuple(row[col] for col in spot_columns),
                    )
                    inserted = True
                    break
                except sqlite3.OperationalError as exc:
                    if not _is_locked(exc):
                        raise
                    lock_insert_retries += 1
                    if attempt < len(backoffs):
                        await asyncio.sleep(backoffs[attempt])
            if not inserted:
                dropped_rows_lock += 1
                now = time.monotonic()
                if (now - last_lock_log_ts) >= 60.0:
                    logger.warning(
                        "coinbase_spot_insert_locked",
                        extra={
                            "dropped_rows_lock": dropped_rows_lock,
                            "lock_insert_retries": lock_insert_retries,
                        },
                    )
                    last_lock_log_ts = now
                return

            rows_since_commit += 1
            total_rows += 1
            if event_sink.enabled:
                try:
                    await event_sink.publish(
                        SpotTickEvent(
                            source="collector.coinbase",
                            payload={
                                "ts": int(row["ts"]),
                                "product_id": str(row["product_id"]),
                                "price": float(row["price"]),
                                "best_bid": (
                                    float(row["best_bid"])
                                    if row["best_bid"] is not None
                                    else None
                                ),
                                "best_ask": (
                                    float(row["best_ask"])
                                    if row["best_ask"] is not None
                                    else None
                                ),
                                "bid_qty": (
                                    float(row["bid_qty"])
                                    if row["bid_qty"] is not None
                                    else None
                                ),
                                "ask_qty": (
                                    float(row["ask_qty"])
                                    if row["ask_qty"] is not None
                                    else None
                                ),
                                "sequence_num": (
                                    int(row["sequence_num"])
                                    if row["sequence_num"] is not None
                                    else None
                                ),
                            },
                        )
                    )
                except Exception:
                    event_publish_failures += 1

            now = time.monotonic()
            if rows_since_commit >= 50 or (now - last_commit) >= 1.0:
                committed = await _commit_with_retry()
                if not committed:
                    now = time.monotonic()
                    if (now - last_lock_log_ts) >= 60.0:
                        logger.warning(
                            "coinbase_spot_commit_locked",
                            extra={
                                "rows_buffered": rows_since_commit,
                                "commit_failures": commit_failures,
                            },
                        )
                        last_lock_log_ts = now
                    return
                rows_since_commit = 0

        try:
            try:
                await client.run(handle_row, run_seconds=seconds)
            finally:
                if rows_since_commit:
                    flushed = await _commit_with_retry()
                    if flushed:
                        rows_since_commit = 0
                    else:
                        logger.warning(
                            "coinbase_spot_shutdown_commit_failed",
                            extra={
                                "rows_buffered": rows_since_commit,
                                "commit_failures": commit_failures,
                            },
                        )
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
                        "event_publish_failures": event_publish_failures,
                        "lock_insert_retries": lock_insert_retries,
                        "dropped_rows_lock": dropped_rows_lock,
                        "commit_failures": commit_failures,
                    },
                )
        finally:
            await event_sink.close()


def _parse_market_tickers(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


async def _run_kalshi(
    settings: Settings,
    logger: logging.Logger,
    seconds: int,
    message_source: AsyncIterator[dict[str, Any]] | None = None,
    market_data: list[dict[str, Any]] | None = None,
) -> None:
    market_tickers = _parse_market_tickers(settings.kalshi_market_tickers)
    markets: list[dict[str, Any]] = market_data or []

    end_time = time.monotonic() + seconds

    if (
        not market_tickers
        and market_data is None
        and settings.kalshi_api_key_id
        and settings.kalshi_private_key_path
    ):
        rest_client = KalshiRestClient(
            base_url=settings.kalshi_rest_url,
            api_key_id=settings.kalshi_api_key_id,
            private_key_path=settings.kalshi_private_key_path,
            logger=logger,
        )
        markets = await rest_client.list_markets(
            limit=settings.kalshi_market_limit,
            max_pages=settings.kalshi_market_max_pages,
            status=settings.kalshi_market_status,
            event_ticker=settings.kalshi_event_ticker,
            series_ticker=settings.kalshi_series_ticker,
            tickers=market_tickers or None,
            end_time=end_time,
        )
        if not market_tickers:
            market_tickers = [
                market.get("ticker")
                for market in markets
                if isinstance(market, dict) and market.get("ticker")
            ]

    if not market_tickers:
        raise RuntimeError(
            "Kalshi market_tickers required (set KALSHI_MARKET_TICKERS or provide REST credentials)"
        )
    if not markets:
        markets = [{"ticker": ticker} for ticker in market_tickers]
    logger.info(
        "kalshi_subscribe_tickers_count",
        extra={
            "count": len(market_tickers),
            "market_tickers": market_tickers,
        },
    )

    client = KalshiWsClient(
        ws_url=settings.kalshi_ws_url,
        api_key_id=settings.kalshi_api_key_id,
        private_key_path=str(settings.kalshi_private_key_path)
        if settings.kalshi_private_key_path
        else None,
        auth_mode=settings.kalshi_ws_auth_mode,
        auth_query_key=settings.kalshi_ws_auth_query_key,
        auth_query_signature=settings.kalshi_ws_auth_query_signature,
        auth_query_timestamp=settings.kalshi_ws_auth_query_timestamp,
        market_tickers=market_tickers,
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

        ts_loaded = int(time.time())
        for market in markets:
            if not isinstance(market, dict):
                continue
            ticker = market.get("ticker")
            if not ticker:
                continue
            row = {
                "market_id": ticker,
                "ts_loaded": ts_loaded,
                "title": market.get("title"),
                "strike": None,
                "settlement_ts": None,
                "close_ts": None,
                "expected_expiration_ts": None,
                "expiration_ts": None,
                "status": market.get("status"),
                "raw_json": json.dumps(market),
            }
            await dao.upsert_kalshi_market(row)
        await conn.commit()

        rows_since_commit = 0
        last_commit = time.monotonic()
        total_rows = 0
        total_commits = 0

        async def handle_ticker(row: dict[str, Any]) -> None:
            nonlocal rows_since_commit, last_commit, total_rows, total_commits
            await dao.insert_kalshi_ticker(row)
            rows_since_commit += 1
            total_rows += 1
            now = time.monotonic()
            if rows_since_commit >= 50 or (now - last_commit) >= 1.0:
                await conn.commit()
                rows_since_commit = 0
                last_commit = now
                total_commits += 1

        async def handle_snapshot(row: dict[str, Any]) -> None:
            nonlocal rows_since_commit, last_commit, total_rows, total_commits
            await dao.insert_kalshi_orderbook_snapshot(row)
            rows_since_commit += 1
            total_rows += 1
            now = time.monotonic()
            if rows_since_commit >= 50 or (now - last_commit) >= 1.0:
                await conn.commit()
                rows_since_commit = 0
                last_commit = now
                total_commits += 1

        async def handle_delta(row: dict[str, Any]) -> None:
            nonlocal rows_since_commit, last_commit, total_rows, total_commits
            await dao.insert_kalshi_orderbook_delta(row)
            rows_since_commit += 1
            total_rows += 1
            now = time.monotonic()
            if rows_since_commit >= 50 or (now - last_commit) >= 1.0:
                await conn.commit()
                rows_since_commit = 0
                last_commit = now
                total_commits += 1

        try:
            await client.run(
                handle_ticker, handle_snapshot, handle_delta, run_seconds=seconds
            )
        finally:
            if rows_since_commit:
                await conn.commit()
                total_commits += 1
            logger.info(
                "kalshi_run_summary",
                extra={
                    "connect_attempts": client.connect_attempts,
                    "connect_successes": client.connect_successes,
                    "recv_count": client.recv_count,
                    "parsed_ticker_count": client.parsed_ticker_count,
                    "parsed_snapshot_count": client.parsed_snapshot_count,
                    "parsed_delta_count": client.parsed_delta_count,
                    "error_message_count": client.error_message_count,
                    "close_count": client.close_count,
                    "total_inserted_rows": total_rows,
                    "total_commits": total_commits,
                },
            )


async def run_collector(
    settings: Settings,
    coinbase: bool,
    kalshi: bool,
    seconds: int,
    message_source: AsyncIterator[dict[str, Any]] | None = None,
    kalshi_message_source: AsyncIterator[dict[str, Any]] | None = None,
    kalshi_market_data: list[dict[str, Any]] | None = None,
    debug: bool = False,
    events_jsonl_path: str | None = None,
    events_bus_url: str | None = None,
    coinbase_publish_only: bool = False,
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
            "kalshi_enabled": kalshi,
            "collector_seconds": seconds,
        },
    )

    if coinbase:
        await _run_coinbase(
            settings,
            logger,
            seconds,
            message_source=message_source,
            events_jsonl_path=events_jsonl_path,
            events_bus_url=events_bus_url,
            publish_only=coinbase_publish_only,
        )
    if kalshi:
        await _run_kalshi(
            settings,
            logger,
            seconds,
            message_source=kalshi_message_source,
            market_data=kalshi_market_data,
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
            kalshi=args.kalshi,
            seconds=args.seconds,
            debug=args.debug,
            events_jsonl_path=args.events_jsonl_path,
            events_bus_url=(
                args.events_bus_url
                if args.events_bus_url
                else (settings.bus_url if args.events_bus else None)
            ),
            coinbase_publish_only=args.coinbase_publish_only,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
