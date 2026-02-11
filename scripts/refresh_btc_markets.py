from __future__ import annotations

import asyncio
import fcntl
import json
import time
from pathlib import Path
from typing import Any

import aiosqlite
import sqlite3

from kalshi_bot.config import load_settings
from kalshi_bot.data.dao import Dao
from kalshi_bot.data.db import init_db
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.kalshi.btc_markets import (
    backfill_market_times,
    empty_series_tickers,
    extract_close_ts,
    extract_expected_expiration_ts,
    extract_expiration_ts,
    extract_strike_basic,
    fetch_btc_markets,
)
from kalshi_bot.kalshi.rest_client import KalshiRestClient
from kalshi_bot.kalshi.rest_client import KalshiRestError


RETRY_DELAYS_SECONDS = [5.0, 10.0, 20.0, 30.0]
DB_LOCK_RETRY_DELAYS_SECONDS = [5.0, 10.0, 20.0, 30.0]
LOCK_PATH = Path("/tmp/kalshi_refresh_btc_markets.lock")


async def _load_existing_market_rows(
    conn: aiosqlite.Connection, market_ids: list[str]
) -> dict[str, dict[str, Any]]:
    if not market_ids:
        return {}
    existing: dict[str, dict[str, Any]] = {}
    chunk_size = 500
    for idx in range(0, len(market_ids), chunk_size):
        chunk = market_ids[idx : idx + chunk_size]
        placeholders = ", ".join("?" for _ in chunk)
        cursor = await conn.execute(
            f"""
            SELECT market_id, title, strike, settlement_ts, close_ts,
                   expected_expiration_ts, expiration_ts, status
            FROM kalshi_markets
            WHERE market_id IN ({placeholders})
            """,
            chunk,
        )
        for (
            market_id,
            title,
            strike,
            settlement_ts,
            close_ts,
            expected_expiration_ts,
            expiration_ts,
            status,
        ) in await cursor.fetchall():
            existing[market_id] = {
                "title": title,
                "strike": strike,
                "settlement_ts": settlement_ts,
                "close_ts": close_ts,
                "expected_expiration_ts": expected_expiration_ts,
                "expiration_ts": expiration_ts,
                "status": status,
            }
    return existing


def _same_value(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is b
    if isinstance(a, float) or isinstance(b, float):
        try:
            return abs(float(a) - float(b)) < 1e-9
        except (TypeError, ValueError):
            return False
    return a == b


def _market_row_changed(existing: dict[str, Any] | None, row: dict[str, Any]) -> bool:
    if existing is None:
        return True
    keys = (
        "title",
        "strike",
        "settlement_ts",
        "close_ts",
        "expected_expiration_ts",
        "expiration_ts",
        "status",
    )
    for key in keys:
        if not _same_value(existing.get(key), row.get(key)):
            return True
    return False


async def _refresh_btc_markets() -> int:
    lock_file = LOCK_PATH.open("w")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("refresh_btc_markets already running; skipping.")
        lock_file.close()
        return 0

    settings = load_settings()
    logger = setup_logger(settings.log_path)

    logger.info(
        "kalshi_refresh_db_path",
        extra={"db_path": str(settings.db_path)},
    )
    print(f"DB path: {settings.db_path}")

    await init_db(settings.db_path)

    rest_client = KalshiRestClient(
        base_url=settings.kalshi_rest_url,
        api_key_id=settings.kalshi_api_key_id,
        private_key_path=settings.kalshi_private_key_path,
        logger=logger,
    )

    attempt = 0
    while True:
        try:
            markets, per_series = await fetch_btc_markets(
                rest_client,
                status=settings.kalshi_market_status,
                limit=settings.kalshi_market_limit,
                logger=logger,
            )
            break
        except KalshiRestError as exc:
            if not exc.transient or attempt >= len(RETRY_DELAYS_SECONDS):
                if exc.transient:
                    print(
                        "WARNING: refresh_btc_markets rate-limited repeatedly; "
                        "skipping this run to avoid crash."
                    )
                    logger.warning(
                        "kalshi_refresh_btc_markets_rate_limited_skip",
                        extra={
                            "status": exc.status,
                            "body": exc.body,
                            "attempts": attempt + 1,
                        },
                    )
                    return 0
                raise
            delay = RETRY_DELAYS_SECONDS[attempt]
            attempt += 1
            print(
                f"WARNING: refresh_btc_markets transient error "
                f"(status={exc.status}); retrying in {delay:.0f}s "
                f"(attempt {attempt}/{len(RETRY_DELAYS_SECONDS)})"
            )
            logger.warning(
                "kalshi_refresh_btc_markets_retry",
                extra={
                    "status": exc.status,
                    "body": exc.body,
                    "attempt": attempt,
                    "delay_seconds": delay,
                },
            )
            await asyncio.sleep(delay)

    sample_tickers = [
        market.get("ticker") for market in markets if market.get("ticker")
    ][:10]

    print(
        f"FOUND {len(markets)} BTC markets. "
        f"Sample: {', '.join(sample_tickers) if sample_tickers else 'None'}"
    )
    per_series_counts = {
        series: len(info.get("markets", [])) for series, info in per_series.items()
    }
    if per_series_counts:
        print(f"Series counts: {per_series_counts}")

    async def _write_market_rows_once() -> tuple[int, int, int]:
        async with aiosqlite.connect(settings.db_path) as conn:
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute("PRAGMA journal_mode = WAL;")
            await conn.execute("PRAGMA synchronous = NORMAL;")
            await conn.execute("PRAGMA busy_timeout = 15000;")
            await conn.commit()

            dao = Dao(conn)
            ts_loaded = int(time.time())
            upserted = 0
            unchanged = 0
            market_ids = [
                market.get("ticker") or market.get("market_id")
                for market in markets
                if isinstance(market, dict)
                and isinstance((market.get("ticker") or market.get("market_id")), str)
                and (market.get("ticker") or market.get("market_id"))
            ]
            existing_rows = await _load_existing_market_rows(conn, market_ids)

            for market in markets:
                if not isinstance(market, dict):
                    continue
                market_id = market.get("ticker") or market.get("market_id")
                if not market_id:
                    continue
                strike = extract_strike_basic(market)
                close_ts = extract_close_ts(market, logger=logger)
                row = {
                    "market_id": market_id,
                    "ts_loaded": ts_loaded,
                    "title": market.get("title"),
                    "strike": strike,
                    "settlement_ts": close_ts,
                    "close_ts": close_ts,
                    "expected_expiration_ts": extract_expected_expiration_ts(
                        market, logger=logger
                    ),
                    "expiration_ts": extract_expiration_ts(market, logger=logger),
                    "status": market.get("status"),
                    "raw_json": json.dumps(market),
                }
                if not _market_row_changed(existing_rows.get(market_id), row):
                    unchanged += 1
                    continue
                await dao.upsert_kalshi_market(row)
                upserted += 1
                # Reduce long write-lock windows during large refreshes.
                if upserted % 100 == 0:
                    await conn.commit()

            backfilled = await backfill_market_times(conn, logger)
            await conn.commit()
            return upserted, unchanged, backfilled

    db_lock_attempt = 0
    while True:
        try:
            upserted, unchanged, backfilled = await _write_market_rows_once()
            break
        except sqlite3.OperationalError as exc:
            if "database is locked" not in str(exc).lower():
                raise
            if db_lock_attempt >= len(DB_LOCK_RETRY_DELAYS_SECONDS):
                print(
                    "WARNING: refresh_btc_markets database remained locked after retries; "
                    "skipping this run."
                )
                logger.warning(
                    "kalshi_refresh_btc_markets_db_locked_skip",
                    extra={"attempts": db_lock_attempt + 1, "error": str(exc)},
                )
                lock_file.close()
                return 0
            delay = DB_LOCK_RETRY_DELAYS_SECONDS[db_lock_attempt]
            db_lock_attempt += 1
            print(
                f"WARNING: refresh_btc_markets database locked; retrying in {delay:.0f}s "
                f"(attempt {db_lock_attempt}/{len(DB_LOCK_RETRY_DELAYS_SECONDS)})"
            )
            logger.warning(
                "kalshi_refresh_btc_markets_db_locked_retry",
                extra={
                    "attempt": db_lock_attempt,
                    "delay_seconds": delay,
                    "error": str(exc),
                },
            )
            await asyncio.sleep(delay)

    logger.info(
        "kalshi_refresh_btc_markets",
        extra={
            "fetched_total": len(markets),
            "selected_total": len(markets),
            "upserted_total": upserted,
            "unchanged_total": unchanged,
            "backfilled_total": backfilled,
            "sample_tickers": sample_tickers,
            "series_counts": per_series_counts,
        },
    )

    empty_series = empty_series_tickers(per_series)
    if empty_series:
        for series_ticker in empty_series:
            payloads = per_series.get(series_ticker, {}).get("payloads", [])
            logger.error(
                "kalshi_btc_series_empty",
                extra={
                    "series_ticker": series_ticker,
                    "raw_payloads": payloads,
                },
            )
        print(f"ERROR: No markets returned for series {empty_series}")
        lock_file.close()
        return 1

    lock_file.close()
    return 0


def main() -> int:
    return asyncio.run(_refresh_btc_markets())


if __name__ == "__main__":
    raise SystemExit(main())
