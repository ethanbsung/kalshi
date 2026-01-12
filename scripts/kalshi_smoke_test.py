from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
import time
from typing import Any

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.data import Dao, init_db
from kalshi_bot.infra import setup_logger
from kalshi_bot.kalshi import KalshiWsClient
from kalshi_bot.kalshi.ws_client import compute_best_prices


async def _run_ws(settings, logger) -> None:
    raw_tickers = settings.kalshi_market_tickers
    if not raw_tickers:
        raise RuntimeError("KALSHI_MARKET_TICKERS must be set")
    market_tickers = [t.strip() for t in raw_tickers.split(",") if t.strip()]
    if not market_tickers:
        raise RuntimeError("KALSHI_MARKET_TICKERS must include at least one ticker")

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

        async def handle_ticker(row: dict[str, Any]) -> None:
            nonlocal rows_since_commit, last_commit
            await dao.insert_kalshi_ticker(row)
            rows_since_commit += 1
            now = time.monotonic()
            if rows_since_commit >= 50 or (now - last_commit) >= 1.0:
                await conn.commit()
                rows_since_commit = 0
                last_commit = now

        async def handle_snapshot(row: dict[str, Any]) -> None:
            nonlocal rows_since_commit, last_commit
            await dao.insert_kalshi_orderbook_snapshot(row)
            rows_since_commit += 1
            now = time.monotonic()
            if rows_since_commit >= 50 or (now - last_commit) >= 1.0:
                await conn.commit()
                rows_since_commit = 0
                last_commit = now

        async def handle_delta(row: dict[str, Any]) -> None:
            nonlocal rows_since_commit, last_commit
            await dao.insert_kalshi_orderbook_delta(row)
            rows_since_commit += 1
            now = time.monotonic()
            if rows_since_commit >= 50 or (now - last_commit) >= 1.0:
                await conn.commit()
                rows_since_commit = 0
                last_commit = now

        await client.run(handle_ticker, handle_snapshot, handle_delta, run_seconds=60)
        if rows_since_commit:
            await conn.commit()


def _load_json_array(value: str | None) -> list[list[Any]]:
    if not value:
        return []
    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        return []
    if isinstance(data, list):
        return data
    return []


def main() -> int:
    settings = load_settings()
    logger = setup_logger(settings.log_path, also_stdout=True)
    asyncio.run(init_db(settings.db_path))

    try:
        asyncio.run(_run_ws(settings, logger))
    except Exception as exc:
        print(f"FAIL: {exc}")
        return 1

    conn = sqlite3.connect(settings.db_path)
    try:
        snapshots_count = conn.execute(
            "SELECT COUNT(*) FROM kalshi_orderbook_snapshots"
        ).fetchone()[0]
        deltas_count = conn.execute(
            "SELECT COUNT(*) FROM kalshi_orderbook_deltas"
        ).fetchone()[0]
        tickers_count = conn.execute(
            "SELECT COUNT(*) FROM kalshi_tickers"
        ).fetchone()[0]

        snapshot_row = conn.execute(
            "SELECT market_id, seq, yes_bids_json, no_bids_json "
            "FROM kalshi_orderbook_snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()
        ticker_row = conn.execute(
            "SELECT market_id, best_yes_bid, best_yes_ask, best_no_bid, best_no_ask, "
            "volume, open_interest "
            "FROM kalshi_tickers ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    print("Kalshi smoke test report")
    print(f"snapshots_count={snapshots_count}")
    print(f"deltas_count={deltas_count}")
    print(f"tickers_count={tickers_count}")

    empty_snapshot = True
    if snapshot_row is not None:
        market_id, seq, yes_json, no_json = snapshot_row
        yes_levels = _load_json_array(yes_json)
        no_levels = _load_json_array(no_json)
        best = compute_best_prices(yes_levels, no_levels)
        computed = any(value is not None for value in best.values())
        empty_snapshot = not yes_levels and not no_levels
        print(
            "newest_snapshot="
            f"market_id={market_id} seq={seq} "
            f"yes_levels={len(yes_levels)} no_levels={len(no_levels)} "
            f"best_computed={computed}"
        )
    else:
        print("newest_snapshot=none")

    if ticker_row is not None:
        (
            market_id,
            best_yes_bid,
            best_yes_ask,
            best_no_bid,
            best_no_ask,
            volume,
            open_interest,
        ) = ticker_row
        print(
            "newest_ticker="
            f"market_id={market_id} best_yes_bid={best_yes_bid} "
            f"best_yes_ask={best_yes_ask} best_no_bid={best_no_bid} "
            f"best_no_ask={best_no_ask} volume={volume} open_interest={open_interest}"
        )
    else:
        print("newest_ticker=none")

    if snapshots_count == 0 or empty_snapshot:
        print("FAIL: empty snapshot")
        return 1

    if tickers_count == 0 and deltas_count == 0:
        print("WARN: no tickers or deltas received")

    print("PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
