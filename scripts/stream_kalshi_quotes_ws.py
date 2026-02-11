from __future__ import annotations

import argparse
import asyncio
import sqlite3
import time
from typing import Any

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.data.dao import Dao
from kalshi_bot.data.db import init_db
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.kalshi import KalshiWsClient
from kalshi_bot.kalshi.btc_markets import BTC_SERIES_TICKERS
from kalshi_bot.kalshi.market_filters import (
    fetch_status_counts,
    no_markets_message,
    normalize_db_status,
    normalize_series,
)
from kalshi_bot.kalshi.quotes import (
    build_quote_row_from_topbook,
    load_market_tickers,
)
from kalshi_bot.kalshi.validation import validate_quote_row


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream Kalshi quote snapshots via WS")
    parser.add_argument("--seconds", type=int, default=900)
    parser.add_argument("--status", type=str, default=None)
    parser.add_argument("--max-horizon-seconds", type=int, default=6 * 3600)
    parser.add_argument(
        "--series",
        action="append",
        default=None,
        help="Repeatable series ticker filter (defaults to BTC series)",
    )
    parser.add_argument(
        "--min-write-seconds",
        type=int,
        default=10,
        help="Minimum seconds between writes per market_id.",
    )
    parser.add_argument(
        "--commit-every-rows",
        type=int,
        default=50,
        help="Commit after this many inserts.",
    )
    parser.add_argument(
        "--commit-every-seconds",
        type=float,
        default=1.0,
        help="Commit at least this often while receiving messages.",
    )
    return parser.parse_args()


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()
    logger = setup_logger(settings.log_path)

    print(f"DB path: {settings.db_path}")
    await init_db(settings.db_path)

    series = (
        normalize_series(args.series)
        if args.series is not None
        else list(BTC_SERIES_TICKERS)
    )
    status = normalize_db_status(args.status)
    min_write_seconds = max(int(args.min_write_seconds), 0)
    commit_every_rows = max(int(args.commit_every_rows), 1)
    commit_every_seconds = max(float(args.commit_every_seconds), 0.1)
    no_markets_retry_seconds = 10.0
    no_markets_log_every_seconds = 60.0
    run_window_seconds = max(int(args.seconds), 1)
    end_time = time.monotonic() + run_window_seconds

    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 15000;")
        await conn.commit()

        tickers: list[str] = []
        last_no_market_log = 0.0
        zero_market_wait_seconds = 0.0
        zero_market_retries = 0
        while True:
            now_ts = int(time.time())
            tickers = await load_market_tickers(
                conn,
                status,
                series,
                now_ts=now_ts,
                max_horizon_seconds=args.max_horizon_seconds,
            )
            if tickers:
                break
            zero_market_retries += 1
            now_mono = time.monotonic()
            if (now_mono - last_no_market_log) >= no_markets_log_every_seconds:
                counts = await fetch_status_counts(conn)
                message = no_markets_message(status, counts)
                logger.warning(
                    "kalshi_ws_quotes_no_markets_retrying",
                    extra={
                        "status": status,
                        "series": series,
                        "status_counts": counts,
                        "retry_seconds": no_markets_retry_seconds,
                        "max_horizon_seconds": args.max_horizon_seconds,
                    },
                )
                print(f"WARNING: {message}; retrying in {int(no_markets_retry_seconds)}s")
                last_no_market_log = now_mono
            sleep_for = no_markets_retry_seconds
            await asyncio.sleep(sleep_for)
            zero_market_wait_seconds += sleep_for
            if time.monotonic() >= end_time:
                # Do not exit on empty windows; keep waiting until markets appear.
                end_time = time.monotonic() + run_window_seconds

        if zero_market_retries > 0:
            logger.info(
                "kalshi_ws_quotes_zero_market_wait",
                extra={
                    "wait_seconds": int(zero_market_wait_seconds),
                    "retries": zero_market_retries,
                    "market_count": len(tickers),
                    "status": status,
                    "series": series,
                    "max_horizon_seconds": args.max_horizon_seconds,
                },
            )
            print(
                "WS zero-market wait: wait_s={wait_s} retries={retries} markets={markets}".format(
                    wait_s=int(zero_market_wait_seconds),
                    retries=zero_market_retries,
                    markets=len(tickers),
                )
            )
            # Reset the run window so we get a full stream interval after markets appear.
            end_time = time.monotonic() + run_window_seconds

        client = KalshiWsClient(
            ws_url=settings.kalshi_ws_url,
            api_key_id=settings.kalshi_api_key_id,
            private_key_path=(
                str(settings.kalshi_private_key_path)
                if settings.kalshi_private_key_path
                else None
            ),
            auth_mode=settings.kalshi_ws_auth_mode,
            auth_query_key=settings.kalshi_ws_auth_query_key,
            auth_query_signature=settings.kalshi_ws_auth_query_signature,
            auth_query_timestamp=settings.kalshi_ws_auth_query_timestamp,
            market_tickers=tickers,
            logger=logger,
            channels=["ticker"],
        )

        dao = Dao(conn)

        rows_since_commit = 0
        last_commit = time.monotonic()
        inserted = 0
        invalid = 0
        write_errors = 0
        throttled = 0
        last_throttle_log = 0.0
        last_write_error_log = 0.0
        last_written_ts_by_market: dict[str, int] = {}

        async def _maybe_commit(force: bool = False) -> None:
            nonlocal rows_since_commit, last_commit
            if rows_since_commit <= 0 and not force:
                return
            now = time.monotonic()
            if not force:
                if (
                    rows_since_commit < commit_every_rows
                    and (now - last_commit) < commit_every_seconds
                ):
                    return
            await conn.commit()
            rows_since_commit = 0
            last_commit = now

        async def on_ticker(row: dict[str, Any]) -> None:
            nonlocal rows_since_commit, inserted, invalid, write_errors, throttled
            nonlocal last_throttle_log, last_write_error_log
            market_id = str(row.get("market_id") or "")
            if not market_id:
                invalid += 1
                return

            ts = int(row.get("ts") or time.time())
            if min_write_seconds > 0:
                prev_ts = last_written_ts_by_market.get(market_id)
                if prev_ts is not None and (ts - prev_ts) < min_write_seconds:
                    throttled += 1
                    now_mono = time.monotonic()
                    if (now_mono - last_throttle_log) >= 60.0:
                        logger.info(
                            "kalshi_ws_quotes_throttled",
                            extra={
                                "throttled_total": throttled,
                                "min_write_seconds": min_write_seconds,
                            },
                        )
                        last_throttle_log = now_mono
                    return

            quote_row = build_quote_row_from_topbook(
                market_id=market_id,
                ts=ts,
                yes_bid=row.get("best_yes_bid"),
                yes_ask=row.get("best_yes_ask"),
                no_bid=row.get("best_no_bid"),
                no_ask=row.get("best_no_ask"),
                volume=row.get("volume"),
                open_interest=row.get("open_interest"),
                raw_json=str(row.get("raw_json") or "{}"),
            )
            is_valid, _ = validate_quote_row(quote_row)
            if not is_valid:
                invalid += 1
                return

            try:
                await dao.insert_kalshi_quote(quote_row)
            except sqlite3.OperationalError as exc:
                write_errors += 1
                now_mono = time.monotonic()
                if (now_mono - last_write_error_log) >= 60.0:
                    logger.warning(
                        "kalshi_ws_quotes_insert_locked",
                        extra={
                            "write_errors_total": write_errors,
                            "error": str(exc),
                        },
                    )
                    last_write_error_log = now_mono
                return

            inserted += 1
            rows_since_commit += 1
            last_written_ts_by_market[market_id] = ts
            await _maybe_commit(force=False)

        async def on_snapshot(_: dict[str, Any]) -> None:
            return

        async def on_delta(_: dict[str, Any]) -> None:
            return

        try:
            run_seconds = max(int(end_time - time.monotonic()), 1)
            await client.run(
                on_ticker,
                on_snapshot,
                on_delta,
                run_seconds=run_seconds,
            )
        finally:
            await _maybe_commit(force=True)

        summary = {
            "inserted": inserted,
            "invalid": invalid,
            "write_errors": write_errors,
            "throttled": throttled,
            "market_count": len(tickers),
        }
        logger.info("kalshi_ws_quotes_summary", extra=summary)
        print(
            "WS quote summary: markets={market_count} inserted={inserted} "
            "invalid={invalid} write_errors={write_errors} throttled={throttled}".format(
                **summary
            )
        )
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
