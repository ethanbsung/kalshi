from __future__ import annotations

import argparse
import asyncio
import fcntl
import time
from pathlib import Path

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.data.db import init_db
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.kalshi.btc_markets import BTC_SERIES_TICKERS
from kalshi_bot.kalshi.contracts import KalshiContractRefresher, load_market_rows
from kalshi_bot.kalshi.market_filters import normalize_series
from kalshi_bot.kalshi.rest_client import KalshiRestClient

LOCK_PATH = Path("/tmp/kalshi_refresh_kalshi_contracts.lock")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh Kalshi contract bounds")
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--status", type=str, default=None)
    parser.add_argument(
        "--series",
        action="append",
        default=None,
        help="Repeatable series ticker filter (defaults to BTC series)",
    )
    parser.add_argument(
        "--refresh-age-seconds",
        type=int,
        default=6 * 3600,
        help="Only refresh contracts older than this age (or missing/incomplete).",
    )
    parser.add_argument(
        "--include-closed",
        action="store_true",
        help="Include already-closed markets in refresh candidate set.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max number of contracts to refresh this run (0 = no limit).",
    )
    return parser.parse_args()


async def _run() -> int:
    lock_file = LOCK_PATH.open("w")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("refresh_kalshi_contracts already running; skipping.")
        lock_file.close()
        return 0

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
    refresher = KalshiContractRefresher(
        rest_client=rest_client,
        logger=logger,
        max_concurrency=args.concurrency,
    )

    series = (
        normalize_series(args.series)
        if args.series is not None
        else list(BTC_SERIES_TICKERS)
    )
    now_ts = int(time.time())

    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 15000;")
        await conn.commit()

        rows = await load_market_rows(
            conn,
            args.status,
            series,
            now_ts=now_ts,
            include_closed=args.include_closed,
            refresh_age_seconds=max(int(args.refresh_age_seconds), 0),
            limit=args.limit if args.limit > 0 else None,
        )
        if not rows:
            logger.info(
                "kalshi_contracts_no_rows_due",
                extra={
                    "status": args.status,
                    "series": series,
                    "refresh_age_seconds": max(int(args.refresh_age_seconds), 0),
                    "include_closed": bool(args.include_closed),
                    "limit": int(args.limit or 0),
                },
            )
            print("No contracts due for refresh.")
            lock_file.close()
            return 0

        summary = await refresher.refresh(
            conn,
            status=args.status,
            series=series,
            rows=rows,
        )

    print(
        "Contract refresh summary: selected={selected} successes={successes} "
        "failures={failures} upserted={upserted}".format(
            selected=len(rows), **summary
        )
    )
    lock_file.close()
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
