from __future__ import annotations

import argparse
import asyncio
import fcntl
import logging
import tempfile
import time
from pathlib import Path

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.data.db import init_db
from kalshi_bot.events import EventPublisher
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.kalshi.btc_markets import (
    BTC_SERIES_TICKERS,
    extract_close_ts,
    extract_expected_expiration_ts,
)
from kalshi_bot.kalshi.contracts import KalshiContractRefresher, load_market_rows
from kalshi_bot.kalshi.market_filters import normalize_db_status, normalize_series
from kalshi_bot.kalshi.rest_client import KalshiRestClient

LOCK_PATH = Path(tempfile.gettempdir()) / "kalshi_refresh_kalshi_contracts.lock"


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
    parser.add_argument(
        "--publish-only",
        action="store_true",
        help="Publish contract_update events only and skip SQLite writes.",
    )
    parser.add_argument(
        "--events-jsonl-path",
        type=str,
        default=None,
        help="Optional JSONL file path for publishing contract_update events.",
    )
    parser.add_argument(
        "--events-bus",
        action="store_true",
        help="Publish contract_update events to JetStream.",
    )
    parser.add_argument(
        "--events-bus-url",
        type=str,
        default=None,
        help="JetStream bus URL override (default BUS_URL).",
    )
    return parser.parse_args()


async def _load_market_rows_from_rest(
    *,
    rest_client: KalshiRestClient,
    status: str | None,
    series: list[str],
    limit: int | None,
    logger: logging.Logger | None,
) -> list[tuple[str, int | None]]:
    rows: list[tuple[str, int | None]] = []
    seen: set[str] = set()

    for series_ticker in series:
        markets, _ = await rest_client.list_markets_by_series(
            series_ticker=series_ticker,
            status=status,
            limit=limit,
        )
        for market in markets:
            ticker_raw = market.get("ticker") or market.get("market_id")
            if not isinstance(ticker_raw, str) or not ticker_raw:
                continue
            ticker = ticker_raw
            if ticker in seen:
                continue
            seen.add(ticker)
            close_ts = extract_close_ts(market, logger=logger) or extract_expected_expiration_ts(
                market, logger=logger
            )
            rows.append((ticker, close_ts))
            if limit is not None and limit > 0 and len(rows) >= limit:
                return rows

    return rows


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
    status = normalize_db_status(args.status)
    now_ts = int(time.time())

    if args.publish_only:
        rows = await _load_market_rows_from_rest(
            rest_client=rest_client,
            status=status,
            series=series,
            limit=args.limit if args.limit > 0 else None,
            logger=logger,
        )
        if not rows:
            logger.info(
                "kalshi_contracts_no_rows_due_publish_only",
                extra={
                    "status": status,
                    "series": series,
                    "limit": int(args.limit or 0),
                },
            )
            print("No contracts due for refresh.")
            lock_file.close()
            return 0

        bus_url = (
            args.events_bus_url
            if args.events_bus_url
            else (settings.bus_url if args.events_bus else None)
        )
        event_sink = await EventPublisher.create(
            jsonl_path=args.events_jsonl_path,
            bus_url=bus_url,
        )
        try:
            summary = await refresher.refresh(
                conn=None,
                status=status,
                series=series,
                rows=rows,
                event_sink=event_sink,
                publish_only=True,
            )
        finally:
            await event_sink.close()

        print(
            "Contract refresh summary: publish_only=True selected={selected} "
            "successes={successes} failures={failures} "
            "event_publish_failures={event_publish_failures}".format(
                selected=len(rows), **summary
            )
        )
        lock_file.close()
        return 0

    print(f"DB path: {settings.db_path}")
    await init_db(settings.db_path)

    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 15000;")
        await conn.commit()

        rows = await load_market_rows(
            conn,
            status,
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
                    "status": status,
                    "series": series,
                    "refresh_age_seconds": max(int(args.refresh_age_seconds), 0),
                    "include_closed": bool(args.include_closed),
                    "limit": int(args.limit or 0),
                },
            )
            print("No contracts due for refresh.")
            lock_file.close()
            return 0

        bus_url = (
            args.events_bus_url
            if args.events_bus_url
            else (settings.bus_url if args.events_bus else None)
        )
        event_sink = await EventPublisher.create(
            jsonl_path=args.events_jsonl_path,
            bus_url=bus_url,
        )
        try:
            summary = await refresher.refresh(
                conn,
                status=status,
                series=series,
                rows=rows,
                event_sink=event_sink,
            )
        finally:
            await event_sink.close()

    print(
        "Contract refresh summary: selected={selected} successes={successes} "
        "failures={failures} upserted={upserted} "
        "event_publish_failures={event_publish_failures}".format(
            selected=len(rows), **summary
        )
    )
    lock_file.close()
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
