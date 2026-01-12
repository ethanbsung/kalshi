from __future__ import annotations

"""Smoke test for Kalshi BTC pipeline.

Usage:
  python3 scripts/smoke_kalshi_pipeline.py --seconds 30 --interval 5

Requires Kalshi REST credentials in env (see Settings).
"""

import argparse
import asyncio
import json
import time

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.data import init_db
from kalshi_bot.data.dao import Dao
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.kalshi.btc_markets import (
    BTC_SERIES_TICKERS,
    extract_settlement_ts,
    extract_strike_basic,
    fetch_btc_markets,
)
from kalshi_bot.kalshi.contracts import KalshiContractRefresher
from kalshi_bot.kalshi.health import get_quote_coverage, get_quote_freshness
from kalshi_bot.kalshi.market_filters import fetch_status_counts
from kalshi_bot.kalshi.quotes import KalshiQuotePoller
from kalshi_bot.kalshi.rest_client import KalshiRestClient


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test Kalshi BTC pipeline")
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--concurrency", type=int, default=5)
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

    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 5000;")
        await conn.commit()

        markets, _ = await fetch_btc_markets(
            rest_client,
            status=settings.kalshi_market_status,
            limit=settings.kalshi_market_limit,
            logger=logger,
        )

        dao = Dao(conn)
        ts_loaded = int(time.time())
        for market in markets:
            if not isinstance(market, dict):
                continue
            market_id = market.get("ticker") or market.get("market_id")
            if not market_id:
                continue
            row = {
                "market_id": market_id,
                "ts_loaded": ts_loaded,
                "title": market.get("title"),
                "strike": extract_strike_basic(market),
                "settlement_ts": extract_settlement_ts(market, logger=logger),
                "status": market.get("status"),
                "raw_json": json.dumps(market),
            }
            await dao.upsert_kalshi_market(row)
        await conn.commit()

        contract_refresher = KalshiContractRefresher(
            rest_client=rest_client,
            logger=logger,
            max_concurrency=args.concurrency,
        )
        await contract_refresher.refresh(
            conn,
            status="active",
            series=list(BTC_SERIES_TICKERS),
        )

        poller = KalshiQuotePoller(
            rest_client=rest_client,
            logger=logger,
            max_concurrency=args.concurrency,
        )
        poll_summary = await poller.run(
            conn,
            run_seconds=args.seconds,
            poll_interval=args.interval,
            status="active",
            series=list(BTC_SERIES_TICKERS),
        )

        status_counts = await fetch_status_counts(conn)
        freshness = await get_quote_freshness(
            conn,
            within_seconds=30,
            status="active",
            series=list(BTC_SERIES_TICKERS),
        )
        coverage = await get_quote_coverage(
            conn,
            lookback_seconds=max(1, int(args.interval)),
            status="active",
            series=list(BTC_SERIES_TICKERS),
        )

    active_count = status_counts.get("active", 0)
    print(f"markets_by_status={status_counts}")
    print(f"quotes_inserted={poll_summary['inserted']}")
    print(
        f"latest_quote_ts={freshness['latest_ts']} age_seconds={freshness['age_seconds']}"
    )
    print(
        "coverage_pct={coverage_pct:.1f} markets_with_quotes={markets_with_quotes} total_markets={total_markets}".format(
            **coverage
        )
    )
    print(
        f"error_counts={poll_summary['error_counts']} failed_tickers_sample={poll_summary['failed_tickers_sample']}"
    )

    if active_count == 0:
        print("ERROR: no active markets found")
        return 1
    if poll_summary["inserted"] == 0:
        print("ERROR: no quotes inserted")
        return 1
    if not freshness["is_fresh"]:
        print("ERROR: quotes are stale")
        return 1
    if coverage["coverage_pct"] < 80.0:
        print("ERROR: quote coverage below 80%")
        return 1

    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
