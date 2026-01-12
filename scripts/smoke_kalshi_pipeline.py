from __future__ import annotations

"""Smoke test for Kalshi BTC pipeline.

Usage:
  python3 scripts/smoke_kalshi_pipeline.py --seconds 30 --interval 5

Requires Kalshi REST credentials in env (see Settings).
Global coverage is informational only; pass/fail uses relevant near-spot coverage.
The --status flag applies to DB filtering; REST listing uses KALSHI_MARKET_STATUS.
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
from kalshi_bot.kalshi.health import (
    evaluate_smoke_conditions,
    get_quote_coverage,
    get_quote_freshness,
    get_relevant_quote_coverage,
    get_relevant_universe,
)
from kalshi_bot.kalshi.market_filters import fetch_status_counts, normalize_series
from kalshi_bot.kalshi.quotes import KalshiQuotePoller
from kalshi_bot.kalshi.rest_client import KalshiRestClient


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test Kalshi BTC pipeline")
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--pct-band", type=float, default=3.0)
    parser.add_argument("--top-n", type=int, default=40)
    parser.add_argument("--freshness-seconds", type=int, default=30)
    parser.add_argument("--min-relevant-coverage", type=float, default=80.0)
    parser.add_argument("--status", type=str, default="active")
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

    series = (
        normalize_series(args.series)
        if args.series is not None
        else list(BTC_SERIES_TICKERS)
    )
    rest_status = settings.kalshi_market_status
    if args.status and rest_status and args.status != rest_status:
        logger.info(
            "kalshi_smoke_status_mismatch",
            extra={
                "db_status_filter": args.status,
                "rest_status_filter": rest_status,
            },
        )

    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 5000;")
        await conn.commit()

        try:
            markets, _ = await fetch_btc_markets(
                rest_client,
                status=rest_status,
                limit=settings.kalshi_market_limit,
                logger=logger,
            )
        except Exception as exc:
            logger.error(
                "kalshi_smoke_market_refresh_failed",
                extra={"error": str(exc), "rest_status": rest_status},
            )
            print(f"ERROR: market refresh failed ({exc})")
            return 1

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

        status_counts = await fetch_status_counts(conn)
        status_filter = args.status
        if status_filter is not None and str(status_filter) not in status_counts:
            logger.warning(
                "kalshi_smoke_status_fallback",
                extra={
                    "requested_status": status_filter,
                    "status_counts": status_counts,
                },
            )
            status_filter = None

        contract_refresher = KalshiContractRefresher(
            rest_client=rest_client,
            logger=logger,
            max_concurrency=args.concurrency,
        )
        await contract_refresher.refresh(
            conn,
            status=status_filter,
            series=series,
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
            status=status_filter,
            series=series,
        )

        relevant_ids, spot_price, selection_summary = await get_relevant_universe(
            conn,
            pct_band=args.pct_band,
            top_n=args.top_n,
            status=status_filter,
            series=series,
            product_id=settings.coinbase_product_id,
        )

        freshness = await get_quote_freshness(
            conn,
            within_seconds=args.freshness_seconds,
            status=status_filter,
            series=series,
        )
        coverage = await get_quote_coverage(
            conn,
            lookback_seconds=args.freshness_seconds,
            status=status_filter,
            series=series,
        )
        relevant_coverage = await get_relevant_quote_coverage(
            conn,
            market_ids=relevant_ids,
            freshness_seconds=args.freshness_seconds,
        )

    print(f"markets_by_status={status_counts}")
    print(f"quotes_inserted={poll_summary['inserted']}")
    print(f"spot_price={spot_price}")
    print(
        f"selection_method={selection_summary['method']} pct_band={selection_summary['pct_band']} top_n={selection_summary['top_n']}"
    )
    print(
        f"relevant_total={relevant_coverage['relevant_total']} "
        f"relevant_with_quotes={relevant_coverage['relevant_with_recent_quotes']} "
        f"relevant_coverage_pct={relevant_coverage['relevant_coverage_pct']:.1f}"
    )
    print(
        "global_total={total_markets} global_with_quotes={markets_with_quotes} global_coverage_pct={coverage_pct:.1f}".format(
            **coverage
        )
    )
    print(
        f"latest_quote_ts={freshness['latest_ts']} age_seconds={freshness['age_seconds']}"
    )
    print(
        f"error_counts={poll_summary['error_counts']} failed_tickers_sample={poll_summary['failed_tickers_sample']}"
    )

    failures, reasons = evaluate_smoke_conditions(
        latest_age_seconds=freshness["age_seconds"],
        freshness_seconds=args.freshness_seconds,
        relevant_total=relevant_coverage["relevant_total"],
        relevant_coverage_pct=relevant_coverage["relevant_coverage_pct"],
        min_relevant_coverage=args.min_relevant_coverage,
    )
    if spot_price is None:
        failures = True
        reasons.append("spot_price_unavailable")
    if failures:
        print(f"ERROR: smoke check failed ({', '.join(reasons)})")
        return 1

    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
