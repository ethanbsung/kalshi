from __future__ import annotations

import asyncio
import json
import time

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.data.dao import Dao
from kalshi_bot.data.db import init_db
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.kalshi.btc_markets import (
    empty_series_tickers,
    extract_settlement_ts,
    extract_strike_basic,
    fetch_btc_markets,
)
from kalshi_bot.kalshi.rest_client import KalshiRestClient


async def _refresh_btc_markets() -> int:
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

    markets, per_series = await fetch_btc_markets(
        rest_client,
        status=settings.kalshi_market_status,
        limit=settings.kalshi_market_limit,
        logger=logger,
    )

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

    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 5000;")
        await conn.commit()

        dao = Dao(conn)
        ts_loaded = int(time.time())
        upserted = 0

        for market in markets:
            if not isinstance(market, dict):
                continue
            market_id = market.get("ticker") or market.get("market_id")
            if not market_id:
                continue
            strike = extract_strike_basic(market)
            row = {
                "market_id": market_id,
                "ts_loaded": ts_loaded,
                "title": market.get("title"),
                "strike": strike,
                "settlement_ts": extract_settlement_ts(market, logger=logger),
                "status": market.get("status"),
                "raw_json": json.dumps(market),
            }
            await dao.upsert_kalshi_market(row)
            upserted += 1

        await conn.commit()

    logger.info(
        "kalshi_refresh_btc_markets",
        extra={
            "fetched_total": len(markets),
            "selected_total": len(markets),
            "upserted_total": upserted,
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
        return 1

    return 0


def main() -> int:
    return asyncio.run(_refresh_btc_markets())


if __name__ == "__main__":
    raise SystemExit(main())
