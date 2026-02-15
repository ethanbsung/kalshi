from __future__ import annotations

import asyncio

from kalshi_bot.config import load_settings
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.kalshi.btc_markets import empty_series_tickers, fetch_btc_markets
from kalshi_bot.kalshi.rest_client import KalshiRestClient


async def _scan() -> int:
    settings = load_settings()
    logger = setup_logger(settings.log_path)
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
    tickers = [
        ticker
        for market in markets
        for ticker in [market.get("ticker")]
        if isinstance(ticker, str) and ticker
    ][:10]
    print(
        f"FOUND {len(markets)} BTC markets "
        f"(series counts: "
        f"{ {series: len(info.get('markets', [])) for series, info in per_series.items()} })"
    )
    if tickers:
        print("Sample tickers:", ", ".join(tickers))
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
    return asyncio.run(_scan())


if __name__ == "__main__":
    raise SystemExit(main())
