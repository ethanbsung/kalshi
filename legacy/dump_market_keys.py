from __future__ import annotations

import asyncio
import os
from typing import Any

from kalshi_bot.config import load_settings
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.kalshi.btc_markets import fetch_btc_markets
from kalshi_bot.kalshi.rest_client import KalshiRestClient


def _nested_keys(value: Any) -> list[str] | None:
    if isinstance(value, dict):
        return sorted(value.keys())
    if isinstance(value, list) and value:
        first = value[0]
        if isinstance(first, dict):
            return sorted(first.keys())
    return None


async def _dump_keys() -> int:
    settings = load_settings()
    logger = setup_logger(settings.log_path)
    client = KalshiRestClient(
        base_url=settings.kalshi_rest_url,
        api_key_id=settings.kalshi_api_key_id,
        private_key_path=settings.kalshi_private_key_path,
        logger=logger,
    )
    markets, per_series = await fetch_btc_markets(
        client,
        status=settings.kalshi_market_status,
        limit=int(os.getenv("KALSHI_DISCOVERY_LIMIT", "200")),
        logger=logger,
    )

    print(
        "series counts: "
        f"{ {series: len(info.get('markets', [])) for series, info in per_series.items()} }"
    )
    if markets:
        keys = sorted(k for k in markets[0].keys())
        print(f"sample market keys: {keys}")
        for key in keys:
            nested = _nested_keys(markets[0].get(key))
            if nested:
                print(f"  {key} keys: {nested}")
    return 0


def main() -> int:
    return asyncio.run(_dump_keys())


if __name__ == "__main__":
    raise SystemExit(main())
