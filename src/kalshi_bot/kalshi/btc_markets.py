from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from kalshi_bot.kalshi.rest_client import KalshiRestClient

BTC_SERIES_TICKERS = ["KXBTC", "KXBTC15M"]


def extract_strike_basic(market: dict[str, Any]) -> float | None:
    custom_strike = market.get("custom_strike")
    if isinstance(custom_strike, (int, float)):
        return float(custom_strike)
    if isinstance(custom_strike, str):
        stripped = custom_strike.replace(",", "").strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _parse_ts(value: Any) -> int | None:
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
        try:
            if stripped.endswith("Z"):
                stripped = stripped[:-1] + "+00:00"
            parsed = datetime.fromisoformat(stripped)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
            return int(parsed.timestamp())
        except ValueError:
            return None
    return None


def extract_settlement_ts(
    market: dict[str, Any], logger: logging.Logger | None = None
) -> int | None:
    for key in ("expiration_time", "close_time"):
        if key in market:
            value = market.get(key)
            parsed = _parse_ts(value)
            if parsed is not None:
                return parsed
            if logger is not None and value is not None:
                logger.info(
                    "kalshi_settlement_parse_failed",
                    extra={"key": key, "value": value},
                )
    return None


def empty_series_tickers(
    per_series: dict[str, dict[str, Any]]
) -> list[str]:
    empty = []
    for series, info in per_series.items():
        markets = info.get("markets")
        if not markets:
            empty.append(series)
    return empty


async def fetch_btc_markets(
    rest_client: KalshiRestClient,
    status: str | None,
    limit: int | None,
    logger: logging.Logger,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    all_markets: list[dict[str, Any]] = []
    per_series: dict[str, dict[str, Any]] = {}

    for series_ticker in BTC_SERIES_TICKERS:
        markets, payloads = await rest_client.list_markets_by_series(
            series_ticker=series_ticker,
            status=status,
            limit=limit,
        )
        per_series[series_ticker] = {
            "markets": markets,
            "payloads": payloads,
        }
        all_markets.extend(markets)
        logger.info(
            "kalshi_btc_series_markets",
            extra={
                "series_ticker": series_ticker,
                "count": len(markets),
            },
        )

    return all_markets, per_series
