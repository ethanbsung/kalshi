from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import json

import aiosqlite

from kalshi_bot.kalshi.rest_client import KalshiRestClient

BTC_SERIES_TICKERS = ["KXBTC", "KXBTC15M", "KXBTCD"]


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


def extract_close_ts(
    market: dict[str, Any], logger: logging.Logger | None = None
) -> int | None:
    for key in ("close_time", "expected_expiration_time"):
        if key in market:
            value = market.get(key)
            parsed = _parse_ts(value)
            if parsed is not None:
                return parsed
            if logger is not None and value is not None:
                logger.info(
                    "kalshi_close_parse_failed",
                    extra={"key": key, "value": value},
                )
    return None


def extract_expected_expiration_ts(
    market: dict[str, Any], logger: logging.Logger | None = None
) -> int | None:
    value = market.get("expected_expiration_time")
    parsed = _parse_ts(value)
    if parsed is not None:
        return parsed
    if logger is not None and value is not None:
        logger.info(
            "kalshi_expected_expiration_parse_failed",
            extra={"value": value},
        )
    return None


def extract_expiration_ts(
    market: dict[str, Any], logger: logging.Logger | None = None
) -> int | None:
    value = market.get("expiration_time")
    parsed = _parse_ts(value)
    if parsed is not None:
        return parsed
    if logger is not None and value is not None:
        logger.info(
            "kalshi_expiration_parse_failed",
            extra={"value": value},
        )
    return None


def extract_settlement_ts(
    market: dict[str, Any], logger: logging.Logger | None = None
) -> int | None:
    """Backward-compatible alias: settlement_ts now means market close time."""
    return extract_close_ts(market, logger=logger)


def empty_series_tickers(
    per_series: dict[str, dict[str, Any]]
) -> list[str]:
    empty = []
    for series, info in per_series.items():
        markets = info.get("markets")
        if not markets:
            empty.append(series)
    return empty


async def backfill_market_times(
    conn: aiosqlite.Connection, logger: logging.Logger | None
) -> int:
    cursor = await conn.execute(
        "SELECT market_id, settlement_ts, close_ts, expected_expiration_ts, raw_json "
        "FROM kalshi_markets"
    )
    rows = await cursor.fetchall()
    updated = 0
    for market_id, settlement_ts, close_ts_db, expected_expiration_db, raw_json in rows:
        if not raw_json:
            continue
        try:
            market = json.loads(raw_json)
        except (TypeError, ValueError):
            continue
        if not isinstance(market, dict):
            continue
        close_ts = extract_close_ts(market, logger=logger) or close_ts_db
        expected_expiration_ts = (
            extract_expected_expiration_ts(market, logger=logger)
            or expected_expiration_db
        )
        expiration_ts = extract_expiration_ts(market, logger=logger)
        new_settlement = close_ts if close_ts is not None else settlement_ts
        if (
            new_settlement is None
            and close_ts is None
            and expected_expiration_ts is None
            and expiration_ts is None
        ):
            continue
        await conn.execute(
            "UPDATE kalshi_markets "
            "SET settlement_ts = ?, "
            "close_ts = COALESCE(?, close_ts), "
            "expected_expiration_ts = COALESCE(?, expected_expiration_ts), "
            "expiration_ts = COALESCE(?, expiration_ts) "
            "WHERE market_id = ?",
            (
                new_settlement,
                close_ts,
                expected_expiration_ts,
                expiration_ts,
                market_id,
            ),
        )
        updated += 1

    await conn.execute(
        "UPDATE kalshi_contracts "
        "SET settlement_ts = (SELECT settlement_ts FROM kalshi_markets WHERE market_id = ticker), "
        "close_ts = (SELECT close_ts FROM kalshi_markets WHERE market_id = ticker), "
        "expected_expiration_ts = (SELECT expected_expiration_ts FROM kalshi_markets WHERE market_id = ticker), "
        "expiration_ts = (SELECT expiration_ts FROM kalshi_markets WHERE market_id = ticker) "
        "WHERE ticker IN (SELECT market_id FROM kalshi_markets)"
    )
    return updated


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
