from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any

import aiosqlite

from kalshi_bot.data.dao import Dao
from kalshi_bot.kalshi.error_utils import (
    add_failed_sample,
    classify_exception,
    init_error_counts,
)
from kalshi_bot.kalshi.market_filters import build_series_clause, normalize_series
from kalshi_bot.kalshi.rest_client import KalshiRestClient

_STRIKE_PATTERN = re.compile(r"-(?P<side>[AB])(?P<strike>\d+(?:\.\d+)?)$")


def _parse_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_strike_type(value: Any) -> str | None:
    if value in {"between", "less", "greater"}:
        return value
    return None


def bounds_from_payload(
    market: dict[str, Any]
) -> tuple[float | None, float | None, str | None]:
    floor = _parse_float(market.get("floor_strike"))
    cap = _parse_float(market.get("cap_strike"))
    strike_type = _normalize_strike_type(market.get("strike_type"))

    if floor is None and cap is None:
        return None, None, strike_type
    if floor is not None and cap is not None:
        return floor, cap, strike_type or "between"
    if floor is not None:
        return floor, None, strike_type or "greater"
    return None, cap, strike_type or "less"


def bounds_from_ticker(ticker: str) -> tuple[float | None, float | None, str | None]:
    match = _STRIKE_PATTERN.search(ticker)
    if not match:
        return None, None, None
    strike = _parse_float(match.group("strike"))
    if strike is None:
        return None, None, None
    side = match.group("side")
    if side == "A":
        return strike, None, "greater"
    if side == "B":
        return None, strike, "less"
    return None, None, None


def build_contract_row(
    ticker: str,
    settlement_ts: int | None,
    market: dict[str, Any] | None,
    logger: logging.Logger,
) -> dict[str, Any]:
    lower: float | None = None
    upper: float | None = None
    strike_type: str | None = None

    if market is not None:
        lower, upper, strike_type = bounds_from_payload(market)

    if lower is None and upper is None:
        parsed_lower, parsed_upper, parsed_type = bounds_from_ticker(ticker)
        if parsed_type is not None:
            lower, upper, strike_type = parsed_lower, parsed_upper, parsed_type
            logger.warning(
                "kalshi_contract_ticker_fallback",
                extra={"ticker": ticker, "strike_type": strike_type},
            )
        else:
            logger.warning(
                "kalshi_contract_parse_ambiguous",
                extra={"ticker": ticker},
            )

    return {
        "ticker": ticker,
        "lower": lower,
        "upper": upper,
        "strike_type": strike_type,
        "settlement_ts": settlement_ts,
        "updated_ts": int(time.time()),
    }


async def load_market_rows(
    conn: aiosqlite.Connection, status: str | None, series: list[str] | None
) -> list[tuple[str, int | None]]:
    where: list[str] = []
    params: list[Any] = []
    if status is not None:
        where.append("status = ?")
        params.append(status)
    series_clause, series_params = build_series_clause(series)
    if series_clause:
        where.append(series_clause)
        params.extend(series_params)
    sql = "SELECT market_id, settlement_ts FROM kalshi_markets"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY market_id"
    cursor = await conn.execute(sql, params)
    rows = await cursor.fetchall()
    return [(row[0], row[1]) for row in rows if row and row[0]]


class KalshiContractRefresher:
    def __init__(
        self,
        rest_client: KalshiRestClient,
        logger: logging.Logger,
        max_concurrency: int = 5,
    ) -> None:
        self._rest_client = rest_client
        self._logger = logger
        self._max_concurrency = max_concurrency

    async def refresh(
        self,
        conn: aiosqlite.Connection,
        status: str | None = None,
        series: list[str] | None = None,
        rows: list[tuple[str, int | None]] | None = None,
        end_time: float | None = None,
    ) -> dict[str, Any]:
        normalized_series = normalize_series(series)
        if rows is None:
            rows = await load_market_rows(conn, status, normalized_series)
        self._logger.info(
            "kalshi_contracts_load_markets",
            extra={
                "status": status,
                "series": normalized_series,
                "count": len(rows),
            },
        )
        if not rows:
            self._logger.warning(
                "kalshi_contracts_no_markets",
                extra={"status": status, "series": normalized_series},
            )
            empty_counts = init_error_counts()
            return {
                "successes": 0,
                "failures": 0,
                "upserted": 0,
                "error_counts": empty_counts,
                "failed_tickers_sample": [],
            }

        semaphore = asyncio.Semaphore(self._max_concurrency)

        async def fetch_market(
            ticker: str,
        ) -> tuple[str, dict[str, Any] | None, str | None]:
            async with semaphore:
                try:
                    market = await self._rest_client.get_market(
                        ticker, end_time=end_time
                    )
                except Exception as exc:
                    bucket = classify_exception(exc)
                    if bucket == "auth_error":
                        self._logger.error(
                            "kalshi_contract_fetch_failed",
                            extra={
                                "ticker": ticker,
                                "error": str(exc),
                                "bucket": bucket,
                            },
                        )
                    elif self._logger.isEnabledFor(logging.DEBUG):
                        self._logger.warning(
                            "kalshi_contract_fetch_failed",
                            extra={
                                "ticker": ticker,
                                "error": str(exc),
                                "bucket": bucket,
                            },
                        )
                    return ticker, None, bucket
                if not isinstance(market, dict):
                    return ticker, None, "invalid_payload"
                return ticker, market, None

        tasks = [asyncio.create_task(fetch_market(ticker)) for ticker, _ in rows]
        payloads = await asyncio.gather(*tasks)

        dao = Dao(conn)
        successes = 0
        failures = 0
        upserted = 0
        error_counts = init_error_counts()
        failed_samples: list[str] = []
        failed_set: set[str] = set()

        for (ticker, settlement_ts), (_, market, bucket) in zip(rows, payloads):
            if bucket is not None:
                failures += 1
                error_counts[bucket] += 1
                add_failed_sample(failed_samples, failed_set, ticker)
                market = None
            else:
                successes += 1
            row = build_contract_row(ticker, settlement_ts, market, self._logger)
            await dao.upsert_kalshi_contract(row)
            upserted += 1

        await conn.commit()

        self._logger.info(
            "kalshi_contracts_refresh_summary",
            extra={
                "successes": successes,
                "failures": failures,
                "upserted": upserted,
                "error_counts": error_counts,
                "failed_tickers_sample": failed_samples,
            },
        )
        return {
            "successes": successes,
            "failures": failures,
            "upserted": upserted,
            "error_counts": error_counts,
            "failed_tickers_sample": failed_samples,
        }
