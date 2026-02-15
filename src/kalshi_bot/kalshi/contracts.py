from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Any, Protocol

import aiosqlite

from kalshi_bot.data.dao import Dao
from kalshi_bot.events import ContractUpdateEvent, ContractUpdatePayload
from kalshi_bot.kalshi.error_utils import (
    add_failed_sample,
    classify_exception,
    init_error_counts,
)
from kalshi_bot.kalshi.btc_markets import (
    BTC_SERIES_TICKERS,
    extract_close_ts,
    extract_expected_expiration_ts,
    extract_expiration_ts,
)
from kalshi_bot.kalshi.market_filters import (
    build_series_clause,
    normalize_db_status,
    normalize_series,
)
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


def _is_btc_series_ticker(ticker: str) -> bool:
    for series in BTC_SERIES_TICKERS:
        if ticker == series or ticker.startswith(f"{series}-"):
            return True
    return False


def build_contract_row(
    ticker: str,
    settlement_ts: int | None,
    market: dict[str, Any] | None,
    logger: logging.Logger,
) -> dict[str, Any]:
    lower: float | None = None
    upper: float | None = None
    strike_type: str | None = None
    expiration_ts: int | None = None
    expected_expiration_ts: int | None = None
    close_ts = settlement_ts  # settlement_ts now represents market close time.

    raw_json: str | None = None
    if market is not None:
        lower, upper, strike_type = bounds_from_payload(market)
        parsed_close = extract_close_ts(market, logger=logger)
        if parsed_close is not None:
            close_ts = parsed_close
        expected_expiration_ts = extract_expected_expiration_ts(
            market, logger=logger
        )
        expiration_ts = extract_expiration_ts(market, logger=logger)
        try:
            raw_json = json.dumps(market)
        except (TypeError, ValueError):
            raw_json = None

    if lower is None and upper is None:
        if _is_btc_series_ticker(ticker):
            logger.warning(
                "kalshi_contract_btc_missing_bounds",
                extra={"ticker": ticker},
            )
        else:
            parsed_lower, parsed_upper, parsed_type = bounds_from_ticker(ticker)
            if parsed_type is not None:
                lower, upper, strike_type = (
                    parsed_lower,
                    parsed_upper,
                    parsed_type,
                )
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
        "settlement_ts": close_ts,
        "close_ts": close_ts,
        "expected_expiration_ts": expected_expiration_ts,
        "expiration_ts": expiration_ts,
        "settled_ts": None,
        "outcome": None,
        "raw_json": raw_json,
        "updated_ts": int(time.time()),
    }


async def load_market_rows(
    conn: aiosqlite.Connection,
    status: str | None,
    series: list[str] | None,
    *,
    now_ts: int | None = None,
    include_closed: bool = True,
    refresh_age_seconds: int | None = None,
    limit: int | None = None,
) -> list[tuple[str, int | None]]:
    status = normalize_db_status(status)
    where: list[str] = []
    params: list[Any] = []
    if status is not None:
        where.append("m.status = ?")
        params.append(status)
    series_clause, series_params = build_series_clause(series, column="m.market_id")
    if series_clause:
        where.append(series_clause)
        params.extend(series_params)
    if not include_closed and now_ts is not None:
        where.append(
            "COALESCE(m.close_ts, m.expected_expiration_ts, m.settlement_ts) >= ?"
        )
        params.append(now_ts)
    if refresh_age_seconds is not None and refresh_age_seconds >= 0 and now_ts is not None:
        refresh_before_ts = now_ts - int(refresh_age_seconds)
        where.append(
            "("
            "c.ticker IS NULL "
            "OR c.updated_ts IS NULL "
            "OR c.updated_ts <= ? "
            "OR (c.lower IS NULL AND c.upper IS NULL) "
            "OR c.close_ts IS NULL "
            "OR c.expected_expiration_ts IS NULL"
            ")"
        )
        params.append(refresh_before_ts)
    sql = (
        "SELECT m.market_id, COALESCE(m.close_ts, m.expected_expiration_ts) "
        "FROM kalshi_markets m "
        "LEFT JOIN kalshi_contracts c ON c.ticker = m.market_id"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY COALESCE(m.close_ts, m.expected_expiration_ts, m.settlement_ts), m.market_id"
    if limit is not None and limit > 0:
        sql += f" LIMIT {int(limit)}"
    cursor = await conn.execute(sql, params)
    rows = await cursor.fetchall()
    return [(row[0], row[1]) for row in rows if row and row[0]]


class EventSink(Protocol):
    @property
    def enabled(self) -> bool:
        ...

    async def publish(self, event: ContractUpdateEvent) -> None:
        ...


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
        conn: aiosqlite.Connection | None,
        status: str | None = None,
        series: list[str] | None = None,
        rows: list[tuple[str, int | None]] | None = None,
        end_time: float | None = None,
        event_sink: EventSink | None = None,
        publish_only: bool = False,
    ) -> dict[str, Any]:
        normalized_series = normalize_series(series)
        if rows is None:
            if conn is None:
                raise ValueError(
                    "conn is required when rows are not provided in refresh()"
                )
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

        dao = Dao(conn) if (conn is not None and not publish_only) else None
        successes = 0
        failures = 0
        upserted = 0
        error_counts = init_error_counts()
        failed_samples: list[str] = []
        failed_set: set[str] = set()
        event_publish_failures = 0

        for (ticker, settlement_ts), (_, market, bucket) in zip(rows, payloads):
            if bucket is not None:
                failures += 1
                error_counts[bucket] += 1
                add_failed_sample(failed_samples, failed_set, ticker)
                market = None
            else:
                successes += 1
            row = build_contract_row(ticker, settlement_ts, market, self._logger)
            if dao is not None:
                await dao.upsert_kalshi_contract(row)
                upserted += 1
            if event_sink is not None and event_sink.enabled:
                try:
                    await event_sink.publish(
                        ContractUpdateEvent(
                            source="refresh_kalshi_contracts",
                            payload=ContractUpdatePayload(
                                ticker=str(row["ticker"]),
                                lower=row.get("lower"),
                                upper=row.get("upper"),
                                strike_type=row.get("strike_type"),
                                close_ts=row.get("close_ts"),
                                expected_expiration_ts=row.get(
                                    "expected_expiration_ts"
                                ),
                                expiration_ts=row.get("expiration_ts"),
                                settled_ts=row.get("settled_ts"),
                                outcome=row.get("outcome"),
                            ),
                        )
                    )
                except Exception:
                    event_publish_failures += 1
            # Release writer lock periodically to reduce SQLite contention.
            if dao is not None and conn is not None and upserted % 100 == 0:
                await conn.commit()

        if dao is not None and conn is not None:
            await conn.commit()

        self._logger.info(
            "kalshi_contracts_refresh_summary",
            extra={
                "successes": successes,
                "failures": failures,
                "upserted": upserted,
                "error_counts": error_counts,
                "failed_tickers_sample": failed_samples,
                "event_publish_failures": event_publish_failures,
            },
        )
        return {
            "successes": successes,
            "failures": failures,
            "upserted": upserted,
            "error_counts": error_counts,
            "failed_tickers_sample": failed_samples,
            "event_publish_failures": event_publish_failures,
        }
