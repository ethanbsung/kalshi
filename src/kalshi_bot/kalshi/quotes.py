from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from typing import Any, Awaitable, Callable

import aiosqlite

from kalshi_bot.data.dao import Dao
from kalshi_bot.kalshi.error_utils import (
    add_failed_sample,
    classify_exception,
    init_error_counts,
)
from kalshi_bot.kalshi.market_filters import (
    build_series_clause,
    normalize_db_status,
    normalize_series,
)
from kalshi_bot.kalshi.rest_client import KalshiRestClient
from kalshi_bot.kalshi.validation import validate_quote_row

MarketFetcher = Callable[[str, float | None], Awaitable[dict[str, Any]]]


def _parse_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _mid(bid: float | None, ask: float | None) -> float | None:
    if bid is None or ask is None:
        return None
    return (bid + ask) / 2.0


def build_quote_row_from_topbook(
    *,
    market_id: str,
    ts: int,
    yes_bid: float | None,
    yes_ask: float | None,
    no_bid: float | None,
    no_ask: float | None,
    volume: int | None,
    open_interest: int | None,
    raw_json: str,
) -> dict[str, Any]:
    if not market_id:
        raise ValueError("missing market_id")

    yes_mid = _mid(yes_bid, yes_ask)
    no_mid = _mid(no_bid, no_ask)
    p_mid = yes_mid / 100.0 if yes_mid is not None else None

    return {
        "ts": ts,
        "market_id": market_id,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "yes_mid": yes_mid,
        "no_mid": no_mid,
        "p_mid": p_mid,
        "volume": volume,
        "volume_24h": None,
        "open_interest": open_interest,
        "raw_json": raw_json,
    }


def build_quote_row(market: dict[str, Any], ts: int) -> dict[str, Any]:
    market_id = market.get("ticker") or market.get("market_id")
    if not market_id:
        raise ValueError("missing market_id")

    yes_bid = _parse_float(market.get("yes_bid"))
    yes_ask = _parse_float(market.get("yes_ask"))
    no_bid = _parse_float(market.get("no_bid"))
    no_ask = _parse_float(market.get("no_ask"))

    row = build_quote_row_from_topbook(
        market_id=market_id,
        ts=ts,
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        volume=_parse_int(market.get("volume")),
        open_interest=_parse_int(market.get("open_interest")),
        raw_json=json.dumps(market),
    )
    row["volume_24h"] = _parse_int(market.get("volume_24h"))
    return row


async def load_market_tickers(
    conn: aiosqlite.Connection,
    status: str | None,
    series: list[str] | None,
    *,
    now_ts: int | None = None,
    max_horizon_seconds: int | None = None,
    grace_seconds: int = 3600,
) -> list[str]:
    status = normalize_db_status(status)
    where: list[str] = []
    params: list[Any] = []
    if status is not None:
        where.append("m.status = ?")
        params.append(status)
    series_clause, series_params = build_series_clause(
        series, column="m.market_id"
    )
    if series_clause:
        where.append(series_clause)
        params.extend(series_params)
    close_expr = (
        "COALESCE(c.close_ts, m.close_ts, "
        "c.expected_expiration_ts, m.expected_expiration_ts, "
        "c.settlement_ts, m.settlement_ts)"
    )
    if now_ts is not None:
        where.append(f"{close_expr} IS NOT NULL")
        where.append(f"{close_expr} >= ?")
        params.append(int(now_ts) - 5)
        if max_horizon_seconds is not None and max_horizon_seconds > 0:
            horizon_cap = int(now_ts) + int(max_horizon_seconds) + int(
                max(grace_seconds, 0)
            )
            where.append(f"{close_expr} <= ?")
            params.append(horizon_cap)
    sql = (
        "SELECT m.market_id FROM kalshi_markets m "
        "LEFT JOIN kalshi_contracts c ON c.ticker = m.market_id"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY " + close_expr + ", m.market_id"
    cursor = await conn.execute(sql, params)
    rows = await cursor.fetchall()
    return [row[0] for row in rows if row and row[0]]


class KalshiQuotePoller:
    _write_backoffs: tuple[float, ...] = (0.2, 0.5, 1.0, 2.0)

    def __init__(
        self,
        rest_client: KalshiRestClient,
        logger: logging.Logger,
        max_concurrency: int = 5,
        fetch_market: MarketFetcher | None = None,
    ) -> None:
        self._rest_client = rest_client
        self._logger = logger
        self._max_concurrency = max_concurrency
        self._fetch_market = fetch_market or self._rest_client.get_market

    @staticmethod
    def _is_locked_error(exc: sqlite3.OperationalError) -> bool:
        message = str(exc).lower()
        return "locked" in message

    async def run(
        self,
        conn: aiosqlite.Connection,
        run_seconds: int,
        poll_interval: float,
        status: str | None = None,
        series: list[str] | None = None,
        tickers: list[str] | None = None,
        max_horizon_seconds: int | None = None,
    ) -> dict[str, Any]:
        normalized_series = normalize_series(series)
        if tickers is None:
            tickers = await load_market_tickers(
                conn,
                status,
                normalized_series,
                now_ts=int(time.time()),
                max_horizon_seconds=max_horizon_seconds,
            )
        self._logger.info(
            "kalshi_quotes_load_markets",
            extra={
                "status": status,
                "series": normalized_series,
                "count": len(tickers),
            },
        )
        if not tickers:
            self._logger.warning(
                "kalshi_quotes_no_markets",
                extra={"status": status, "series": normalized_series},
            )
            empty_counts = init_error_counts()
            return {
                "successes": 0,
                "failures": 0,
                "inserted": 0,
                "error_counts": empty_counts,
                "failed_tickers_sample": [],
            }

        end_time = time.monotonic() + run_seconds
        total_success = 0
        total_fail = 0
        total_inserted = 0
        total_error_counts = init_error_counts()
        total_failed_samples: list[str] = []
        total_failed_set: set[str] = set()

        while time.monotonic() < end_time:
            summary = await self.poll_once(
                conn, tickers, end_time=end_time
            )
            total_success += summary["successes"]
            total_fail += summary["failures"]
            total_inserted += summary["inserted"]
            for key, value in summary["error_counts"].items():
                total_error_counts[key] = total_error_counts.get(key, 0) + value
            for ticker in summary["failed_tickers_sample"]:
                add_failed_sample(
                    total_failed_samples, total_failed_set, ticker
                )
            now = time.monotonic()
            if now >= end_time:
                break
            sleep_for = min(poll_interval, end_time - now)
            if sleep_for <= 0:
                break
            await asyncio.sleep(sleep_for)

        self._logger.info(
            "kalshi_quotes_run_summary",
            extra={
                "successes": total_success,
                "failures": total_fail,
                "inserted": total_inserted,
                "error_counts": total_error_counts,
                "failed_tickers_sample": total_failed_samples,
            },
        )
        return {
            "successes": total_success,
            "failures": total_fail,
            "inserted": total_inserted,
            "error_counts": total_error_counts,
            "failed_tickers_sample": total_failed_samples,
        }

    async def poll_once(
        self,
        conn: aiosqlite.Connection,
        tickers: list[str],
        end_time: float | None = None,
    ) -> dict[str, Any]:
        semaphore = asyncio.Semaphore(self._max_concurrency)
        error_counts = init_error_counts()
        failed_samples: list[str] = []
        failed_set: set[str] = set()

        async def fetch_one(
            ticker: str,
        ) -> tuple[str, dict[str, Any] | None, str | None]:
            async with semaphore:
                try:
                    market = await self._fetch_market(ticker, end_time)
                except Exception as exc:
                    bucket = classify_exception(exc)
                    if bucket == "auth_error":
                        self._logger.error(
                            "kalshi_quote_fetch_failed",
                            extra={
                                "ticker": ticker,
                                "error": str(exc),
                                "bucket": bucket,
                            },
                        )
                    elif self._logger.isEnabledFor(logging.DEBUG):
                        self._logger.warning(
                            "kalshi_quote_fetch_failed",
                            extra={
                                "ticker": ticker,
                                "error": str(exc),
                                "bucket": bucket,
                            },
                        )
                    return ticker, None, bucket
                if not isinstance(market, dict):
                    if self._logger.isEnabledFor(logging.DEBUG):
                        self._logger.warning(
                            "kalshi_quote_payload_invalid",
                            extra={"ticker": ticker},
                        )
                    return ticker, None, "invalid_payload"
                return ticker, market, None

        tasks = [asyncio.create_task(fetch_one(ticker)) for ticker in tickers]
        results = await asyncio.gather(*tasks)

        rows: list[dict[str, Any]] = []
        ts_now = int(time.time())
        for ticker, market, error_bucket in results:
            if error_bucket is not None:
                error_counts[error_bucket] += 1
                add_failed_sample(failed_samples, failed_set, ticker)
                continue
            if market is None:
                error_counts["invalid_payload"] += 1
                add_failed_sample(failed_samples, failed_set, ticker)
                continue
            try:
                row = build_quote_row(market, ts_now)
            except ValueError as exc:
                error_counts["invalid_payload"] += 1
                add_failed_sample(failed_samples, failed_set, ticker)
                if self._logger.isEnabledFor(logging.DEBUG):
                    self._logger.warning(
                        "kalshi_quote_row_invalid",
                        extra={"ticker": ticker, "error": str(exc)},
                    )
                continue

            valid, reason = validate_quote_row(row)
            if not valid:
                error_counts["invalid_payload"] += 1
                add_failed_sample(failed_samples, failed_set, ticker)
                if self._logger.isEnabledFor(logging.DEBUG):
                    self._logger.warning(
                        "kalshi_quote_row_invalid",
                        extra={"ticker": ticker, "reason": reason},
                    )
                continue
            rows.append(row)

        failures = sum(error_counts.values())
        successes = len(results) - failures
        if not rows:
            summary = {
                "successes": successes,
                "failures": failures,
                "inserted": 0,
                "error_counts": error_counts,
                "failed_tickers_sample": failed_samples,
            }
            self._logger.info(
                "kalshi_quote_poll_iteration_summary",
                extra=summary,
            )
            return summary

        dao = Dao(conn)
        write_backoffs = list(self._write_backoffs)
        inserted_count = 0
        failed_write_rows: list[dict[str, Any]] = []
        for row in rows:
            try:
                await dao.insert_kalshi_quote(row)
                inserted_count += 1
            except sqlite3.OperationalError as exc:
                if not self._is_locked_error(exc):
                    raise
                failed_write_rows.append(row)

        commit_error: sqlite3.OperationalError | None = None
        if inserted_count > 0:
            for attempt in range(len(write_backoffs) + 1):
                try:
                    await conn.commit()
                    commit_error = None
                    break
                except sqlite3.OperationalError as exc:
                    if not self._is_locked_error(exc):
                        raise
                    commit_error = exc
                    if attempt < len(write_backoffs):
                        await asyncio.sleep(write_backoffs[attempt])
            if commit_error is not None:
                # Clear pending transaction state before next iteration.
                await conn.rollback()

        write_failures = len(failed_write_rows)
        if write_failures > 0:
            error_counts["unknown"] += write_failures
            for row in failed_write_rows:
                add_failed_sample(failed_samples, failed_set, row.get("market_id"))

        if commit_error is not None:
            error_counts["unknown"] += inserted_count
            for row in rows[:inserted_count]:
                add_failed_sample(failed_samples, failed_set, row.get("market_id"))
            summary = {
                "successes": max(len(results) - sum(error_counts.values()), 0),
                "failures": sum(error_counts.values()),
                "inserted": 0,
                "error_counts": error_counts,
                "failed_tickers_sample": failed_samples,
            }
            self._logger.warning(
                "kalshi_quote_db_write_locked",
                extra={
                    **summary,
                    "attempts": len(write_backoffs) + 1,
                    "error": str(commit_error),
                },
            )
            return summary

        summary = {
            "successes": max(len(results) - sum(error_counts.values()), 0),
            "failures": sum(error_counts.values()),
            "inserted": inserted_count,
            "error_counts": error_counts,
            "failed_tickers_sample": failed_samples,
        }
        self._logger.info(
            "kalshi_quote_poll_iteration_summary",
            extra=summary,
        )
        return summary
