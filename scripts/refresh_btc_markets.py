from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import time
from pathlib import Path
from typing import Any

import aiosqlite
import sqlite3

from kalshi_bot.config import load_settings
from kalshi_bot.data.dao import Dao
from kalshi_bot.data.db import init_db
from kalshi_bot.events import EventPublisher, MarketLifecycleEvent
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.kalshi.btc_markets import (
    BTC_SERIES_TICKERS,
    backfill_market_times,
    empty_series_tickers,
    extract_close_ts,
    extract_expected_expiration_ts,
    extract_expiration_ts,
    extract_strike_basic,
    fetch_btc_markets,
)
from kalshi_bot.kalshi.rest_client import KalshiRestClient
from kalshi_bot.kalshi.rest_client import KalshiRestError
from kalshi_bot.kalshi.market_filters import normalize_db_status


RETRY_DELAYS_SECONDS = [5.0, 10.0, 20.0, 30.0]
DB_LOCK_RETRY_DELAYS_SECONDS = [5.0, 10.0, 20.0, 30.0]
LOCK_PATH = Path("/tmp/kalshi_refresh_btc_markets.lock")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh BTC markets")
    parser.add_argument(
        "--status",
        type=str,
        default=None,
        help=(
            "Kalshi market status filter override. "
            "Default behavior: publish-only uses all statuses."
        ),
    )
    parser.add_argument(
        "--publish-only",
        action="store_true",
        help="Publish market_lifecycle events only and skip SQLite writes.",
    )
    parser.add_argument(
        "--events-jsonl-path",
        type=str,
        default=None,
        help="Optional JSONL file path for publishing market_lifecycle events.",
    )
    parser.add_argument(
        "--events-bus",
        action="store_true",
        help="Publish market_lifecycle events to JetStream.",
    )
    parser.add_argument(
        "--events-bus-url",
        type=str,
        default=None,
        help="JetStream bus URL override (default BUS_URL).",
    )
    return parser.parse_args()


async def _load_existing_market_rows(
    conn: aiosqlite.Connection, market_ids: list[str]
) -> dict[str, dict[str, Any]]:
    if not market_ids:
        return {}
    existing: dict[str, dict[str, Any]] = {}
    chunk_size = 500
    for idx in range(0, len(market_ids), chunk_size):
        chunk = market_ids[idx : idx + chunk_size]
        placeholders = ", ".join("?" for _ in chunk)
        cursor = await conn.execute(
            f"""
            SELECT market_id, title, strike, settlement_ts, close_ts,
                   expected_expiration_ts, expiration_ts, status
            FROM kalshi_markets
            WHERE market_id IN ({placeholders})
            """,
            chunk,
        )
        for (
            market_id,
            title,
            strike,
            settlement_ts,
            close_ts,
            expected_expiration_ts,
            expiration_ts,
            status,
        ) in await cursor.fetchall():
            existing[market_id] = {
                "title": title,
                "strike": strike,
                "settlement_ts": settlement_ts,
                "close_ts": close_ts,
                "expected_expiration_ts": expected_expiration_ts,
                "expiration_ts": expiration_ts,
                "status": status,
            }
    return existing


def _series_market_clause(
    series_tickers: list[str],
) -> tuple[str, list[str]]:
    clauses: list[str] = []
    params: list[str] = []
    for series in series_tickers:
        clauses.append("(market_id = ? OR market_id LIKE ?)")
        params.extend([series, f"{series}-%"])
    return " OR ".join(clauses), params


async def _demote_missing_active_markets(
    conn: aiosqlite.Connection,
    *,
    fetched_market_ids: set[str],
    now_ts: int,
    ts_loaded: int,
    series_tickers: list[str],
) -> dict[str, Any]:
    if not series_tickers:
        return {
            "active_considered": 0,
            "demoted_total": 0,
            "demoted_expired": 0,
            "demoted_closed": 0,
            "demoted_samples": [],
            "demoted_rows": [],
        }

    series_where, series_params = _series_market_clause(series_tickers)
    cursor = await conn.execute(
        "SELECT market_id, close_ts, expected_expiration_ts, settlement_ts "
        "FROM kalshi_markets "
        f"WHERE status = 'active' AND ({series_where})",
        series_params,
    )
    rows = await cursor.fetchall()

    updates: list[tuple[str, int, str]] = []
    demoted_expired = 0
    demoted_closed = 0
    demoted_samples: list[str] = []
    demoted_rows: list[dict[str, Any]] = []

    for market_id, close_ts, expected_expiration_ts, settlement_ts in rows:
        if market_id in fetched_market_ids:
            continue
        close_value = close_ts or expected_expiration_ts or settlement_ts
        if close_value is not None and int(close_value) < (now_ts - 5):
            new_status = "expired"
            demoted_expired += 1
        else:
            new_status = "closed"
            demoted_closed += 1
        updates.append((new_status, ts_loaded, str(market_id)))
        demoted_rows.append(
            {
                "market_id": str(market_id),
                "status": new_status,
                "close_ts": close_ts,
                "expected_expiration_ts": expected_expiration_ts,
                "settlement_ts": settlement_ts,
            }
        )
        if len(demoted_samples) < 10:
            demoted_samples.append(str(market_id))

    if updates:
        await conn.executemany(
            "UPDATE kalshi_markets "
            "SET status = ?, ts_loaded = ? "
            "WHERE market_id = ? AND status = 'active'",
            updates,
        )

    return {
        "active_considered": len(rows),
        "demoted_total": len(updates),
        "demoted_expired": demoted_expired,
        "demoted_closed": demoted_closed,
        "demoted_samples": demoted_samples,
        "demoted_rows": demoted_rows,
    }


def _same_value(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is b
    if isinstance(a, float) or isinstance(b, float):
        try:
            return abs(float(a) - float(b)) < 1e-9
        except (TypeError, ValueError):
            return False
    return a == b


def _market_row_changed(existing: dict[str, Any] | None, row: dict[str, Any]) -> bool:
    if existing is None:
        return True
    keys = (
        "title",
        "strike",
        "settlement_ts",
        "close_ts",
        "expected_expiration_ts",
        "expiration_ts",
        "status",
    )
    for key in keys:
        if not _same_value(existing.get(key), row.get(key)):
            return True
    return False


async def _refresh_btc_markets(args: argparse.Namespace) -> int:
    lock_file = LOCK_PATH.open("w")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("refresh_btc_markets already running; skipping.")
        lock_file.close()
        return 0

    settings = load_settings()
    logger = setup_logger(settings.log_path)

    rest_client = KalshiRestClient(
        base_url=settings.kalshi_rest_url,
        api_key_id=settings.kalshi_api_key_id,
        private_key_path=settings.kalshi_private_key_path,
        logger=logger,
    )
    status_filter = (
        normalize_db_status(args.status)
        if args.status is not None
        else (
            None
            if args.publish_only
            else normalize_db_status(settings.kalshi_market_status)
        )
    )

    attempt = 0
    while True:
        try:
            markets, per_series = await fetch_btc_markets(
                rest_client,
                status=status_filter,
                limit=settings.kalshi_market_limit,
                logger=logger,
            )
            break
        except KalshiRestError as exc:
            if not exc.transient or attempt >= len(RETRY_DELAYS_SECONDS):
                if exc.transient:
                    print(
                        "WARNING: refresh_btc_markets rate-limited repeatedly; "
                        "skipping this run to avoid crash."
                    )
                    logger.warning(
                        "kalshi_refresh_btc_markets_rate_limited_skip",
                        extra={
                            "status": exc.status,
                            "body": exc.body,
                            "attempts": attempt + 1,
                        },
                    )
                    return 0
                raise
            delay = RETRY_DELAYS_SECONDS[attempt]
            attempt += 1
            print(
                f"WARNING: refresh_btc_markets transient error "
                f"(status={exc.status}); retrying in {delay:.0f}s "
                f"(attempt {attempt}/{len(RETRY_DELAYS_SECONDS)})"
            )
            logger.warning(
                "kalshi_refresh_btc_markets_retry",
                extra={
                    "status": exc.status,
                    "body": exc.body,
                    "attempt": attempt,
                    "delay_seconds": delay,
                },
            )
            await asyncio.sleep(delay)

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

    if args.publish_only:
        event_publish_failures = 0
        bus_url = (
            args.events_bus_url
            if args.events_bus_url
            else (settings.bus_url if args.events_bus else None)
        )
        event_sink = await EventPublisher.create(
            jsonl_path=args.events_jsonl_path,
            bus_url=bus_url,
        )
        try:
            if event_sink.enabled:
                for market in markets:
                    market_id = market.get("ticker") or market.get("market_id")
                    status = market.get("status")
                    if not isinstance(market_id, str) or not market_id:
                        continue
                    if not isinstance(status, str) or not status:
                        continue
                    try:
                        close_ts = extract_close_ts(market, logger=logger)
                        await event_sink.publish(
                            MarketLifecycleEvent(
                                source="refresh_btc_markets",
                                payload={
                                    "market_id": market_id,
                                    "status": status,
                                    "close_ts": close_ts,
                                    "expected_expiration_ts": (
                                        extract_expected_expiration_ts(
                                            market, logger=logger
                                        )
                                    ),
                                    "expiration_ts": extract_expiration_ts(
                                        market, logger=logger
                                    ),
                                    "settlement_ts": close_ts,
                                },
                            )
                        )
                    except Exception:
                        event_publish_failures += 1
        finally:
            await event_sink.close()
        print(
            "Refresh summary: publish_only=True fetched={fetched} "
            "event_publish_failures={event_publish_failures}".format(
                fetched=len(markets),
                event_publish_failures=event_publish_failures,
            )
        )
        lock_file.close()
        return 0

    logger.info(
        "kalshi_refresh_db_path",
        extra={"db_path": str(settings.db_path)},
    )
    print(f"DB path: {settings.db_path}")
    await init_db(settings.db_path)

    async def _write_market_rows_once() -> tuple[
        int, int, int, dict[str, Any], list[dict[str, Any]]
    ]:
        async with aiosqlite.connect(settings.db_path) as conn:
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute("PRAGMA journal_mode = WAL;")
            await conn.execute("PRAGMA synchronous = NORMAL;")
            await conn.execute("PRAGMA busy_timeout = 15000;")
            await conn.commit()

            dao = Dao(conn)
            ts_loaded = int(time.time())
            upserted = 0
            unchanged = 0
            changed_rows: list[dict[str, Any]] = []
            market_ids = [
                market.get("ticker") or market.get("market_id")
                for market in markets
                if isinstance(market, dict)
                and isinstance((market.get("ticker") or market.get("market_id")), str)
                and (market.get("ticker") or market.get("market_id"))
            ]
            existing_rows = await _load_existing_market_rows(conn, market_ids)

            for market in markets:
                if not isinstance(market, dict):
                    continue
                market_id = market.get("ticker") or market.get("market_id")
                if not market_id:
                    continue
                strike = extract_strike_basic(market)
                close_ts = extract_close_ts(market, logger=logger)
                row = {
                    "market_id": market_id,
                    "ts_loaded": ts_loaded,
                    "title": market.get("title"),
                    "strike": strike,
                    "settlement_ts": close_ts,
                    "close_ts": close_ts,
                    "expected_expiration_ts": extract_expected_expiration_ts(
                        market, logger=logger
                    ),
                    "expiration_ts": extract_expiration_ts(market, logger=logger),
                    "status": market.get("status"),
                    "raw_json": json.dumps(market),
                }
                if not _market_row_changed(existing_rows.get(market_id), row):
                    unchanged += 1
                    continue
                await dao.upsert_kalshi_market(row)
                changed_rows.append(row)
                upserted += 1
                # Reduce long write-lock windows during large refreshes.
                if upserted % 100 == 0:
                    await conn.commit()

            backfilled = await backfill_market_times(conn, logger)
            demotion = await _demote_missing_active_markets(
                conn,
                fetched_market_ids=set(market_ids),
                now_ts=ts_loaded,
                ts_loaded=ts_loaded,
                series_tickers=list(BTC_SERIES_TICKERS),
            )
            await conn.commit()
            return upserted, unchanged, backfilled, demotion, changed_rows

    db_lock_attempt = 0
    while True:
        try:
            (
                upserted,
                unchanged,
                backfilled,
                demotion,
                changed_rows,
            ) = await _write_market_rows_once()
            break
        except sqlite3.OperationalError as exc:
            if "database is locked" not in str(exc).lower():
                raise
            if db_lock_attempt >= len(DB_LOCK_RETRY_DELAYS_SECONDS):
                print(
                    "WARNING: refresh_btc_markets database remained locked after retries; "
                    "skipping this run."
                )
                logger.warning(
                    "kalshi_refresh_btc_markets_db_locked_skip",
                    extra={"attempts": db_lock_attempt + 1, "error": str(exc)},
                )
                lock_file.close()
                return 0
            delay = DB_LOCK_RETRY_DELAYS_SECONDS[db_lock_attempt]
            db_lock_attempt += 1
            print(
                f"WARNING: refresh_btc_markets database locked; retrying in {delay:.0f}s "
                f"(attempt {db_lock_attempt}/{len(DB_LOCK_RETRY_DELAYS_SECONDS)})"
            )
            logger.warning(
                "kalshi_refresh_btc_markets_db_locked_retry",
                extra={
                    "attempt": db_lock_attempt,
                    "delay_seconds": delay,
                    "error": str(exc),
                },
            )
            await asyncio.sleep(delay)

    event_publish_failures = 0
    bus_url = (
        args.events_bus_url
        if args.events_bus_url
        else (settings.bus_url if args.events_bus else None)
    )
    event_sink = await EventPublisher.create(
        jsonl_path=args.events_jsonl_path,
        bus_url=bus_url,
    )
    try:
        if event_sink.enabled:
            for row in changed_rows:
                status = row.get("status")
                market_id = row.get("market_id")
                if not isinstance(market_id, str) or not market_id:
                    continue
                if not isinstance(status, str) or not status:
                    continue
                try:
                    await event_sink.publish(
                        MarketLifecycleEvent(
                            source="refresh_btc_markets",
                            payload={
                                "market_id": market_id,
                                "status": status,
                                "close_ts": row.get("close_ts"),
                                "expected_expiration_ts": row.get(
                                    "expected_expiration_ts"
                                ),
                                "expiration_ts": row.get("expiration_ts"),
                                "settlement_ts": row.get("settlement_ts"),
                            },
                        )
                    )
                except Exception:
                    event_publish_failures += 1

            for demoted in demotion.get("demoted_rows", []):
                if not isinstance(demoted, dict):
                    continue
                market_id = demoted.get("market_id")
                status = demoted.get("status")
                if not isinstance(market_id, str) or not market_id:
                    continue
                if not isinstance(status, str) or not status:
                    continue
                try:
                    await event_sink.publish(
                        MarketLifecycleEvent(
                            source="refresh_btc_markets",
                            payload={
                                "market_id": market_id,
                                "status": status,
                                "close_ts": demoted.get("close_ts"),
                                "expected_expiration_ts": demoted.get(
                                    "expected_expiration_ts"
                                ),
                                "expiration_ts": None,
                                "settlement_ts": demoted.get("settlement_ts"),
                            },
                        )
                    )
                except Exception:
                    event_publish_failures += 1
    finally:
        await event_sink.close()

    logger.info(
        "kalshi_refresh_btc_markets",
        extra={
            "fetched_total": len(markets),
            "selected_total": len(markets),
            "upserted_total": upserted,
            "unchanged_total": unchanged,
            "backfilled_total": backfilled,
            "demoted_total": demotion.get("demoted_total", 0),
            "demoted_expired": demotion.get("demoted_expired", 0),
            "demoted_closed": demotion.get("demoted_closed", 0),
            "demoted_samples": demotion.get("demoted_samples", []),
            "event_publish_failures": event_publish_failures,
            "sample_tickers": sample_tickers,
            "series_counts": per_series_counts,
        },
    )
    print(
        "Refresh summary: upserted={upserted} unchanged={unchanged} "
        "demoted={demoted_total} event_publish_failures={event_publish_failures}".format(
            upserted=upserted,
            unchanged=unchanged,
            demoted_total=demotion.get("demoted_total", 0),
            event_publish_failures=event_publish_failures,
        )
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
        lock_file.close()
        return 1

    lock_file.close()
    return 0


def main() -> int:
    args = _parse_args()
    return asyncio.run(_refresh_btc_markets(args))


if __name__ == "__main__":
    raise SystemExit(main())
