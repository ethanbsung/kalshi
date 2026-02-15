from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import logging
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.data import init_db
from kalshi_bot.data.dao import Dao
from kalshi_bot.events import ContractUpdateEvent, ContractUpdatePayload, EventPublisher
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.kalshi.rest_client import KalshiRestClient, KalshiRestError
from kalshi_bot.kalshi.contracts import build_contract_row
from kalshi_bot.kalshi.btc_markets import BTC_SERIES_TICKERS
from kalshi_bot.kalshi.market_filters import normalize_series

LOCK_PATH = Path(tempfile.gettempdir()) / "kalshi_refresh_kalshi_settlements.lock"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh settled outcomes into kalshi_contracts."
    )
    parser.add_argument(
        "--status",
        type=str,
        default="resolved",
        choices=["resolved", "settled", "all"],
        help="Settlement status filter (resolved maps to settled).",
    )
    parser.add_argument(
        "--since-seconds",
        type=int,
        default=7 * 24 * 3600,
        help="Only consider markets settled within this many seconds.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=600,
        help="Maximum number of markets to fetch.",
    )
    parser.add_argument(
        "--series",
        action="append",
        default=None,
        help="Only process market tickers with these series prefixes.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite conflicting outcomes already stored in DB.",
    )
    parser.add_argument(
        "--events-jsonl-path",
        type=str,
        default=None,
        help="Optional JSONL file path for publishing contract_update events.",
    )
    parser.add_argument(
        "--events-bus",
        action="store_true",
        help="Publish contract_update events to JetStream.",
    )
    parser.add_argument(
        "--events-bus-url",
        type=str,
        default=None,
        help="JetStream bus URL override (default BUS_URL).",
    )
    parser.add_argument(
        "--pg-dsn",
        type=str,
        default=None,
        help="Optional Postgres DSN override (defaults to PG_DSN).",
    )
    return parser.parse_args()


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


def _parse_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_status(value: str) -> str | None:
    lowered = value.lower().strip()
    if lowered == "resolved":
        return "settled"
    if lowered == "all":
        return None
    return lowered


def _parse_outcome_from_result(value: Any) -> int | None:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        if value == 1 or value == 1.0:
            return 1
        if value == 0 or value == 0.0:
            return 0
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "yes":
            return 1
        if lowered == "no":
            return 0
        if lowered == "true":
            return 1
        if lowered == "false":
            return 0
    return None


def _derive_outcome_from_settlement_value(market: dict[str, Any]) -> int | None:
    settlement_value = _parse_float(market.get("settlement_value"))
    if settlement_value is None:
        return None
    strike_type = market.get("strike_type")
    if strike_type not in {"greater", "less", "between"}:
        return None
    floor = _parse_float(market.get("floor_strike"))
    cap = _parse_float(market.get("cap_strike"))
    if strike_type == "greater" and floor is not None:
        return 1 if settlement_value >= floor else 0
    if strike_type == "less" and cap is not None:
        return 1 if settlement_value <= cap else 0
    if strike_type == "between" and floor is not None and cap is not None:
        return 1 if floor <= settlement_value < cap else 0
    return None


def _extract_settled_ts(
    market: dict[str, Any], now_ts: int
) -> tuple[int, list[str], str]:
    errors: list[str] = []
    for key in (
        "settlement_ts",
        "settled_ts",
        "settled_time",
        "settlement_time",
        "resolved_time",
        "result_time",
        "close_time",
        "expected_expiration_time",
        "expiration_time",
    ):
        if key not in market:
            continue
        parsed = _parse_ts(market.get(key))
        if parsed is not None:
            return parsed, errors, key
    errors.append("settled_ts_missing")
    return now_ts, errors, "fallback_now"


def _extract_outcome(market: dict[str, Any]) -> tuple[int | None, list[str]]:
    outcome = _parse_outcome_from_result(market.get("result"))
    if outcome is not None:
        return outcome, []
    derived = _derive_outcome_from_settlement_value(market)
    if derived is not None:
        return derived, []
    errors: list[str] = []
    if market.get("result") is None:
        errors.append("missing_result")
    else:
        errors.append("invalid_result")
    if market.get("settlement_value") is None:
        errors.append("missing_settlement_value")
    else:
        errors.append("unable_to_derive_outcome")
    return None, errors


def build_settlement_updates(
    markets: list[dict[str, Any]],
    existing_contracts: dict[str, dict[str, Any]],
    now_ts: int,
    since_ts: int,
    logger: logging.Logger | None,
    force: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    updates: list[dict[str, Any]] = []
    create_rows: list[dict[str, Any]] = []
    if logger is None:
        logger = logging.getLogger("kalshi_settlements")
    counters = {
        "markets_after_since_filter": 0,
        "failures": 0,
        "updated_outcomes_count": 0,
        "already_had_outcome_count": 0,
        "updates_skipped_no_change": 0,
        "created_contracts": 0,
        "contracts_missing_before_create": 0,
        "conflict_outcome_count": 0,
        "updates_built_total": 0,
        "updates_with_outcome": 0,
        "updates_without_outcome": 0,
    }
    for market in markets:
        if not isinstance(market, dict):
            counters["failures"] += 1
            continue
        ticker = market.get("ticker") or market.get("market_id")
        if not isinstance(ticker, str) or not ticker:
            counters["failures"] += 1
            continue
        existing = existing_contracts.get(ticker)
        settled_ts, ts_errors, ts_source = _extract_settled_ts(market, now_ts)
        existing_settled_ts: int | None
        if settled_ts < since_ts:
            continue
        counters["markets_after_since_filter"] += 1
        outcome, outcome_errors = _extract_outcome(market)
        error_set = set(ts_errors + outcome_errors)

        if existing is None:
            create_rows.append(
                build_contract_row(ticker, settled_ts, market, logger)
            )
            existing_outcome = None
            # New contract rows are created with settlement/close timestamps,
            # so only outcome changes should require a follow-up update.
            existing_settled_ts = settled_ts
            counters["created_contracts"] += 1
            counters["contracts_missing_before_create"] += 1
        else:
            existing_outcome = existing.get("outcome")
            raw_existing_settled_ts = existing.get("settled_ts")
            try:
                existing_settled_ts = (
                    int(raw_existing_settled_ts)
                    if raw_existing_settled_ts is not None
                    else None
                )
            except (TypeError, ValueError):
                existing_settled_ts = None
        if outcome is not None:
            if existing_outcome is None or existing_outcome != outcome:
                counters["updated_outcomes_count"] += 1
            else:
                counters["already_had_outcome_count"] += 1
        elif existing_outcome is not None:
            counters["already_had_outcome_count"] += 1
        if (
            outcome is not None
            and existing_outcome is not None
            and existing_outcome != outcome
            and not force
        ):
            counters["conflict_outcome_count"] += 1
            error_set.add("outcome_conflict")
        conflict_without_force = (
            outcome is not None
            and existing_outcome is not None
            and existing_outcome != outcome
            and not force
        )
        needs_outcome_write = (
            outcome is not None
            and (
                existing_outcome is None
                or existing_outcome != outcome
            )
            and not conflict_without_force
        )
        needs_settled_ts_write = (
            settled_ts is not None and existing_settled_ts != settled_ts
        )
        if not needs_outcome_write and not needs_settled_ts_write:
            counters["updates_skipped_no_change"] += 1
            continue

        raw_json = json.dumps(
            {
                "market": market,
                "errors": sorted(error_set),
                "settled_ts_source": ts_source,
                "existing_outcome": existing_outcome,
                "new_outcome": outcome,
            }
        )
        updates.append(
            {
                "ticker": ticker,
                "outcome": outcome,
                "settled_ts": settled_ts,
                "raw_json": raw_json,
            }
        )
        counters["updates_built_total"] += 1
        if outcome is None:
            counters["updates_without_outcome"] += 1
        else:
            counters["updates_with_outcome"] += 1
    return updates, create_rows, counters


async def _load_existing_contracts(
    conn: aiosqlite.Connection, tickers: list[str]
) -> dict[str, dict[str, Any]]:
    existing: dict[str, dict[str, Any]] = {}
    if not tickers:
        return existing
    chunk_size = 500
    for idx in range(0, len(tickers), chunk_size):
        chunk = tickers[idx : idx + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        sql = (
            f"SELECT ticker, lower, upper, strike_type, close_ts, "
            f"expected_expiration_ts, expiration_ts, outcome, settled_ts "
            f"FROM kalshi_contracts "
            f"WHERE ticker IN ({placeholders})"
        )
        cursor = await conn.execute(
            sql,
            chunk,
        )
        rows = await cursor.fetchall()
        for (
            ticker,
            lower,
            upper,
            strike_type,
            close_ts,
            expected_expiration_ts,
            expiration_ts,
            outcome,
            settled_ts,
        ) in rows:
            existing[ticker] = {
                "lower": lower,
                "upper": upper,
                "strike_type": strike_type,
                "close_ts": close_ts,
                "expected_expiration_ts": expected_expiration_ts,
                "expiration_ts": expiration_ts,
                "outcome": outcome,
                "settled_ts": settled_ts,
            }
    return existing


async def _load_contract_rows_for_events(
    conn: aiosqlite.Connection, tickers: list[str]
) -> dict[str, dict[str, Any]]:
    rows_by_ticker: dict[str, dict[str, Any]] = {}
    if not tickers:
        return rows_by_ticker
    chunk_size = 500
    for idx in range(0, len(tickers), chunk_size):
        chunk = tickers[idx : idx + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        sql = (
            f"SELECT ticker, lower, upper, strike_type, close_ts, "
            f"expected_expiration_ts, expiration_ts, settled_ts, outcome "
            f"FROM kalshi_contracts WHERE ticker IN ({placeholders})"
        )
        cursor = await conn.execute(sql, chunk)
        rows = await cursor.fetchall()
        for (
            ticker,
            lower,
            upper,
            strike_type,
            close_ts,
            expected_expiration_ts,
            expiration_ts,
            settled_ts,
            outcome,
        ) in rows:
            rows_by_ticker[str(ticker)] = {
                "ticker": ticker,
                "lower": lower,
                "upper": upper,
                "strike_type": strike_type,
                "close_ts": close_ts,
                "expected_expiration_ts": expected_expiration_ts,
                "expiration_ts": expiration_ts,
                "settled_ts": settled_ts,
                "outcome": outcome,
            }
    return rows_by_ticker


def _load_existing_contracts_postgres(
    conn: Any, tickers: list[str]
) -> dict[str, dict[str, Any]]:
    existing: dict[str, dict[str, Any]] = {}
    if not tickers:
        return existing
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                ticker,
                lower,
                upper,
                strike_type,
                close_ts,
                expected_expiration_ts,
                expiration_ts,
                outcome,
                settled_ts
            FROM event_store.state_contract_latest
            WHERE ticker = ANY(%s)
            """,
            (tickers,),
        )
        for (
            ticker,
            lower,
            upper,
            strike_type,
            close_ts,
            expected_expiration_ts,
            expiration_ts,
            outcome,
            settled_ts,
        ) in cur.fetchall():
            existing[str(ticker)] = {
                "lower": lower,
                "upper": upper,
                "strike_type": strike_type,
                "close_ts": close_ts,
                "expected_expiration_ts": expected_expiration_ts,
                "expiration_ts": expiration_ts,
                "outcome": outcome,
                "settled_ts": settled_ts,
            }
    return existing


def _upsert_contract_rows_postgres(conn: Any, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO event_store.state_contract_latest (
                    ticker,
                    lower,
                    upper,
                    strike_type,
                    close_ts,
                    expected_expiration_ts,
                    expiration_ts,
                    settled_ts,
                    outcome
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker) DO UPDATE SET
                    lower = COALESCE(EXCLUDED.lower, event_store.state_contract_latest.lower),
                    upper = COALESCE(EXCLUDED.upper, event_store.state_contract_latest.upper),
                    strike_type = COALESCE(EXCLUDED.strike_type, event_store.state_contract_latest.strike_type),
                    close_ts = COALESCE(EXCLUDED.close_ts, event_store.state_contract_latest.close_ts),
                    expected_expiration_ts = COALESCE(
                        EXCLUDED.expected_expiration_ts,
                        event_store.state_contract_latest.expected_expiration_ts
                    ),
                    expiration_ts = COALESCE(EXCLUDED.expiration_ts, event_store.state_contract_latest.expiration_ts),
                    settled_ts = COALESCE(EXCLUDED.settled_ts, event_store.state_contract_latest.settled_ts),
                    outcome = COALESCE(EXCLUDED.outcome, event_store.state_contract_latest.outcome),
                    updated_at = NOW()
                """,
                (
                    row.get("ticker"),
                    row.get("lower"),
                    row.get("upper"),
                    row.get("strike_type"),
                    row.get("close_ts"),
                    row.get("expected_expiration_ts"),
                    row.get("expiration_ts"),
                    row.get("settled_ts"),
                    row.get("outcome"),
                ),
            )


def _apply_updates_postgres(
    conn: Any,
    updates: list[dict[str, Any]],
    *,
    force: bool,
) -> int:
    if not updates:
        return 0
    applied = 0
    with conn.cursor() as cur:
        for update in updates:
            cur.execute(
                """
                INSERT INTO event_store.state_contract_latest (
                    ticker, settled_ts, outcome
                ) VALUES (%s, %s, %s)
                ON CONFLICT (ticker) DO UPDATE SET
                    settled_ts = COALESCE(EXCLUDED.settled_ts, event_store.state_contract_latest.settled_ts),
                    outcome = CASE
                        WHEN EXCLUDED.outcome IS NULL THEN event_store.state_contract_latest.outcome
                        WHEN event_store.state_contract_latest.outcome IS NULL THEN EXCLUDED.outcome
                        WHEN %s THEN EXCLUDED.outcome
                        ELSE event_store.state_contract_latest.outcome
                    END,
                    updated_at = NOW()
                """,
                (
                    update.get("ticker"),
                    update.get("settled_ts"),
                    update.get("outcome"),
                    bool(force),
                ),
            )
            if cur.rowcount > 0:
                applied += 1
    return applied


def _load_contract_rows_for_events_postgres(
    conn: Any, tickers: list[str]
) -> dict[str, dict[str, Any]]:
    rows_by_ticker: dict[str, dict[str, Any]] = {}
    if not tickers:
        return rows_by_ticker
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                ticker,
                lower,
                upper,
                strike_type,
                close_ts,
                expected_expiration_ts,
                expiration_ts,
                settled_ts,
                outcome
            FROM event_store.state_contract_latest
            WHERE ticker = ANY(%s)
            """,
            (tickers,),
        )
        for (
            ticker,
            lower,
            upper,
            strike_type,
            close_ts,
            expected_expiration_ts,
            expiration_ts,
            settled_ts,
            outcome,
        ) in cur.fetchall():
            rows_by_ticker[str(ticker)] = {
                "ticker": ticker,
                "lower": lower,
                "upper": upper,
                "strike_type": strike_type,
                "close_ts": close_ts,
                "expected_expiration_ts": expected_expiration_ts,
                "expiration_ts": expiration_ts,
                "settled_ts": settled_ts,
                "outcome": outcome,
            }
    return rows_by_ticker


async def _publish_contract_updates(
    *,
    event_sink: EventPublisher,
    rows_by_ticker: dict[str, dict[str, Any]],
) -> int:
    if not event_sink.enabled or not rows_by_ticker:
        return 0
    failures = 0
    for row in rows_by_ticker.values():
        try:
            await event_sink.publish(
                ContractUpdateEvent(
                    source="refresh_kalshi_settlements",
                    payload=ContractUpdatePayload(
                        ticker=str(row["ticker"]),
                        lower=(
                            float(row["lower"])
                            if row.get("lower") is not None
                            else None
                        ),
                        upper=(
                            float(row["upper"])
                            if row.get("upper") is not None
                            else None
                        ),
                        strike_type=(
                            str(row["strike_type"])
                            if row.get("strike_type") is not None
                            else None
                        ),
                        close_ts=(
                            int(row["close_ts"])
                            if row.get("close_ts") is not None
                            else None
                        ),
                        expected_expiration_ts=(
                            int(row["expected_expiration_ts"])
                            if row.get("expected_expiration_ts") is not None
                            else None
                        ),
                        expiration_ts=(
                            int(row["expiration_ts"])
                            if row.get("expiration_ts") is not None
                            else None
                        ),
                        settled_ts=(
                            int(row["settled_ts"])
                            if row.get("settled_ts") is not None
                            else None
                        ),
                        outcome=(
                            int(row["outcome"])
                            if row.get("outcome") is not None
                            else None
                        ),
                    ),
                )
            )
        except Exception:
            failures += 1
    return failures


async def _apply_updates(
    conn: aiosqlite.Connection,
    updates: list[dict[str, Any]],
    now_ts: int,
    dry_run: bool,
    force: bool,
) -> int:
    if dry_run or not updates:
        return 0
    dao = Dao(conn)
    applied = 0
    for idx, update in enumerate(updates, start=1):
        rowcount = await dao.update_contract_outcome(
            ticker=update["ticker"],
            outcome=update["outcome"],
            settled_ts=update["settled_ts"],
            updated_ts=now_ts,
            raw_json=update["raw_json"],
            force=force,
        )
        if rowcount > 0:
            applied += 1
        if idx % 100 == 0:
            await conn.commit()
    return applied


async def _run() -> int:
    lock_file = LOCK_PATH.open("w")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("refresh_kalshi_settlements already running; skipping.")
        lock_file.close()
        return 0

    args = _parse_args()
    settings = load_settings()
    logger = setup_logger(settings.log_path)
    bus_url = (
        args.events_bus_url
        if args.events_bus_url
        else (settings.bus_url if args.events_bus else None)
    )
    event_sink = await EventPublisher.create(
        jsonl_path=args.events_jsonl_path,
        bus_url=bus_url,
    )

    pg_dsn = args.pg_dsn or settings.pg_dsn
    if pg_dsn:
        print("backend=postgres")
    else:
        print("backend=sqlite")
        print(f"DB path: {settings.db_path}")
        await init_db(settings.db_path)

    rest_client = KalshiRestClient(
        base_url=settings.kalshi_rest_url,
        api_key_id=settings.kalshi_api_key_id,
        private_key_path=settings.kalshi_private_key_path,
        logger=logger,
    )

    status = _normalize_status(args.status)
    if args.status == "resolved":
        logger.info(
            "kalshi_settlements_status_alias",
            extra={"provided": args.status, "resolved_to": status},
        )

    now_ts = int(time.time())
    since_ts = now_ts - max(args.since_seconds, 0)
    per_page = min(args.limit, 1000) if args.limit else None
    series = (
        normalize_series(args.series)
        if args.series is not None
        else list(BTC_SERIES_TICKERS)
    )

    markets: list[dict[str, Any]] = []
    if series:
        per_series_limit = None
        if args.limit:
            per_series_limit = max(args.limit // len(series), 1)
        for series_ticker in series:
            try:
                series_markets = await rest_client.list_markets(
                    limit=per_page,
                    status=status,
                    series_ticker=series_ticker,
                    min_settled_ts=since_ts,
                    max_settled_ts=now_ts,
                    max_total=per_series_limit,
                )
            except KalshiRestError as exc:
                if exc.transient:
                    print(
                        "WARNING: settlements transient API error; "
                        f"skipping series={series_ticker} status={exc.status}"
                    )
                    logger.warning(
                        "kalshi_settlements_series_skipped",
                        extra={
                            "series_ticker": series_ticker,
                            "status": exc.status,
                            "body": exc.body,
                        },
                    )
                    continue
                raise
            markets.extend(series_markets)
    else:
        try:
            markets = await rest_client.list_markets(
                limit=per_page,
                status=status,
                min_settled_ts=since_ts,
                max_settled_ts=now_ts,
                max_total=args.limit,
            )
        except KalshiRestError as exc:
            if exc.transient:
                print(
                    "WARNING: settlements transient API error; skipping this run "
                    f"(status={exc.status})"
                )
                logger.warning(
                    "kalshi_settlements_run_skipped",
                    extra={"status": exc.status, "body": exc.body},
                )
                lock_file.close()
                return 0
            raise
    markets_fetched_total = len(markets)

    # Guard against duplicate rows if the same ticker appears across pages/calls.
    deduped_markets: list[dict[str, Any]] = []
    seen_tickers: set[str] = set()
    for market in markets:
        if not isinstance(market, dict):
            continue
        ticker = market.get("ticker") or market.get("market_id")
        if not isinstance(ticker, str) or not ticker:
            continue
        if ticker in seen_tickers:
            continue
        seen_tickers.add(ticker)
        deduped_markets.append(market)
    markets = deduped_markets
    series_prefixes = tuple(
        f"{value}-" for value in series if isinstance(value, str) and value
    )
    if series_prefixes:
        markets = [
            market
            for market in markets
            if isinstance(market, dict)
            and isinstance((market.get("ticker") or market.get("market_id")), str)
            and str(market.get("ticker") or market.get("market_id")).startswith(
                series_prefixes
            )
        ]

    tickers = [
        ticker
        for market in markets
        if isinstance(market, dict)
        for ticker in [market.get("ticker") or market.get("market_id")]
        if isinstance(ticker, str) and ticker
    ]
    markets_with_ticker = len([t for t in tickers if isinstance(t, str) and t])

    event_publish_failures = 0
    applied = 0
    updates: list[dict[str, Any]] = []
    counters: dict[str, int] = {}
    try:
        if pg_dsn:
            try:
                import psycopg
            except ModuleNotFoundError as exc:
                raise RuntimeError(
                    "psycopg is required for Postgres settlements refresh. "
                    "Install with: pip install psycopg[binary]"
                ) from exc

            with psycopg.connect(pg_dsn) as pg_conn:
                existing = _load_existing_contracts_postgres(pg_conn, tickers)
                updates, create_rows, counters = build_settlement_updates(
                    markets, existing, now_ts, since_ts, logger, args.force
                )
                if not args.dry_run:
                    _upsert_contract_rows_postgres(pg_conn, create_rows)
                    applied = _apply_updates_postgres(
                        pg_conn, updates, force=args.force
                    )
                    pg_conn.commit()
                    if event_sink.enabled:
                        changed_tickers = sorted(
                            {
                                str(update["ticker"])
                                for update in updates
                                if isinstance(update.get("ticker"), str)
                            }
                        )
                        rows_for_events = _load_contract_rows_for_events_postgres(
                            pg_conn, changed_tickers
                        )
                        event_publish_failures = await _publish_contract_updates(
                            event_sink=event_sink,
                            rows_by_ticker=rows_for_events,
                        )
                else:
                    applied = 0
        else:
            async with aiosqlite.connect(settings.db_path) as conn:
                await conn.execute("PRAGMA foreign_keys = ON;")
                await conn.execute("PRAGMA journal_mode = WAL;")
                await conn.execute("PRAGMA synchronous = NORMAL;")
                await conn.execute("PRAGMA busy_timeout = 15000;")
                await conn.commit()

                existing = await _load_existing_contracts(conn, tickers)
                updates, create_rows, counters = build_settlement_updates(
                    markets, existing, now_ts, since_ts, logger, args.force
                )

                dao = Dao(conn)
                if not args.dry_run and create_rows:
                    for idx, row in enumerate(create_rows, start=1):
                        await dao.upsert_kalshi_contract(row)
                        if idx % 100 == 0:
                            await conn.commit()
                    await conn.commit()

                applied = await _apply_updates(
                    conn, updates, now_ts, args.dry_run, args.force
                )
                if not args.dry_run and applied:
                    await conn.commit()

                if not args.dry_run and event_sink.enabled:
                    changed_tickers = sorted(
                        {
                            str(update["ticker"])
                            for update in updates
                            if isinstance(update.get("ticker"), str)
                        }
                    )
                    rows_for_events = await _load_contract_rows_for_events(
                        conn, changed_tickers
                    )
                    event_publish_failures = await _publish_contract_updates(
                        event_sink=event_sink,
                        rows_by_ticker=rows_for_events,
                    )
    finally:
        await event_sink.close()

    if args.dry_run:
        failures = counters["failures"]
        successes = len(updates)
    else:
        failures = counters["failures"] + (len(updates) - applied)
        successes = applied

    print(
        "Settlement refresh summary: successes={successes} failures={failures} "
        "updated_outcomes_count={updated_outcomes_count} "
        "already_had_outcome_count={already_had_outcome_count} "
        "created_contracts={created_contracts} "
        "conflict_outcome_count={conflict_outcome_count}".format(
            successes=successes,
            failures=failures,
            updated_outcomes_count=counters["updated_outcomes_count"],
            already_had_outcome_count=counters["already_had_outcome_count"],
            created_contracts=counters["created_contracts"],
            conflict_outcome_count=counters["conflict_outcome_count"],
        )
    )
    print(
        "Settlement refresh counters: markets_fetched_total={markets_fetched_total} "
        "markets_with_ticker={markets_with_ticker} "
        "markets_after_since_filter={markets_after_since_filter} "
        "contracts_missing_before_create={contracts_missing_before_create} "
        "updates_skipped_no_change={updates_skipped_no_change} "
        "updates_built_total={updates_built_total} "
        "updates_with_outcome={updates_with_outcome} "
        "updates_without_outcome={updates_without_outcome} "
        "applied_total={applied_total}".format(
            markets_fetched_total=markets_fetched_total,
            markets_with_ticker=markets_with_ticker,
            markets_after_since_filter=counters["markets_after_since_filter"],
            contracts_missing_before_create=counters[
                "contracts_missing_before_create"
            ],
            updates_skipped_no_change=counters["updates_skipped_no_change"],
            updates_built_total=counters["updates_built_total"],
            updates_with_outcome=counters["updates_with_outcome"],
            updates_without_outcome=counters["updates_without_outcome"],
            applied_total=applied,
        )
    )
    print(f"Settlement refresh event_publish_failures={event_publish_failures}")
    if args.debug and markets:
        sample = next((m for m in markets if isinstance(m, dict)), None)
        if sample:
            keys = sorted(sample.keys())
            print(
                "sample_market keys={keys} ticker={ticker} status={status} "
                "result={result} settlement_value={settlement_value} "
                "settlement_ts={settlement_ts} settled_time={settled_time} "
                "settlement_time={settlement_time} resolved_time={resolved_time} "
                "result_time={result_time} close_time={close_time} "
                "expected_expiration_time={expected_expiration_time} expiration_time={expiration_time}".format(
                    keys=keys,
                    ticker=sample.get("ticker") or sample.get("market_id"),
                    status=sample.get("status"),
                    result=sample.get("result"),
                    settlement_value=sample.get("settlement_value"),
                    settlement_ts=sample.get("settlement_ts"),
                    settled_time=sample.get("settled_time"),
                    settlement_time=sample.get("settlement_time"),
                    resolved_time=sample.get("resolved_time"),
                    result_time=sample.get("result_time"),
                    close_time=sample.get("close_time"),
                    expected_expiration_time=sample.get("expected_expiration_time"),
                    expiration_time=sample.get("expiration_time"),
                )
            )
    if args.debug and updates:
        print(f"sample_update={updates[0]}")
    lock_file.close()
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
