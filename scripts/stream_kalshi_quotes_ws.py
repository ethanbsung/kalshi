from __future__ import annotations

import argparse
import asyncio
import sqlite3
import time
from typing import Any

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.data.dao import Dao
from kalshi_bot.data.db import init_db
from kalshi_bot.events import EventPublisher, QuoteUpdateEvent
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.kalshi import KalshiRestClient, KalshiWsClient
from kalshi_bot.kalshi.btc_markets import (
    BTC_SERIES_TICKERS,
    extract_close_ts,
    extract_expected_expiration_ts,
    extract_expiration_ts,
    fetch_btc_markets,
)
from kalshi_bot.kalshi.market_filters import (
    fetch_status_counts,
    no_markets_message,
    normalize_db_status,
    normalize_series,
)
from kalshi_bot.kalshi.quotes import (
    build_quote_row_from_topbook,
    load_market_tickers,
)
from kalshi_bot.kalshi.validation import validate_quote_row


def _matches_series(market_id: str, series: list[str]) -> bool:
    if not series:
        return True
    for value in series:
        if market_id == value or market_id.startswith(f"{value}-"):
            return True
    return False


def _select_tickers_from_markets(
    markets: list[dict[str, Any]],
    *,
    status: str | None,
    series: list[str],
    now_ts: int,
    max_horizon_seconds: int | None,
    grace_seconds: int = 3600,
) -> list[str]:
    requested_status = str(status).strip().lower() if status is not None else None
    if requested_status == "active":
        requested_status = "open"

    selected: list[tuple[int, str]] = []
    horizon_cap: int | None = None
    if max_horizon_seconds is not None and max_horizon_seconds > 0:
        horizon_cap = now_ts + int(max_horizon_seconds) + max(int(grace_seconds), 0)

    for market in markets:
        market_id_raw = market.get("ticker") or market.get("market_id")
        if not isinstance(market_id_raw, str) or not market_id_raw:
            continue
        market_id = market_id_raw

        if requested_status is not None:
            market_status = str(market.get("status") or "").strip().lower()
            if market_status == "active":
                market_status = "open"
            if market_status != requested_status:
                continue

        if not _matches_series(market_id, series):
            continue

        close_ts = (
            extract_close_ts(market)
            or extract_expected_expiration_ts(market)
            or extract_expiration_ts(market)
        )
        if close_ts is None:
            continue
        if close_ts < (now_ts - 5):
            continue
        if horizon_cap is not None and close_ts > horizon_cap:
            continue

        selected.append((close_ts, market_id))

    selected.sort(key=lambda item: (item[0], item[1]))
    return [market_id for _, market_id in selected]


async def _load_market_tickers_from_rest(
    *,
    rest_client: KalshiRestClient,
    status: str | None,
    series: list[str],
    max_horizon_seconds: int | None,
    logger: Any,
) -> list[str]:
    markets, _ = await fetch_btc_markets(
        rest_client,
        status=status,
        limit=None,
        logger=logger,
    )
    return _select_tickers_from_markets(
        markets,
        status=status,
        series=series,
        now_ts=int(time.time()),
        max_horizon_seconds=max_horizon_seconds,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream Kalshi quote snapshots via WS")
    parser.add_argument("--seconds", type=int, default=900)
    parser.add_argument("--status", type=str, default=None)
    parser.add_argument("--max-horizon-seconds", type=int, default=6 * 3600)
    parser.add_argument(
        "--series",
        action="append",
        default=None,
        help="Repeatable series ticker filter (defaults to BTC series)",
    )
    parser.add_argument(
        "--min-write-seconds",
        type=int,
        default=10,
        help="Minimum seconds between writes per market_id.",
    )
    parser.add_argument(
        "--commit-every-rows",
        type=int,
        default=50,
        help="Commit after this many inserts.",
    )
    parser.add_argument(
        "--commit-every-seconds",
        type=float,
        default=1.0,
        help="Commit at least this often while receiving messages.",
    )
    parser.add_argument(
        "--events-jsonl-path",
        type=str,
        default=None,
        help="Optional JSONL file path for shadow event publishing.",
    )
    parser.add_argument(
        "--events-bus",
        action="store_true",
        help="Publish quote_update events to JetStream.",
    )
    parser.add_argument(
        "--events-bus-url",
        type=str,
        default=None,
        help="JetStream bus URL override (default BUS_URL).",
    )
    parser.add_argument(
        "--publish-only",
        action="store_true",
        help="Publish quote_update events only and skip SQLite quote inserts.",
    )
    return parser.parse_args()


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()
    logger = setup_logger(settings.log_path)

    series = (
        normalize_series(args.series)
        if args.series is not None
        else list(BTC_SERIES_TICKERS)
    )
    status = normalize_db_status(args.status)
    min_write_seconds = max(int(args.min_write_seconds), 0)
    commit_every_rows = max(int(args.commit_every_rows), 1)
    commit_every_seconds = max(float(args.commit_every_seconds), 0.1)
    no_markets_retry_seconds = 10.0
    no_markets_log_every_seconds = 60.0
    run_window_seconds = max(int(args.seconds), 1)
    end_time = time.monotonic() + run_window_seconds

    if args.publish_only:
        rest_client = KalshiRestClient(
            base_url=settings.kalshi_rest_url,
            api_key_id=settings.kalshi_api_key_id,
            private_key_path=settings.kalshi_private_key_path,
            logger=logger,
        )

        tickers: list[str] = []
        last_no_market_log = 0.0
        zero_market_wait_seconds = 0.0
        zero_market_retries = 0
        while True:
            tickers = await _load_market_tickers_from_rest(
                rest_client=rest_client,
                status=status,
                series=series,
                max_horizon_seconds=args.max_horizon_seconds,
                logger=logger,
            )
            if tickers:
                break
            zero_market_retries += 1
            now_mono = time.monotonic()
            if (now_mono - last_no_market_log) >= no_markets_log_every_seconds:
                logger.warning(
                    "kalshi_ws_quotes_no_markets_retrying_publish_only",
                    extra={
                        "status": status,
                        "series": series,
                        "retry_seconds": no_markets_retry_seconds,
                        "max_horizon_seconds": args.max_horizon_seconds,
                    },
                )
                print(
                    "WARNING: No markets matched REST filter "
                    f"status={status} series={series}; retrying in {int(no_markets_retry_seconds)}s"
                )
                last_no_market_log = now_mono
            sleep_for = no_markets_retry_seconds
            await asyncio.sleep(sleep_for)
            zero_market_wait_seconds += sleep_for
            if time.monotonic() >= end_time:
                end_time = time.monotonic() + run_window_seconds

        if zero_market_retries > 0:
            logger.info(
                "kalshi_ws_quotes_zero_market_wait_publish_only",
                extra={
                    "wait_seconds": int(zero_market_wait_seconds),
                    "retries": zero_market_retries,
                    "market_count": len(tickers),
                    "status": status,
                    "series": series,
                    "max_horizon_seconds": args.max_horizon_seconds,
                },
            )
            print(
                "WS zero-market wait: wait_s={wait_s} retries={retries} markets={markets}".format(
                    wait_s=int(zero_market_wait_seconds),
                    retries=zero_market_retries,
                    markets=len(tickers),
                )
            )
            end_time = time.monotonic() + run_window_seconds

        client = KalshiWsClient(
            ws_url=settings.kalshi_ws_url,
            api_key_id=settings.kalshi_api_key_id,
            private_key_path=(
                str(settings.kalshi_private_key_path)
                if settings.kalshi_private_key_path
                else None
            ),
            auth_mode=settings.kalshi_ws_auth_mode,
            auth_query_key=settings.kalshi_ws_auth_query_key,
            auth_query_signature=settings.kalshi_ws_auth_query_signature,
            auth_query_timestamp=settings.kalshi_ws_auth_query_timestamp,
            market_tickers=tickers,
            logger=logger,
            channels=["ticker"],
        )

        inserted = 0
        invalid = 0
        write_errors = 0
        throttled = 0
        last_throttle_log = 0.0
        last_written_ts_by_market: dict[str, int] = {}
        bus_url = (
            args.events_bus_url
            if args.events_bus_url
            else (settings.bus_url if args.events_bus else None)
        )
        event_sink = await EventPublisher.create(
            jsonl_path=args.events_jsonl_path,
            bus_url=bus_url,
        )
        if not event_sink.enabled:
            await event_sink.close()
            raise RuntimeError(
                "quotes publish-only mode requires an event sink "
                "(--events-jsonl-path and/or --events-bus)."
            )
        event_publish_failures = 0

        async def on_ticker(row: dict[str, Any]) -> None:
            nonlocal inserted, invalid, throttled, event_publish_failures
            nonlocal last_throttle_log
            market_id = str(row.get("market_id") or "")
            if not market_id:
                invalid += 1
                return

            ts = int(row.get("ts") or time.time())
            if min_write_seconds > 0:
                prev_ts = last_written_ts_by_market.get(market_id)
                if prev_ts is not None and (ts - prev_ts) < min_write_seconds:
                    throttled += 1
                    now_mono = time.monotonic()
                    if (now_mono - last_throttle_log) >= 60.0:
                        logger.info(
                            "kalshi_ws_quotes_throttled_publish_only",
                            extra={
                                "throttled_total": throttled,
                                "min_write_seconds": min_write_seconds,
                            },
                        )
                        last_throttle_log = now_mono
                    return

            quote_row = build_quote_row_from_topbook(
                market_id=market_id,
                ts=ts,
                yes_bid=row.get("best_yes_bid"),
                yes_ask=row.get("best_yes_ask"),
                no_bid=row.get("best_no_bid"),
                no_ask=row.get("best_no_ask"),
                volume=row.get("volume"),
                open_interest=row.get("open_interest"),
                raw_json=str(row.get("raw_json") or "{}"),
            )
            is_valid, _ = validate_quote_row(quote_row)
            if not is_valid:
                invalid += 1
                return

            inserted += 1
            last_written_ts_by_market[market_id] = ts
            try:
                await event_sink.publish(
                    QuoteUpdateEvent(
                        source="stream_kalshi_quotes_ws",
                        payload={
                            "ts": int(quote_row["ts"]),
                            "market_id": str(quote_row["market_id"]),
                            "source_msg_id": (
                                str(row.get("seq"))
                                if row.get("seq") is not None
                                else None
                            ),
                            "yes_bid": (
                                float(quote_row["yes_bid"])
                                if quote_row["yes_bid"] is not None
                                else None
                            ),
                            "yes_ask": (
                                float(quote_row["yes_ask"])
                                if quote_row["yes_ask"] is not None
                                else None
                            ),
                            "no_bid": (
                                float(quote_row["no_bid"])
                                if quote_row["no_bid"] is not None
                                else None
                            ),
                            "no_ask": (
                                float(quote_row["no_ask"])
                                if quote_row["no_ask"] is not None
                                else None
                            ),
                            "yes_mid": (
                                float(quote_row["yes_mid"])
                                if quote_row["yes_mid"] is not None
                                else None
                            ),
                            "no_mid": (
                                float(quote_row["no_mid"])
                                if quote_row["no_mid"] is not None
                                else None
                            ),
                            "p_mid": (
                                float(quote_row["p_mid"])
                                if quote_row["p_mid"] is not None
                                else None
                            ),
                        },
                    )
                )
            except Exception:
                event_publish_failures += 1

        async def on_snapshot(_: dict[str, Any]) -> None:
            return

        async def on_delta(_: dict[str, Any]) -> None:
            return

        try:
            run_seconds = max(int(end_time - time.monotonic()), 1)
            await client.run(
                on_ticker,
                on_snapshot,
                on_delta,
                run_seconds=run_seconds,
            )
        finally:
            await event_sink.close()

        summary = {
            "inserted": inserted,
            "invalid": invalid,
            "write_errors": write_errors,
            "throttled": throttled,
            "market_count": len(tickers),
            "event_publish_failures": event_publish_failures,
            "publish_only": True,
        }
        if summary["inserted"] == 0 and summary["market_count"] > 0:
            logger.warning("kalshi_ws_quotes_summary", extra=summary)
            print(
                "WARNING: WS quote summary: markets={market_count} inserted={inserted} "
                "invalid={invalid} write_errors={write_errors} throttled={throttled} "
                "event_publish_failures={event_publish_failures} "
                "publish_only={publish_only}".format(
                    **summary
                )
            )
        else:
            logger.info("kalshi_ws_quotes_summary", extra=summary)
            print(
                "WS quote summary: markets={market_count} inserted={inserted} "
                "invalid={invalid} write_errors={write_errors} throttled={throttled} "
                "event_publish_failures={event_publish_failures} "
                "publish_only={publish_only}".format(
                    **summary
                )
            )
        return 0

    print(f"DB path: {settings.db_path}")
    await init_db(settings.db_path)

    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 15000;")
        await conn.commit()

        tickers: list[str] = []
        last_no_market_log = 0.0
        zero_market_wait_seconds = 0.0
        zero_market_retries = 0
        while True:
            now_ts = int(time.time())
            tickers = await load_market_tickers(
                conn,
                status,
                series,
                now_ts=now_ts,
                max_horizon_seconds=args.max_horizon_seconds,
            )
            if tickers:
                break
            zero_market_retries += 1
            now_mono = time.monotonic()
            if (now_mono - last_no_market_log) >= no_markets_log_every_seconds:
                counts = await fetch_status_counts(conn)
                message = no_markets_message(status, counts)
                logger.warning(
                    "kalshi_ws_quotes_no_markets_retrying",
                    extra={
                        "status": status,
                        "series": series,
                        "status_counts": counts,
                        "retry_seconds": no_markets_retry_seconds,
                        "max_horizon_seconds": args.max_horizon_seconds,
                    },
                )
                print(f"WARNING: {message}; retrying in {int(no_markets_retry_seconds)}s")
                last_no_market_log = now_mono
            sleep_for = no_markets_retry_seconds
            await asyncio.sleep(sleep_for)
            zero_market_wait_seconds += sleep_for
            if time.monotonic() >= end_time:
                # Do not exit on empty windows; keep waiting until markets appear.
                end_time = time.monotonic() + run_window_seconds

        if zero_market_retries > 0:
            logger.info(
                "kalshi_ws_quotes_zero_market_wait",
                extra={
                    "wait_seconds": int(zero_market_wait_seconds),
                    "retries": zero_market_retries,
                    "market_count": len(tickers),
                    "status": status,
                    "series": series,
                    "max_horizon_seconds": args.max_horizon_seconds,
                },
            )
            print(
                "WS zero-market wait: wait_s={wait_s} retries={retries} markets={markets}".format(
                    wait_s=int(zero_market_wait_seconds),
                    retries=zero_market_retries,
                    markets=len(tickers),
                )
            )
            # Reset the run window so we get a full stream interval after markets appear.
            end_time = time.monotonic() + run_window_seconds

        client = KalshiWsClient(
            ws_url=settings.kalshi_ws_url,
            api_key_id=settings.kalshi_api_key_id,
            private_key_path=(
                str(settings.kalshi_private_key_path)
                if settings.kalshi_private_key_path
                else None
            ),
            auth_mode=settings.kalshi_ws_auth_mode,
            auth_query_key=settings.kalshi_ws_auth_query_key,
            auth_query_signature=settings.kalshi_ws_auth_query_signature,
            auth_query_timestamp=settings.kalshi_ws_auth_query_timestamp,
            market_tickers=tickers,
            logger=logger,
            channels=["ticker"],
        )

        dao = Dao(conn)

        rows_since_commit = 0
        last_commit = time.monotonic()
        inserted = 0
        invalid = 0
        write_errors = 0
        throttled = 0
        last_throttle_log = 0.0
        last_write_error_log = 0.0
        last_written_ts_by_market: dict[str, int] = {}
        bus_url = (
            args.events_bus_url
            if args.events_bus_url
            else (settings.bus_url if args.events_bus else None)
        )
        event_sink = await EventPublisher.create(
            jsonl_path=args.events_jsonl_path,
            bus_url=bus_url,
        )
        if args.publish_only and not event_sink.enabled:
            await event_sink.close()
            raise RuntimeError(
                "quotes publish-only mode requires an event sink "
                "(--events-jsonl-path and/or --events-bus)."
            )
        event_publish_failures = 0

        async def _maybe_commit(force: bool = False) -> None:
            nonlocal rows_since_commit, last_commit
            if rows_since_commit <= 0 and not force:
                return
            now = time.monotonic()
            if not force:
                if (
                    rows_since_commit < commit_every_rows
                    and (now - last_commit) < commit_every_seconds
                ):
                    return
            await conn.commit()
            rows_since_commit = 0
            last_commit = now

        async def on_ticker(row: dict[str, Any]) -> None:
            nonlocal rows_since_commit, inserted, invalid, write_errors, throttled
            nonlocal last_throttle_log, last_write_error_log
            nonlocal event_publish_failures
            market_id = str(row.get("market_id") or "")
            if not market_id:
                invalid += 1
                return

            ts = int(row.get("ts") or time.time())
            if min_write_seconds > 0:
                prev_ts = last_written_ts_by_market.get(market_id)
                if prev_ts is not None and (ts - prev_ts) < min_write_seconds:
                    throttled += 1
                    now_mono = time.monotonic()
                    if (now_mono - last_throttle_log) >= 60.0:
                        logger.info(
                            "kalshi_ws_quotes_throttled",
                            extra={
                                "throttled_total": throttled,
                                "min_write_seconds": min_write_seconds,
                            },
                        )
                        last_throttle_log = now_mono
                    return

            quote_row = build_quote_row_from_topbook(
                market_id=market_id,
                ts=ts,
                yes_bid=row.get("best_yes_bid"),
                yes_ask=row.get("best_yes_ask"),
                no_bid=row.get("best_no_bid"),
                no_ask=row.get("best_no_ask"),
                volume=row.get("volume"),
                open_interest=row.get("open_interest"),
                raw_json=str(row.get("raw_json") or "{}"),
            )
            is_valid, _ = validate_quote_row(quote_row)
            if not is_valid:
                invalid += 1
                return

            try:
                if not args.publish_only:
                    await dao.insert_kalshi_quote(quote_row)
            except sqlite3.OperationalError as exc:
                write_errors += 1
                now_mono = time.monotonic()
                if (now_mono - last_write_error_log) >= 60.0:
                    logger.warning(
                        "kalshi_ws_quotes_insert_locked",
                        extra={
                            "write_errors_total": write_errors,
                            "error": str(exc),
                        },
                    )
                    last_write_error_log = now_mono
                return

            inserted += 1
            if not args.publish_only:
                rows_since_commit += 1
            last_written_ts_by_market[market_id] = ts
            if event_sink.enabled:
                try:
                    await event_sink.publish(
                        QuoteUpdateEvent(
                            source="stream_kalshi_quotes_ws",
                            payload={
                                "ts": int(quote_row["ts"]),
                                "market_id": str(quote_row["market_id"]),
                                "source_msg_id": (
                                    str(row.get("seq"))
                                    if row.get("seq") is not None
                                    else None
                                ),
                                "yes_bid": (
                                    float(quote_row["yes_bid"])
                                    if quote_row["yes_bid"] is not None
                                    else None
                                ),
                                "yes_ask": (
                                    float(quote_row["yes_ask"])
                                    if quote_row["yes_ask"] is not None
                                    else None
                                ),
                                "no_bid": (
                                    float(quote_row["no_bid"])
                                    if quote_row["no_bid"] is not None
                                    else None
                                ),
                                "no_ask": (
                                    float(quote_row["no_ask"])
                                    if quote_row["no_ask"] is not None
                                    else None
                                ),
                                "yes_mid": (
                                    float(quote_row["yes_mid"])
                                    if quote_row["yes_mid"] is not None
                                    else None
                                ),
                                "no_mid": (
                                    float(quote_row["no_mid"])
                                    if quote_row["no_mid"] is not None
                                    else None
                                ),
                                "p_mid": (
                                    float(quote_row["p_mid"])
                                    if quote_row["p_mid"] is not None
                                    else None
                                ),
                            },
                        )
                    )
                except Exception:
                    event_publish_failures += 1
            await _maybe_commit(force=False)

        async def on_snapshot(_: dict[str, Any]) -> None:
            return

        async def on_delta(_: dict[str, Any]) -> None:
            return

        try:
            try:
                run_seconds = max(int(end_time - time.monotonic()), 1)
                await client.run(
                    on_ticker,
                    on_snapshot,
                    on_delta,
                    run_seconds=run_seconds,
                )
            finally:
                await _maybe_commit(force=True)
        finally:
            await event_sink.close()

        summary = {
            "inserted": inserted,
            "invalid": invalid,
            "write_errors": write_errors,
            "throttled": throttled,
            "market_count": len(tickers),
            "event_publish_failures": event_publish_failures,
            "publish_only": bool(args.publish_only),
        }
        if summary["inserted"] == 0 and summary["market_count"] > 0:
            logger.warning("kalshi_ws_quotes_summary", extra=summary)
            print(
                "WARNING: WS quote summary: markets={market_count} inserted={inserted} "
                "invalid={invalid} write_errors={write_errors} throttled={throttled} "
                "event_publish_failures={event_publish_failures} "
                "publish_only={publish_only}".format(
                    **summary
                )
            )
        elif summary["write_errors"] > 0:
            logger.warning("kalshi_ws_quotes_summary", extra=summary)
            print(
                "WARNING: WS quote summary: markets={market_count} inserted={inserted} "
                "invalid={invalid} write_errors={write_errors} throttled={throttled} "
                "event_publish_failures={event_publish_failures} "
                "publish_only={publish_only}".format(
                    **summary
                )
            )
        else:
            logger.info("kalshi_ws_quotes_summary", extra=summary)
            print(
                "WS quote summary: markets={market_count} inserted={inserted} "
                "invalid={invalid} write_errors={write_errors} throttled={throttled} "
                "event_publish_failures={event_publish_failures} "
                "publish_only={publish_only}".format(
                    **summary
                )
            )
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
