from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import time
from typing import Any

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.data import init_db
from kalshi_bot.data.dao import Dao
from kalshi_bot.events import (
    EdgeSnapshotEvent,
    EdgeSnapshotPayload,
    EventPublisher,
    connect_jetstream,
    parse_event_dict,
)
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.kalshi.btc_markets import BTC_SERIES_TICKERS
from kalshi_bot.kalshi.market_filters import normalize_db_status, normalize_series
from kalshi_bot.strategy.edge_engine import compute_edges
from kalshi_bot.strategy.edge_state_engine import (
    SigmaMemory,
    compute_edges_from_live_state,
)
from kalshi_bot.state import LiveMarketState

EDGE_SUMMARY_HEARTBEAT_SECONDS = 60
NO_RELEVANT_HEARTBEAT_SECONDS = 15 * 60


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continuously compute edges and write snapshots."
    )
    parser.add_argument("--interval-seconds", type=int, default=10)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--product-id", type=str, default="BTC-USD")
    parser.add_argument("--lookback-seconds", type=int, default=3900)
    parser.add_argument("--max-spot-points", type=int, default=20000)
    parser.add_argument("--ewma-lambda", type=float, default=0.94)
    parser.add_argument("--min-points", type=int, default=10)
    parser.add_argument("--min-sigma-lookback-seconds", type=int, default=3600)
    parser.add_argument("--sigma-resample-seconds", type=int, default=5)
    parser.add_argument("--sigma-default", type=float, default=0.6)
    parser.add_argument("--sigma-max", type=float, default=5.0)
    parser.add_argument("--status", type=str, default="active")
    parser.add_argument("--series", action="append", default=None)
    parser.add_argument("--pct-band", type=float, default=3.0)
    parser.add_argument("--top-n", type=int, default=40)
    parser.add_argument("--freshness-seconds", type=int, default=300)
    parser.add_argument("--max-horizon-seconds", type=int, default=6 * 3600)
    parser.add_argument("--min-ask-cents", type=float, default=1.0)
    parser.add_argument("--max-ask-cents", type=float, default=99.0)
    parser.add_argument("--contracts", type=int, default=1)
    parser.add_argument("--debug-market", action="append", default=[])
    parser.add_argument("--debug", action="store_true")
    parser.add_argument(
        "--events-jsonl-path",
        type=str,
        default=None,
        help="Optional JSONL file path for shadow event publishing.",
    )
    parser.add_argument(
        "--events-bus",
        action="store_true",
        help="Publish edge_snapshot events to JetStream.",
    )
    parser.add_argument(
        "--events-bus-url",
        type=str,
        default=None,
        help="JetStream bus URL override (default BUS_URL).",
    )
    parser.add_argument(
        "--state-source",
        choices=["sqlite", "events"],
        default="sqlite",
        help="Use SQLite tables (legacy) or market events stream (Phase C).",
    )
    parser.add_argument(
        "--events-stream",
        type=str,
        default="MARKET_EVENTS",
        help="JetStream stream name for market events (events mode only).",
    )
    parser.add_argument(
        "--events-subject",
        type=str,
        default="market.>",
        help="JetStream subject filter for market events (events mode only).",
    )
    parser.add_argument(
        "--events-consumer-durable",
        type=str,
        default=None,
        help=(
            "Optional durable consumer name for market-event feed. "
            "Unset by default so startup replays stream state."
        ),
    )
    parser.add_argument(
        "--events-fetch-batch",
        type=int,
        default=500,
        help="Max messages fetched per pull in events mode.",
    )
    parser.add_argument(
        "--events-fetch-timeout-seconds",
        type=float,
        default=0.25,
        help="Pull timeout for each market-event fetch in events mode.",
    )
    parser.add_argument(
        "--show-titles",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    return parser.parse_args()


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


async def _load_edges(
    conn: aiosqlite.Connection, asof_ts: int
) -> list[dict[str, Any]]:
    cursor = await conn.execute(
        "SELECT market_id, settlement_ts, horizon_seconds, spot_price, "
        "sigma_annualized, prob_yes, ev_take_yes, ev_take_no, raw_json "
        "FROM kalshi_edges WHERE ts = ?",
        (asof_ts,),
    )
    rows = await cursor.fetchall()
    edges: list[dict[str, Any]] = []
    for (
        market_id,
        settlement_ts,
        horizon_seconds,
        spot_price,
        sigma_annualized,
        prob_yes,
        ev_take_yes,
        ev_take_no,
        raw_json,
    ) in rows:
        edges.append(
            {
                "market_id": market_id,
                "settlement_ts": settlement_ts,
                "horizon_seconds": horizon_seconds,
                "spot_price": spot_price,
                "sigma_annualized": sigma_annualized,
                "prob_yes": prob_yes,
                "ev_take_yes": ev_take_yes,
                "ev_take_no": ev_take_no,
                "raw_json": raw_json,
            }
        )
    return edges


async def _load_latest_quotes(
    conn: aiosqlite.Connection, market_ids: list[str], asof_ts: int
) -> dict[str, dict[str, Any]]:
    if not market_ids:
        return {}
    placeholders = ",".join("?" for _ in market_ids)
    sql = f"""
        WITH latest AS (
            SELECT market_id, MAX(ts) AS max_ts
            FROM kalshi_quotes
            WHERE ts <= ? AND market_id IN ({placeholders})
            GROUP BY market_id
        )
        SELECT q.market_id, q.ts, q.yes_bid, q.yes_ask, q.no_bid, q.no_ask,
               q.yes_mid, q.no_mid
        FROM kalshi_quotes q
        JOIN latest l ON q.market_id = l.market_id AND q.ts = l.max_ts
        """
    # nosec B608: placeholders are generated from the market id list length.
    cursor = await conn.execute(
        sql,
        [asof_ts, *market_ids],
    )
    rows = await cursor.fetchall()
    quotes: dict[str, dict[str, Any]] = {}
    for (
        market_id,
        ts,
        yes_bid,
        yes_ask,
        no_bid,
        no_ask,
        yes_mid,
        no_mid,
    ) in rows:
        quotes[market_id] = {
            "quote_ts": ts,
            "yes_bid": _safe_float(yes_bid),
            "yes_ask": _safe_float(yes_ask),
            "no_bid": _safe_float(no_bid),
            "no_ask": _safe_float(no_ask),
            "yes_mid": _safe_float(yes_mid),
            "no_mid": _safe_float(no_mid),
        }
    return quotes


def _compute_mid(bid: float | None, ask: float | None) -> float | None:
    if bid is None or ask is None:
        return None
    return (bid + ask) / 2.0


def _parse_edge_metadata(raw_json: str | None) -> dict[str, Any]:
    if not raw_json:
        return {}
    try:
        payload = json.loads(raw_json)
        if isinstance(payload, dict):
            return payload
    except json.JSONDecodeError:
        return {}
    return {}


_MARKET_EVENT_TYPES = {
    "spot_tick",
    "quote_update",
    "market_lifecycle",
    "contract_update",
}


def _build_edge_event_payload(row: dict[str, Any]) -> dict[str, Any] | None:
    if (
        row.get("asof_ts") is None
        or row.get("market_id") is None
        or row.get("prob_yes") is None
        or row.get("ev_take_yes") is None
        or row.get("ev_take_no") is None
        or row.get("sigma_annualized") is None
        or row.get("spot_price") is None
    ):
        return None
    return {
        "asof_ts": int(row["asof_ts"]),
        "market_id": str(row["market_id"]),
        "prob_yes": float(row["prob_yes"]),
        "ev_take_yes": float(row["ev_take_yes"]),
        "ev_take_no": float(row["ev_take_no"]),
        "sigma_annualized": float(row["sigma_annualized"]),
        "spot_price": float(row["spot_price"]),
        "quote_ts": int(row["quote_ts"]) if row.get("quote_ts") is not None else None,
        "spot_ts": int(row["spot_ts"]) if row.get("spot_ts") is not None else None,
        "settlement_ts": (
            int(row["settlement_ts"]) if row.get("settlement_ts") is not None else None
        ),
        "horizon_seconds": (
            int(row["horizon_seconds"]) if row.get("horizon_seconds") is not None else None
        ),
        "strike": str(row["strike"]) if row.get("strike") is not None else None,
        "prob_yes_raw": (
            float(row["prob_yes_raw"]) if row.get("prob_yes_raw") is not None else None
        ),
        "yes_bid": float(row["yes_bid"]) if row.get("yes_bid") is not None else None,
        "yes_ask": float(row["yes_ask"]) if row.get("yes_ask") is not None else None,
        "no_bid": float(row["no_bid"]) if row.get("no_bid") is not None else None,
        "no_ask": float(row["no_ask"]) if row.get("no_ask") is not None else None,
        "yes_mid": float(row["yes_mid"]) if row.get("yes_mid") is not None else None,
        "no_mid": float(row["no_mid"]) if row.get("no_mid") is not None else None,
        "spot_age_seconds": (
            int(row["spot_age_seconds"])
            if row.get("spot_age_seconds") is not None
            else None
        ),
        "quote_age_seconds": (
            int(row["quote_age_seconds"])
            if row.get("quote_age_seconds") is not None
            else None
        ),
        "raw_json": str(row["raw_json"]) if row.get("raw_json") is not None else None,
    }


async def _publish_edge_events(
    *,
    rows: list[dict[str, Any]],
    event_sink: EventPublisher | None,
) -> int:
    if event_sink is None or not event_sink.enabled:
        return 0
    failures = 0
    for row in rows:
        payload = _build_edge_event_payload(row)
        if payload is None:
            continue
        try:
            await event_sink.publish(
                EdgeSnapshotEvent(
                    source="run_live_edges",
                    payload=EdgeSnapshotPayload.model_validate(payload),
                )
            )
        except Exception:
            failures += 1
    return failures


async def _consume_market_events(
    *,
    subscription: Any,
    state: LiveMarketState,
    max_batch: int,
    timeout_seconds: float,
    max_rounds: int = 8,
) -> tuple[int, int]:
    applied = 0
    parse_errors = 0
    batch_size = max(max_batch, 1)
    fetch_timeout = max(timeout_seconds, 0.01)
    rounds = 0
    while rounds < max(max_rounds, 1):
        try:
            msgs = await subscription.fetch(batch=batch_size, timeout=fetch_timeout)
        except TimeoutError:
            break
        except Exception:
            raise
        rounds += 1

        if not msgs:
            break
        for msg in msgs:
            try:
                raw = json.loads(msg.data.decode("utf-8"))
                event = parse_event_dict(raw)
                if event.event_type in _MARKET_EVENT_TYPES:
                    state.apply_event(event)
                    applied += 1
            except Exception:
                parse_errors += 1
            finally:
                try:
                    await msg.ack()
                except Exception as ack_exc:
                    print(
                        "market_event_ack_error type={type_name} err={err}".format(
                            type_name=type(ack_exc).__name__,
                            err=ack_exc,
                        )
                    )
        if len(msgs) < batch_size:
            break
    return applied, parse_errors


async def _subscribe_market_events(js: Any, args: argparse.Namespace) -> Any:
    subscribe_kwargs: dict[str, Any] = {
        "subject": args.events_subject,
        "stream": args.events_stream,
    }
    durable = (
        args.events_consumer_durable.strip()
        if isinstance(args.events_consumer_durable, str)
        else None
    )
    if durable:
        subscribe_kwargs["durable"] = durable
        print(f"events_consumer_durable={durable}")
    else:
        print("events_consumer_durable=ephemeral (startup replay enabled)")
    return await js.pull_subscribe(**subscribe_kwargs)


def _consume_market_events_from_queue(
    *,
    queue: asyncio.Queue[bytes],
    state: LiveMarketState,
    max_batch: int,
) -> tuple[int, int]:
    applied = 0
    parse_errors = 0
    for _ in range(max(max_batch, 1)):
        try:
            data = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        try:
            raw = json.loads(data.decode("utf-8"))
            event = parse_event_dict(raw)
            if event.event_type in _MARKET_EVENT_TYPES:
                state.apply_event(event)
                applied += 1
        except Exception:
            parse_errors += 1
    return applied, parse_errors


async def run_tick(
    conn: aiosqlite.Connection,
    args: argparse.Namespace,
    event_sink: EventPublisher | None = None,
) -> dict[str, Any]:
    backoffs = [0.2, 0.5, 1.0]
    attempt = 0
    while True:
        try:
            summary = await compute_edges(
                conn,
                product_id=args.product_id,
                lookback_seconds=args.lookback_seconds,
                max_spot_points=args.max_spot_points,
                ewma_lambda=args.ewma_lambda,
                min_points=args.min_points,
                min_sigma_lookback_seconds=args.min_sigma_lookback_seconds,
                resample_seconds=args.sigma_resample_seconds,
                sigma_default=args.sigma_default,
                sigma_max=args.sigma_max,
                status=args.status,
                series=args.series,
                pct_band=args.pct_band,
                top_n=args.top_n,
                freshness_seconds=args.freshness_seconds,
                max_horizon_seconds=args.max_horizon_seconds,
                min_ask_cents=args.min_ask_cents,
                max_ask_cents=args.max_ask_cents,
                contracts=args.contracts,
                debug_market_ids=args.debug_market or None,
                now_ts=getattr(args, "now_ts", None),
            )
            if "error" in summary:
                return summary

            asof_ts = summary["now_ts"]
            edges = await _load_edges(conn, asof_ts)
            if not edges:
                summary["snapshots_inserted"] = 0
                summary["snapshots_total"] = 0
                return summary

            market_ids = [edge["market_id"] for edge in edges]
            quotes = await _load_latest_quotes(conn, market_ids, asof_ts)
            dao = Dao(conn)
            snapshot_rows: list[dict[str, Any]] = []
            max_spot_age: int | None = None
            max_quote_age: int | None = None
            for edge in edges:
                market_id = edge["market_id"]
                quote = quotes.get(market_id, {})
                edge_meta = _parse_edge_metadata(edge.get("raw_json"))
                prob_yes_raw = _safe_float(edge_meta.get("prob_yes_raw"))
                prob_yes_clamped = _safe_float(edge_meta.get("prob_yes_clamped"))
                if prob_yes_clamped is None:
                    prob_yes_clamped = _safe_float(edge.get("prob_yes"))
                quote_ts = quote.get("quote_ts")
                yes_bid = quote.get("yes_bid")
                yes_ask = quote.get("yes_ask")
                no_bid = quote.get("no_bid")
                no_ask = quote.get("no_ask")
                yes_mid = quote.get("yes_mid")
                no_mid = quote.get("no_mid")
                if yes_mid is None:
                    yes_mid = _compute_mid(yes_bid, yes_ask)
                if no_mid is None:
                    no_mid = _compute_mid(no_bid, no_ask)
                spot_ts = summary.get("spot_ts")
                spot_age_seconds = (
                    asof_ts - spot_ts if spot_ts is not None else None
                )
                quote_age_seconds = (
                    asof_ts - quote_ts if quote_ts is not None else None
                )
                if spot_age_seconds is not None:
                    max_spot_age = (
                        spot_age_seconds
                        if max_spot_age is None
                        else max(max_spot_age, spot_age_seconds)
                    )
                if quote_age_seconds is not None:
                    max_quote_age = (
                        quote_age_seconds
                        if max_quote_age is None
                        else max(max_quote_age, quote_age_seconds)
                    )
                snapshot_meta = dict(edge_meta)
                snapshot_meta.update(
                    {
                        "snapshot_version": 1,
                        # Preserve edge-time inputs so downstream invariant checks
                        # can compare apples-to-apples.
                        "edge_quote_ts": edge_meta.get("quote_ts"),
                        "edge_prob_yes": edge.get("prob_yes"),
                        "edge_yes_ask": edge.get("yes_ask"),
                        "edge_no_ask": edge.get("no_ask"),
                        "sigma_source": summary.get("sigma_source"),
                        "sigma_ok": summary.get("sigma_ok"),
                        "sigma_reason": summary.get("sigma_reason"),
                        "sigma_reason_context": summary.get("sigma_reason_context"),
                        "sigma_points_used": summary.get("sigma_points_used"),
                        "min_sigma_points": summary.get("min_sigma_points"),
                        "sigma_lookback_seconds_used": summary.get(
                            "sigma_lookback_seconds_used"
                        ),
                        "min_sigma_lookback_seconds": summary.get(
                            "min_sigma_lookback_seconds"
                        ),
                    }
                )
                snapshot_rows.append(
                    {
                        "asof_ts": asof_ts,
                        "market_id": market_id,
                        "settlement_ts": edge.get("settlement_ts"),
                        "spot_ts": spot_ts,
                        "spot_price": summary.get("spot_price"),
                        "sigma_annualized": edge.get("sigma_annualized"),
                        "prob_yes": prob_yes_clamped,
                        "prob_yes_raw": prob_yes_raw,
                        "horizon_seconds": edge.get("horizon_seconds"),
                        "quote_ts": quote_ts,
                        "yes_bid": yes_bid,
                        "yes_ask": yes_ask,
                        "no_bid": no_bid,
                        "no_ask": no_ask,
                        "yes_mid": yes_mid,
                        "no_mid": no_mid,
                        "ev_take_yes": edge.get("ev_take_yes"),
                        "ev_take_no": edge.get("ev_take_no"),
                        "spot_age_seconds": spot_age_seconds,
                        "quote_age_seconds": quote_age_seconds,
                        "skip_reason": None,
                        "raw_json": json.dumps(snapshot_meta),
                    }
                )
            await dao.insert_kalshi_edge_snapshots(snapshot_rows)
            await conn.commit()
            event_publish_failures = await _publish_edge_events(
                rows=snapshot_rows,
                event_sink=event_sink,
            )

            summary["snapshots_inserted"] = len(snapshot_rows)
            summary["snapshots_total"] = len(snapshot_rows)
            summary["max_spot_age_seconds"] = max_spot_age
            summary["max_quote_age_seconds"] = max_quote_age
            summary["event_publish_failures"] = event_publish_failures
            return summary
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "database is locked" in message:
                try:
                    await conn.rollback()
                except sqlite3.OperationalError:
                    pass
            if "database is locked" in message and attempt < len(backoffs):
                await asyncio.sleep(backoffs[attempt])
                attempt += 1
                continue
            raise


def _print_debug(summary: dict[str, Any], show_titles: bool) -> None:
    print(
        "sigma_source={sigma_source} sigma_ok={sigma_ok} "
        "sigma_quality={sigma_quality} sigma_reason={sigma_reason} "
        "sigma_reason_context={sigma_reason_context} "
        "sigma_points_used={sigma_points_used} "
        "sigma_lookback_seconds_used={sigma_lookback_seconds_used} "
        "resample_seconds={resample_seconds} step_seconds={step_seconds}".format(
            **summary
        )
    )
    print(f"sample_relevant_ids={summary.get('relevant_ids_sample')}")
    if show_titles:
        print(f"sample_relevant_titles={summary.get('relevant_titles_sample')}")
    selection = summary.get("selection", {})
    if selection:
        selection_counts = {
            "now_ts": selection.get("now_ts"),
            "candidate_count_total": selection.get("candidate_count_total"),
            "excluded_expired": selection.get("excluded_expired"),
            "excluded_horizon_out_of_range": selection.get(
                "excluded_horizon_out_of_range"
            ),
            "excluded_missing_bounds": selection.get("excluded_missing_bounds"),
            "excluded_missing_recent_quote": selection.get(
                "excluded_missing_recent_quote"
            ),
            "excluded_untradable": selection.get("excluded_untradable"),
            "selected_count": selection.get("selected_count"),
            "method": selection.get("method"),
        }
        print(f"selection_summary={selection_counts}")


def _log_tick_summary(
    *,
    summary: dict[str, Any],
    args: argparse.Namespace,
    last_edge_log_ts: int | None,
    last_no_relevant_log_ts: int | None,
    last_no_relevant_signature: tuple[tuple[str, int], ...] | None,
) -> tuple[int | None, int | None, tuple[tuple[str, int], ...] | None]:
    if "error" in summary:
        error = str(summary.get("error") or "unknown")
        now_ts = int(
            summary.get("now_ts")
            or (summary.get("selection") or {}).get("now_ts")
            or 0
        )
        if error == "no_relevant_markets":
            skip_reasons = summary.get("skip_reasons") or {}
            signature = tuple(sorted(skip_reasons.items()))
            should_log = False
            if last_no_relevant_log_ts is None:
                should_log = True
            elif signature != last_no_relevant_signature:
                should_log = True
            elif now_ts > 0 and (
                now_ts - last_no_relevant_log_ts
            ) >= NO_RELEVANT_HEARTBEAT_SECONDS:
                should_log = True
            if should_log:
                print(f"ERROR: {error}")
                selection = summary.get("selection", {})
                if selection:
                    print(f"selection_summary={selection}")
                if skip_reasons:
                    print(f"skip_reasons={skip_reasons}")
                last_no_relevant_log_ts = now_ts if now_ts > 0 else 0
                last_no_relevant_signature = signature
        else:
            print(f"ERROR: {error}")
            selection = summary.get("selection", {})
            if selection:
                print(f"selection_summary={selection}")
            if summary.get("skip_reasons"):
                print(f"skip_reasons={summary['skip_reasons']}")
        return (
            last_edge_log_ts,
            last_no_relevant_log_ts,
            last_no_relevant_signature,
        )

    now_ts = int(summary.get("now_ts") or 0)
    should_log_summary = (
        last_edge_log_ts is None
        or now_ts <= 0
        or (now_ts - last_edge_log_ts) >= EDGE_SUMMARY_HEARTBEAT_SECONDS
    )
    if should_log_summary:
        print(
            "asof_ts={now_ts} edges_inserted={edges_inserted} "
            "snapshots_inserted={snapshots_inserted} "
            "max_spot_age_seconds={max_spot_age_seconds} "
            "max_quote_age_seconds={max_quote_age_seconds} "
            "event_publish_failures={event_publish_failures}".format(**summary)
        )
        if summary.get("consumer_lag_pending") is not None:
            print(
                "consumer_lag_pending={consumer_lag_pending} "
                "events_applied_total={events_applied_total} "
                "event_parse_errors_total={event_parse_errors_total}".format(
                    **summary
                )
            )
        if now_ts > 0:
            last_edge_log_ts = now_ts
    if args.debug:
        _print_debug(summary, args.show_titles)
    return (
        last_edge_log_ts,
        last_no_relevant_log_ts,
        last_no_relevant_signature,
    )


async def _run_sqlite_mode(
    *,
    args: argparse.Namespace,
    settings: Any,
    event_sink: EventPublisher,
) -> int:
    print(f"DB path: {settings.db_path}")
    await init_db(settings.db_path)
    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 15000;")
        await conn.commit()

        last_edge_log_ts: int | None = None
        last_no_relevant_log_ts: int | None = None
        last_no_relevant_signature: tuple[tuple[str, int], ...] | None = None
        while True:
            summary = await run_tick(conn, args, event_sink=event_sink)
            if "error" in summary:
                try:
                    await conn.rollback()
                except sqlite3.OperationalError:
                    pass
            (
                last_edge_log_ts,
                last_no_relevant_log_ts,
                last_no_relevant_signature,
            ) = _log_tick_summary(
                summary=summary,
                args=args,
                last_edge_log_ts=last_edge_log_ts,
                last_no_relevant_log_ts=last_no_relevant_log_ts,
                last_no_relevant_signature=last_no_relevant_signature,
            )
            if args.once:
                return 0
            await asyncio.sleep(args.interval_seconds)


async def _run_events_mode(
    *,
    args: argparse.Namespace,
    event_sink: EventPublisher,
    bus_url: str,
) -> int:
    print("state_source=events")
    print(f"event_bus={bus_url}")
    nc, js = await connect_jetstream(bus_url)
    state = LiveMarketState(max_spot_points=args.max_spot_points)
    sigma_memory = SigmaMemory()
    subscription = await _subscribe_market_events(js, args)
    print("events_consumer_mode=jetstream_pull")

    total_applied = 0
    total_parse_errors = 0
    total_fetch_errors = 0
    last_fetch_error_log_mono: float | None = None
    last_edge_log_ts: int | None = None
    last_no_relevant_log_ts: int | None = None
    last_no_relevant_signature: tuple[tuple[str, int], ...] | None = None
    compute_interval = max(int(args.interval_seconds), 1)
    next_compute_at = time.monotonic()
    try:
        while True:
            applied = 0
            parse_errors = 0
            try:
                for _ in range(8):
                    batch_applied, batch_parse_errors = await _consume_market_events(
                        subscription=subscription,
                        state=state,
                        max_batch=args.events_fetch_batch,
                        timeout_seconds=args.events_fetch_timeout_seconds,
                    )
                    applied += batch_applied
                    parse_errors += batch_parse_errors
                    if batch_applied == 0 and batch_parse_errors == 0:
                        break
            except Exception as exc:
                total_fetch_errors += 1
                now_mono = time.monotonic()
                if (
                    last_fetch_error_log_mono is None
                    or (now_mono - last_fetch_error_log_mono) >= 30.0
                ):
                    print(
                        "market_event_fetch_error type={type_name} err={err}".format(
                            type_name=type(exc).__name__,
                            err=exc,
                        )
                    )
                    last_fetch_error_log_mono = now_mono
                await asyncio.sleep(0.2)
            total_applied += applied
            total_parse_errors += parse_errors
            if time.monotonic() < next_compute_at and not args.once:
                await asyncio.sleep(0.2)
                continue

            summary, snapshot_rows = compute_edges_from_live_state(
                state=state,
                sigma_memory=sigma_memory,
                product_id=args.product_id,
                lookback_seconds=args.lookback_seconds,
                max_spot_points=args.max_spot_points,
                ewma_lambda=args.ewma_lambda,
                min_points=args.min_points,
                min_sigma_lookback_seconds=args.min_sigma_lookback_seconds,
                resample_seconds=args.sigma_resample_seconds,
                sigma_default=args.sigma_default,
                sigma_max=args.sigma_max,
                status=args.status,
                series=args.series,
                pct_band=args.pct_band,
                top_n=args.top_n,
                freshness_seconds=args.freshness_seconds,
                max_horizon_seconds=args.max_horizon_seconds,
                contracts=args.contracts,
                now_ts=getattr(args, "now_ts", None) or int(time.time()),
                min_ask_cents=args.min_ask_cents,
                max_ask_cents=args.max_ask_cents,
            )

            summary["event_publish_failures"] = await _publish_edge_events(
                rows=snapshot_rows,
                event_sink=event_sink,
            )
            summary["events_applied"] = applied
            summary["events_applied_total"] = total_applied
            summary["event_parse_errors_total"] = total_parse_errors
            summary["market_event_fetch_errors_total"] = total_fetch_errors
            pending = None
            try:
                consumer_info = await subscription.consumer_info()
                pending = int(getattr(consumer_info, "num_pending", 0))
            except Exception:
                pending = None
            summary["consumer_lag_pending"] = pending

            (
                last_edge_log_ts,
                last_no_relevant_log_ts,
                last_no_relevant_signature,
            ) = _log_tick_summary(
                summary=summary,
                args=args,
                last_edge_log_ts=last_edge_log_ts,
                last_no_relevant_log_ts=last_no_relevant_log_ts,
                last_no_relevant_signature=last_no_relevant_signature,
            )
            next_compute_at = time.monotonic() + compute_interval
            if args.once:
                return 0
    finally:
        try:
            await subscription.unsubscribe()
        except Exception as unsub_exc:
            print(
                "events_unsubscribe_error type={type_name} err={err}".format(
                    type_name=type(unsub_exc).__name__,
                    err=unsub_exc,
                )
            )
        await nc.drain()


async def _run() -> int:
    args = _parse_args()
    args.status = normalize_db_status(args.status)
    args.series = (
        normalize_series(args.series)
        if args.series is not None
        else list(BTC_SERIES_TICKERS)
    )
    settings = load_settings()
    setup_logger(settings.log_path)
    consumer_bus_url = (
        args.events_bus_url
        if args.events_bus_url
        else (
            settings.bus_url
            if (args.events_bus or args.state_source == "events")
            else None
        )
    )
    publisher_bus_url = (
        (args.events_bus_url if args.events_bus_url else settings.bus_url)
        if args.events_bus
        else None
    )
    event_sink = await EventPublisher.create(
        jsonl_path=args.events_jsonl_path,
        bus_url=publisher_bus_url,
    )
    try:
        if args.state_source == "events":
            if not consumer_bus_url:
                raise RuntimeError(
                    "--state-source events requires BUS_URL or --events-bus-url"
                )
            return await _run_events_mode(
                args=args,
                event_sink=event_sink,
                bus_url=consumer_bus_url,
            )
        return await _run_sqlite_mode(args=args, settings=settings, event_sink=event_sink)
    finally:
        await event_sink.close()


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
