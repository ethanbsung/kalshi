from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import time
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.data import init_db
from kalshi_bot.data.dao import Dao
from kalshi_bot.events import (
    ContractUpdateEvent,
    EdgeSnapshotEvent,
    EventPublisher,
    OpportunityDecisionEvent,
    OpportunityDecisionPayload,
    connect_jetstream,
    parse_event_dict,
)
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.strategy.opportunity_engine import (
    OpportunityConfig,
    build_opportunities_from_snapshots,
)

NOISY_PASS_REASONS = {
    "ev_below_threshold",
    "quote_stale",
    "spot_stale",
    "missing_yes_ask",
    "missing_no_ask",
    "position_open",
    "position_open_opposite_side",
    "cooldown_active",
    "take_cap",
    "top_n_cutoff",
}

QUIET_TICK_HEARTBEAT_SECONDS = 600
PASS_SAMPLE_HEARTBEAT_SECONDS = 600
OPPORTUNITY_STDOUT_HEARTBEAT_SECONDS = 60
MIN_TAU_MINUTES_KXBTC15M = 4.0
MIN_TAU_MINUTES_KXBTC = 10.0
MIN_TAU_MINUTES_KXBTCD = 10.0
TAIL_PRICE_MIN_CENTS = 10.0
TAIL_PRICE_MAX_CENTS = 90.0
TAIL_MIN_EV = 0.06
EVENTS_QUEUE_MAX_DEFAULT = 50000
EVENTS_MAX_ASOF_BATCHES_DEFAULT = 8
_LAST_HEARTBEAT_LOG_TS: int | None = None
_LAST_LOGGED_ASOF_TS: int | None = None
_LAST_PASS_SAMPLE_LOG_TS: int | None = None
_LAST_PASS_LOG_TS: int | None = None
_LAST_EV_INVARIANT_ASOF_TS: int | None = None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continuously build opportunity ledger entries."
    )
    parser.add_argument("--interval-seconds", type=int, default=10)
    parser.add_argument("--once", action="store_true")
    parser.add_argument(
        "--state-source",
        choices=["sqlite", "events"],
        default="sqlite",
        help="Read snapshots from SQLite tables (legacy) or event bus (Phase D).",
    )
    parser.add_argument("--min-ev", type=float, default=0.03)
    parser.add_argument(
        "--cost-buffer",
        type=float,
        default=0.01,
        help="Fixed per-contract EV haircut (dollars) applied as ev_net = ev_raw - cost_buffer.",
    )
    parser.add_argument("--top-n", type=int, default=None)
    parser.add_argument("--emit-passes", action="store_true")
    parser.add_argument("--both-sides", action="store_true")
    parser.add_argument("--min-ask-cents", type=float, default=1.0)
    parser.add_argument("--max-ask-cents", type=float, default=99.0)
    parser.add_argument("--max-spot-age", type=int, default=None)
    parser.add_argument("--max-quote-age", type=int, default=None)
    parser.add_argument(
        "--max-snapshot-age-seconds",
        type=int,
        default=60,
        help="If latest snapshot is older than this, skip opportunity generation.",
    )
    parser.add_argument("--paper", action="store_true", default=True)
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument(
        "--decision-log-path",
        type=str,
        default=None,
        help="Path for trader-style TAKE/PASS decision logs.",
    )
    parser.add_argument(
        "--decision-pass-reason-limit",
        type=int,
        default=2,
        help="Maximum number of PASS reasons to print per tick.",
    )
    parser.add_argument(
        "--decision-pass-sample-limit",
        type=int,
        default=1,
        help="Maximum number of PASS sample lines to print per tick.",
    )
    parser.add_argument(
        "--disable-decision-log",
        action="store_true",
        help="Disable trader-style TAKE/PASS decision logging.",
    )
    parser.add_argument(
        "--take-cooldown-seconds",
        type=int,
        default=None,
        help="Suppress repeated TAKEs for the same market+side within this window.",
    )
    parser.add_argument(
        "--max-takes-per-tick",
        type=int,
        default=None,
        help="Maximum TAKE signals to keep per tick after gating.",
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
        help="Publish opportunity_decision events to JetStream.",
    )
    parser.add_argument(
        "--events-bus-url",
        type=str,
        default=None,
        help="JetStream bus URL override (default BUS_URL).",
    )
    parser.add_argument(
        "--events-edge-subject",
        type=str,
        default="strategy.edge_snapshots",
        help="Bus subject for edge snapshot events in events mode.",
    )
    parser.add_argument(
        "--events-contract-subject",
        type=str,
        default="market.contract_updates",
        help="Bus subject for contract updates used to clear resolved positions.",
    )
    parser.add_argument(
        "--events-fetch-batch",
        type=int,
        default=500,
        help="Maximum queued bus messages to apply per batch.",
    )
    parser.add_argument(
        "--events-queue-max",
        type=int,
        default=EVENTS_QUEUE_MAX_DEFAULT,
        help="Max in-memory queue size for subscribed event messages.",
    )
    parser.add_argument(
        "--events-max-asof-batches",
        type=int,
        default=EVENTS_MAX_ASOF_BATCHES_DEFAULT,
        help="Number of edge asof_ts batches to retain in memory.",
    )
    return parser.parse_args()


async def _load_latest_asof_ts(conn: aiosqlite.Connection) -> int | None:
    cursor = await conn.execute("SELECT MAX(asof_ts) FROM kalshi_edge_snapshots")
    row = await cursor.fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


async def _load_snapshots(
    conn: aiosqlite.Connection, asof_ts: int
) -> list[dict[str, Any]]:
    cursor = await conn.execute(
        "SELECT asof_ts, market_id, settlement_ts, spot_ts, spot_price, "
        "sigma_annualized, prob_yes, prob_yes_raw, horizon_seconds, quote_ts, "
        "yes_bid, yes_ask, no_bid, no_ask, yes_mid, no_mid, ev_take_yes, "
        "ev_take_no, spot_age_seconds, quote_age_seconds, skip_reason, raw_json "
        "FROM kalshi_edge_snapshots WHERE asof_ts = ?",
        (asof_ts,),
    )
    rows = await cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    snapshots = [dict(zip(columns, row)) for row in rows]
    return snapshots


EST = timezone(timedelta(hours=-5), name="EST")


def _fmt_ts(ts: Any) -> str:
    try:
        if ts is None:
            return "NA"
        return datetime.fromtimestamp(int(ts), tz=EST).strftime(
            "%Y-%m-%dT%H:%M:%S EST"
        )
    except (TypeError, ValueError, OSError):
        return "NA"


def _fmt_num(value: Any, decimals: int = 4) -> str:
    try:
        if value is None:
            return "NA"
        return f"{float(value):.{decimals}f}"
    except (TypeError, ValueError):
        return "NA"


def _fmt_int(value: Any) -> str:
    try:
        if value is None:
            return "NA"
        return str(int(value))
    except (TypeError, ValueError):
        return "NA"


def _fmt_bool(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return "NA"


def _maybe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _maybe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _min_tau_minutes_for_market(market_id: str) -> float | None:
    if market_id.startswith("KXBTC15M-"):
        return MIN_TAU_MINUTES_KXBTC15M
    if market_id.startswith("KXBTC-"):
        return MIN_TAU_MINUTES_KXBTC
    if market_id.startswith("KXBTCD-"):
        return MIN_TAU_MINUTES_KXBTCD
    return None


def _decision_reason(row: dict[str, Any]) -> str:
    reason = row.get("reason_not_eligible")
    if isinstance(reason, str) and reason:
        return reason
    return "trade_signal"


def _ask_for_side(row: dict[str, Any]) -> Any:
    return row.get("best_yes_ask") if row.get("side") == "YES" else row.get("best_no_ask")


def _bid_for_side(row: dict[str, Any]) -> Any:
    return row.get("best_yes_bid") if row.get("side") == "YES" else row.get("best_no_bid")


def _format_decision_line(label: str, asof_ts: int, row: dict[str, Any]) -> str:
    meta = _raw_meta(row)
    return (
        f"{_fmt_ts(asof_ts)} {label} "
        f"market={row.get('market_id') or 'NA'} "
        f"side={row.get('side') or 'NA'} "
        f"reason={_decision_reason(row)} "
        f"ev={_fmt_num(row.get('ev_net'), 4)} "
        f"ev_raw={_fmt_num(row.get('ev_raw'), 4)} "
        f"ev_net={_fmt_num(row.get('ev_net'), 4)} "
        f"cost_buffer={_fmt_num(row.get('cost_buffer'), 4)} "
        f"p_model={_fmt_num(row.get('p_model'), 4)} "
        f"p_market={_fmt_num(row.get('p_market'), 4)} "
        f"sigma={_fmt_num(row.get('sigma'), 4)} "
        f"ask_c={_fmt_num(_ask_for_side(row), 2)} "
        f"bid_c={_fmt_num(_bid_for_side(row), 2)} "
        f"spread_c={_fmt_num(row.get('spread'), 2)} "
        f"spot={_fmt_num(row.get('spot_price'), 2)} "
        f"tau_m={_fmt_num(row.get('tau'), 1)} "
        f"sigma_src={meta.get('sigma_source') or 'NA'} "
        f"sigma_ok={_fmt_bool(meta.get('sigma_ok'))} "
        f"sigma_reason={meta.get('sigma_reason') or 'NA'} "
        f"quote_age_s={_fmt_int(meta.get('quote_age_seconds'))} "
        f"spot_age_s={_fmt_int(meta.get('spot_age_seconds'))}"
    )


def _raw_meta(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("raw_json")
    if not isinstance(raw, str) or not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def _count_pass_reasons(pass_rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in pass_rows:
        counts[_decision_reason(row)] += 1
    return dict(counts)


def _is_abnormal_pass_reason(reason: str) -> bool:
    return reason not in NOISY_PASS_REASONS


def _abnormal_pass_rows(pass_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row for row in pass_rows if _is_abnormal_pass_reason(_decision_reason(row))
    ]


def _as_pass_row(row: dict[str, Any], reason: str) -> dict[str, Any]:
    updated = dict(row)
    updated["eligible"] = 0
    updated["would_trade"] = 0
    updated["reason_not_eligible"] = reason
    meta = _raw_meta(updated)
    meta["decision"] = "PASS"
    meta["decision_reason"] = reason
    updated["raw_json"] = json.dumps(meta)
    return updated


async def _load_recent_take_keys(
    conn: aiosqlite.Connection, since_ts: int
) -> set[tuple[str, str]]:
    cursor = await conn.execute(
        """
        SELECT market_id, side
        FROM opportunities
        WHERE would_trade = 1 AND ts_eval >= ?
        """,
        (since_ts,),
    )
    rows = await cursor.fetchall()
    keys: set[tuple[str, str]] = set()
    for market_id, side in rows:
        if isinstance(market_id, str) and isinstance(side, str):
            keys.add((market_id, side))
    return keys


async def _load_open_take_state(
    conn: aiosqlite.Connection,
) -> tuple[set[tuple[str, str]], int]:
    cursor = await conn.execute(
        """
        SELECT o.market_id, o.side, COUNT(*) AS qty
        FROM opportunities o
        LEFT JOIN kalshi_contracts c ON c.ticker = o.market_id
        WHERE o.would_trade = 1
          AND c.outcome IS NULL
        GROUP BY o.market_id, o.side
        """
    )
    rows = await cursor.fetchall()
    keys: set[tuple[str, str]] = set()
    open_qty_total = 0
    for market_id, side, qty in rows:
        if isinstance(market_id, str) and isinstance(side, str):
            keys.add((market_id, side))
        try:
            open_qty_total += int(qty or 0)
        except (TypeError, ValueError):
            continue
    return keys, open_qty_total


def _select_pass_samples(
    pass_rows: list[dict[str, Any]],
    reason_counts: dict[str, int],
    limit: int,
) -> list[dict[str, Any]]:
    if limit <= 0 or not pass_rows:
        return []

    samples: list[dict[str, Any]] = []
    selected_ids: set[tuple[str, str, str]] = set()

    def row_id(row: dict[str, Any]) -> tuple[str, str, str]:
        return (
            str(row.get("market_id") or ""),
            str(row.get("side") or ""),
            _decision_reason(row),
        )

    def add_row(row: dict[str, Any]) -> None:
        if len(samples) >= limit:
            return
        rid = row_id(row)
        if rid in selected_ids:
            return
        selected_ids.add(rid)
        samples.append(row)

    near_miss = [
        row
        for row in pass_rows
        if _decision_reason(row) == "ev_below_threshold" and row.get("ev_net") is not None
    ]
    near_miss.sort(key=lambda r: float(r.get("ev_net") or -1e9), reverse=True)
    for row in near_miss:
        add_row(row)
        if len(samples) >= limit:
            return samples

    sorted_reasons = sorted(reason_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    for reason, _ in sorted_reasons:
        reason_rows = [row for row in pass_rows if _decision_reason(row) == reason]
        reason_rows.sort(
            key=lambda r: (
                float(r.get("ev_net") or -1e9),
                str(r.get("market_id") or ""),
            ),
            reverse=True,
        )
        for row in reason_rows:
            add_row(row)
            if len(samples) >= limit:
                return samples

    return samples


def _write_decision_log(
    *,
    path: Path,
    summary: dict[str, Any],
    min_ev: float,
    cost_buffer: float,
    pass_reason_limit: int,
    pass_sample_limit: int,
) -> None:
    global _LAST_HEARTBEAT_LOG_TS, _LAST_LOGGED_ASOF_TS, _LAST_PASS_SAMPLE_LOG_TS, _LAST_PASS_LOG_TS, _LAST_EV_INVARIANT_ASOF_TS

    asof_ts = int(summary["asof_ts"])
    take_rows = list(summary.get("take_rows") or [])
    pass_rows = list(summary.get("pass_rows") or [])
    abnormal_pass_rows = _abnormal_pass_rows(pass_rows)
    reason_counts = _count_pass_reasons(abnormal_pass_rows)
    top_reasons = sorted(reason_counts.items(), key=lambda kv: (-kv[1], kv[0]))[
        : max(pass_reason_limit, 0)
    ]
    pass_samples = _select_pass_samples(
        abnormal_pass_rows, reason_counts, pass_sample_limit
    )
    should_emit_pass_logs = (
        _LAST_PASS_LOG_TS is None
        or (asof_ts - _LAST_PASS_LOG_TS) >= PASS_SAMPLE_HEARTBEAT_SECONDS
    )
    if not should_emit_pass_logs:
        top_reasons = []
        pass_samples = []
    if not pass_samples and pass_rows:
        should_emit_periodic_sample = (
            should_emit_pass_logs
            and (
            _LAST_PASS_SAMPLE_LOG_TS is None
            or (asof_ts - _LAST_PASS_SAMPLE_LOG_TS) >= PASS_SAMPLE_HEARTBEAT_SECONDS
            )
        )
        if should_emit_periodic_sample:
            pass_samples = _select_pass_samples(
                pass_rows,
                _count_pass_reasons(pass_rows),
                min(max(pass_sample_limit, 0), 1),
            )

    take_rows.sort(key=lambda r: float(r.get("ev_net") or -1e9), reverse=True)

    should_write_tick = True
    if int(summary.get("takes") or 0) == 0:
        if _LAST_LOGGED_ASOF_TS == asof_ts:
            should_write_tick = False
        elif (
            _LAST_HEARTBEAT_LOG_TS is not None
            and (asof_ts - _LAST_HEARTBEAT_LOG_TS) < QUIET_TICK_HEARTBEAT_SECONDS
        ):
            should_write_tick = False

    with path.open("a", encoding="utf-8") as f:
        if should_write_tick:
            f.write(
                f"{_fmt_ts(asof_ts)} TICK "
                f"asof_ts={asof_ts} snapshots={summary['snapshots']} "
                f"takes={summary['takes']} passes={summary['passes']} "
                f"inserted={summary['inserted']} open_positions={summary['open_positions']} "
                f"min_ev={_fmt_num(min_ev, 4)} cost_buffer={_fmt_num(cost_buffer, 4)}\n"
            )
            _LAST_LOGGED_ASOF_TS = asof_ts
            if int(summary.get("takes") or 0) == 0:
                _LAST_HEARTBEAT_LOG_TS = asof_ts

        if top_reasons:
            reasons_blob = " ".join(f"{reason}={count}" for reason, count in top_reasons)
            f.write(f"{_fmt_ts(asof_ts)} PASS_SUMMARY {reasons_blob}\n")

        ev_invariant_failed = int(
            (summary.get("counters") or {}).get("ev_invariant_failed") or 0
        )
        ev_invariant_max_diff = float(
            (summary.get("counters") or {}).get("ev_invariant_max_diff") or 0.0
        )
        if ev_invariant_failed > 0 and _LAST_EV_INVARIANT_ASOF_TS != asof_ts:
            f.write(
                f"{_fmt_ts(asof_ts)} EV_INVARIANT_FAILED "
                f"count={ev_invariant_failed} "
                f"max_diff={_fmt_num(ev_invariant_max_diff, 4)}\n"
            )
            _LAST_EV_INVARIANT_ASOF_TS = asof_ts

        for row in take_rows:
            f.write(_format_decision_line("TAKE", asof_ts, row) + "\n")

        for row in pass_samples:
            f.write(_format_decision_line("PASS_SAMPLE", asof_ts, row) + "\n")
        if pass_samples:
            _LAST_PASS_SAMPLE_LOG_TS = asof_ts
        if top_reasons or pass_samples:
            _LAST_PASS_LOG_TS = asof_ts


def _build_summary_from_snapshots(
    *,
    snapshots: list[dict[str, Any]],
    asof_ts: int,
    args: argparse.Namespace,
    open_take_keys: set[tuple[str, str]],
    open_qty_before: int,
    recent_take_keys: set[tuple[str, str]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    config = OpportunityConfig(
        min_ev=args.min_ev,
        cost_buffer=max(float(args.cost_buffer or 0.0), 0.0),
        min_ask_cents=args.min_ask_cents,
        max_ask_cents=args.max_ask_cents,
        max_spot_age=args.max_spot_age,
        max_quote_age=args.max_quote_age,
        top_n=args.top_n,
        emit_passes=True,
        best_side_only=not args.both_sides,
    )
    all_rows, counters = build_opportunities_from_snapshots(snapshots, config)
    take_rows = [row for row in all_rows if int(row.get("would_trade") or 0) == 1]
    pass_rows = [row for row in all_rows if int(row.get("would_trade") or 0) != 1]
    take_rows.sort(key=lambda row: float(row.get("ev_net") or -1e9), reverse=True)

    tail_ev_blocked = 0
    if take_rows:
        kept_take_rows: list[dict[str, Any]] = []
        for row in take_rows:
            ask_c = _ask_for_side(row)
            ev = row.get("ev_net")
            if (
                isinstance(ask_c, (int, float))
                and isinstance(ev, (int, float))
                and (
                    float(ask_c) <= TAIL_PRICE_MIN_CENTS
                    or float(ask_c) >= TAIL_PRICE_MAX_CENTS
                )
                and float(ev) < TAIL_MIN_EV
            ):
                pass_rows.append(_as_pass_row(row, "tail_ev_below_threshold"))
                tail_ev_blocked += 1
            else:
                kept_take_rows.append(row)
        take_rows = kept_take_rows

    near_expiry_blocked = 0
    if take_rows:
        kept_take_rows = []
        for row in take_rows:
            market_id = str(row.get("market_id") or "")
            tau_minutes = row.get("tau")
            min_tau_minutes = _min_tau_minutes_for_market(market_id)
            if (
                min_tau_minutes is not None
                and isinstance(tau_minutes, (int, float))
                and float(tau_minutes) < min_tau_minutes
            ):
                pass_rows.append(_as_pass_row(row, "near_expiry"))
                near_expiry_blocked += 1
            else:
                kept_take_rows.append(row)
        take_rows = kept_take_rows

    open_market_sides: dict[str, set[str]] = {}
    for market_id, side in open_take_keys:
        open_market_sides.setdefault(market_id, set()).add(side)

    open_position_blocked = 0
    open_opposite_side_blocked = 0
    if take_rows:
        kept_take_rows = []
        for row in take_rows:
            market_id = str(row.get("market_id") or "")
            side = str(row.get("side") or "")
            key = (market_id, side)
            if key in open_take_keys:
                pass_rows.append(_as_pass_row(row, "position_open"))
                open_position_blocked += 1
            elif market_id in open_market_sides:
                pass_rows.append(_as_pass_row(row, "position_open_opposite_side"))
                open_opposite_side_blocked += 1
            else:
                kept_take_rows.append(row)
        take_rows = kept_take_rows

    cooldown_blocked = 0
    cooldown_seconds = max(int(args.take_cooldown_seconds or 0), 0)
    if cooldown_seconds > 0 and take_rows:
        kept_take_rows = []
        for row in take_rows:
            key = (str(row.get("market_id") or ""), str(row.get("side") or ""))
            if key in recent_take_keys:
                pass_rows.append(_as_pass_row(row, "cooldown_active"))
                cooldown_blocked += 1
            else:
                kept_take_rows.append(row)
        take_rows = kept_take_rows

    take_cap_blocked = 0
    if args.max_takes_per_tick is not None:
        take_cap = max(int(args.max_takes_per_tick), 0)
        if len(take_rows) > take_cap:
            dropped = take_rows[take_cap:]
            take_rows = take_rows[:take_cap]
            for row in dropped:
                pass_rows.append(_as_pass_row(row, "take_cap"))
            take_cap_blocked = len(dropped)

    if args.live:
        return {"error": "live_trading_not_supported"}, []

    open_qty_after = open_qty_before + len(take_rows)
    counters["open_position_blocked"] = open_position_blocked
    counters["open_opposite_side_blocked"] = open_opposite_side_blocked
    counters["tail_ev_blocked"] = tail_ev_blocked
    counters["near_expiry_blocked"] = near_expiry_blocked
    counters["cooldown_blocked"] = cooldown_blocked
    counters["take_cap_blocked"] = take_cap_blocked
    counters["takes"] = len(take_rows)
    counters["passes"] = len(pass_rows)

    rows_to_insert = (take_rows + pass_rows) if args.emit_passes else take_rows
    summary = {
        "asof_ts": asof_ts,
        "snapshots": len(snapshots),
        "takes": len(take_rows),
        "passes": len(pass_rows),
        "inserted": 0,
        "open_positions": open_qty_after,
        "counters": counters,
        "take_rows": take_rows,
        "pass_rows": pass_rows,
    }
    return summary, rows_to_insert


async def _publish_take_events(
    *,
    rows: list[dict[str, Any]],
    event_sink: EventPublisher,
    asof_ts: int,
) -> int:
    if not event_sink.enabled:
        return 0
    failures = 0
    for row in rows:
        try:
            raw_json = row.get("raw_json")
            payload = OpportunityDecisionPayload(
                ts_eval=int(row.get("ts_eval") or asof_ts),
                market_id=str(row.get("market_id") or ""),
                eligible=bool(int(row.get("eligible") or 0) == 1),
                would_trade=bool(int(row.get("would_trade") or 0) == 1),
                side=(str(row.get("side")) if row.get("side") is not None else None),
                reason_not_eligible=(
                    str(row.get("reason_not_eligible"))
                    if row.get("reason_not_eligible") is not None
                    else None
                ),
                ev_raw=_maybe_float(row.get("ev_raw")),
                ev_net=_maybe_float(row.get("ev_net")),
                settlement_ts=_maybe_int(row.get("settlement_ts")),
                strike=(str(row.get("strike")) if row.get("strike") is not None else None),
                spot_price=_maybe_float(row.get("spot_price")),
                sigma=_maybe_float(row.get("sigma")),
                tau=_maybe_float(row.get("tau")),
                p_model=_maybe_float(row.get("p_model")),
                p_market=_maybe_float(row.get("p_market")),
                best_yes_bid=_maybe_float(row.get("best_yes_bid")),
                best_yes_ask=_maybe_float(row.get("best_yes_ask")),
                best_no_bid=_maybe_float(row.get("best_no_bid")),
                best_no_ask=_maybe_float(row.get("best_no_ask")),
                spread=_maybe_float(row.get("spread")),
                cost_buffer=_maybe_float(row.get("cost_buffer")),
                raw_json=(str(raw_json) if isinstance(raw_json, str) and raw_json else None),
                strategy_version=1,
            )
            await event_sink.publish(
                OpportunityDecisionEvent(
                    source="run_opportunity_loop",
                    payload=payload,
                )
            )
        except Exception:
            failures += 1
    return failures


async def run_tick(conn: aiosqlite.Connection, args: argparse.Namespace) -> dict[str, Any]:
    asof_ts = await _load_latest_asof_ts(conn)
    if asof_ts is None:
        return {"error": "no_edge_snapshots"}
    max_snapshot_age_seconds = max(int(args.max_snapshot_age_seconds or 0), 0)
    if max_snapshot_age_seconds > 0:
        now_ts = int(time.time())
        snapshot_age_seconds = max(0, now_ts - asof_ts)
        if snapshot_age_seconds > max_snapshot_age_seconds:
            return {
                "error": "stale_edge_snapshots",
                "asof_ts": asof_ts,
                "snapshot_age_seconds": snapshot_age_seconds,
                "max_snapshot_age_seconds": max_snapshot_age_seconds,
            }
    snapshots = await _load_snapshots(conn, asof_ts)
    open_take_keys, open_qty_before = await _load_open_take_state(conn)
    cooldown_seconds = max(int(args.take_cooldown_seconds or 0), 0)
    recent_take_keys = (
        await _load_recent_take_keys(conn, asof_ts - cooldown_seconds)
        if cooldown_seconds > 0
        else set()
    )
    summary, rows_to_insert = _build_summary_from_snapshots(
        snapshots=snapshots,
        asof_ts=asof_ts,
        args=args,
        open_take_keys=open_take_keys,
        open_qty_before=open_qty_before,
        recent_take_keys=recent_take_keys,
    )
    if "error" in summary:
        return summary

    inserted = 0
    if rows_to_insert:
        dao = Dao(conn)
        before = conn.total_changes
        await dao.insert_opportunities(rows_to_insert)
        inserted = conn.total_changes - before
        await conn.commit()
    summary["inserted"] = inserted
    return summary


class _OpportunityEventState:
    def __init__(self, *, max_asof_batches: int) -> None:
        self._max_asof_batches = max(max_asof_batches, 2)
        self._snapshots_by_asof: dict[int, dict[str, dict[str, Any]]] = {}
        self._asof_order: deque[int] = deque()
        self.open_take_keys: set[tuple[str, str]] = set()
        self._recent_takes: deque[tuple[int, tuple[str, str]]] = deque()

    def upsert_snapshot(self, row: dict[str, Any]) -> None:
        asof_ts = int(row["asof_ts"])
        market_id = str(row["market_id"])
        bucket = self._snapshots_by_asof.get(asof_ts)
        if bucket is None:
            bucket = {}
            self._snapshots_by_asof[asof_ts] = bucket
            self._asof_order.append(asof_ts)
            while len(self._asof_order) > self._max_asof_batches:
                oldest = self._asof_order.popleft()
                self._snapshots_by_asof.pop(oldest, None)
        bucket[market_id] = row

    def latest_batch(self) -> tuple[int | None, list[dict[str, Any]]]:
        if not self._asof_order:
            return None, []
        asof_ts = max(self._asof_order)
        rows = list((self._snapshots_by_asof.get(asof_ts) or {}).values())
        return asof_ts, rows

    def record_takes(self, take_rows: list[dict[str, Any]], asof_ts: int) -> None:
        for row in take_rows:
            key = (str(row.get("market_id") or ""), str(row.get("side") or ""))
            self.open_take_keys.add(key)
            self._recent_takes.append((asof_ts, key))

    def mark_market_resolved(self, market_id: str) -> None:
        if not market_id:
            return
        self.open_take_keys = {
            key for key in self.open_take_keys if key[0] != market_id
        }

    def recent_take_keys(self, *, now_ts: int, cooldown_seconds: int) -> set[tuple[str, str]]:
        if cooldown_seconds <= 0:
            return set()
        cutoff = now_ts - cooldown_seconds
        while self._recent_takes and self._recent_takes[0][0] < cutoff:
            self._recent_takes.popleft()
        return {key for _, key in self._recent_takes}

    @property
    def open_qty(self) -> int:
        return len(self.open_take_keys)


def _snapshot_from_edge_event(event: EdgeSnapshotEvent) -> dict[str, Any]:
    payload = event.payload
    asof_ts = int(payload.asof_ts)
    spot_ts = int(payload.spot_ts) if payload.spot_ts is not None else asof_ts
    settlement_ts = (
        int(payload.settlement_ts) if payload.settlement_ts is not None else None
    )
    horizon_seconds = (
        int(payload.horizon_seconds) if payload.horizon_seconds is not None else None
    )
    if horizon_seconds is None and settlement_ts is not None and spot_ts is not None:
        horizon_seconds = max(int(settlement_ts - spot_ts), 0)
    now_ts = int(time.time())
    spot_age_seconds = (
        int(payload.spot_age_seconds)
        if payload.spot_age_seconds is not None
        else max(now_ts - spot_ts, 0)
    )
    quote_ts = int(payload.quote_ts) if payload.quote_ts is not None else None
    quote_age_seconds = (
        int(payload.quote_age_seconds)
        if payload.quote_age_seconds is not None
        else (max(now_ts - quote_ts, 0) if quote_ts is not None else None)
    )
    return {
        "asof_ts": asof_ts,
        "market_id": str(payload.market_id),
        "settlement_ts": settlement_ts,
        "spot_ts": spot_ts,
        "spot_price": float(payload.spot_price),
        "sigma_annualized": float(payload.sigma_annualized),
        "prob_yes": float(payload.prob_yes),
        "prob_yes_raw": (
            float(payload.prob_yes_raw)
            if payload.prob_yes_raw is not None
            else float(payload.prob_yes)
        ),
        "horizon_seconds": horizon_seconds,
        "quote_ts": quote_ts,
        "yes_bid": float(payload.yes_bid) if payload.yes_bid is not None else None,
        "yes_ask": float(payload.yes_ask) if payload.yes_ask is not None else None,
        "no_bid": float(payload.no_bid) if payload.no_bid is not None else None,
        "no_ask": float(payload.no_ask) if payload.no_ask is not None else None,
        "yes_mid": float(payload.yes_mid) if payload.yes_mid is not None else None,
        "no_mid": float(payload.no_mid) if payload.no_mid is not None else None,
        "ev_take_yes": float(payload.ev_take_yes),
        "ev_take_no": float(payload.ev_take_no),
        "spot_age_seconds": spot_age_seconds,
        "quote_age_seconds": quote_age_seconds,
        "skip_reason": None,
        "raw_json": payload.raw_json,
        "strike": payload.strike,
    }


def _consume_events_from_queue(
    *,
    queue: asyncio.Queue[bytes],
    state: _OpportunityEventState,
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
            if isinstance(event, EdgeSnapshotEvent):
                state.upsert_snapshot(_snapshot_from_edge_event(event))
                applied += 1
            elif isinstance(event, ContractUpdateEvent):
                payload = event.payload
                if payload.outcome is not None or payload.settled_ts is not None:
                    state.mark_market_resolved(str(payload.ticker))
                applied += 1
        except Exception:
            parse_errors += 1
    return applied, parse_errors


async def _run_sqlite_mode(
    *,
    args: argparse.Namespace,
    settings: Any,
    logger: Any,
    decision_log_path: Path,
    bus_url: str | None,
) -> int:
    print(f"DB path: {settings.db_path}")
    if not args.disable_decision_log:
        print(f"Decision log path: {decision_log_path}")
    await init_db(settings.db_path)

    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 15000;")
        await conn.commit()
        event_sink = await EventPublisher.create(
            jsonl_path=args.events_jsonl_path,
            bus_url=bus_url,
        )
        event_publish_failures_total = 0
        backoff = [0.2, 0.5, 1.0]
        last_stdout_asof_ts: int | None = None
        last_stdout_log_ts: int | None = None
        last_ev_invariant_warn_asof_ts: int | None = None
        last_stale_snapshot_log_ts: int | None = None
        try:
            while True:
                try:
                    summary = await run_tick(conn, args)
                except sqlite3.OperationalError as exc:
                    if "locked" not in str(exc).lower():
                        raise
                    for delay in backoff:
                        await asyncio.sleep(delay)
                        try:
                            summary = await run_tick(conn, args)
                            break
                        except sqlite3.OperationalError as retry_exc:
                            if "locked" not in str(retry_exc).lower():
                                raise
                    else:
                        summary = {"error": "db_locked"}

                if "error" in summary:
                    error = str(summary.get("error") or "unknown")
                    if error == "stale_edge_snapshots":
                        now_ts = int(time.time())
                        should_log = (
                            last_stale_snapshot_log_ts is None
                            or (now_ts - last_stale_snapshot_log_ts) >= 60
                        )
                        if should_log:
                            logger.warning(
                                "opportunity_tick_error",
                                extra={
                                    "error": error,
                                    "asof_ts": summary.get("asof_ts"),
                                    "snapshot_age_seconds": summary.get(
                                        "snapshot_age_seconds"
                                    ),
                                    "max_snapshot_age_seconds": summary.get(
                                        "max_snapshot_age_seconds"
                                    ),
                                },
                            )
                            print(
                                "tick_error={error} asof_ts={asof_ts} "
                                "snapshot_age_s={snapshot_age_s} max_snapshot_age_s={max_age}".format(
                                    error=error,
                                    asof_ts=summary.get("asof_ts"),
                                    snapshot_age_s=summary.get("snapshot_age_seconds"),
                                    max_age=summary.get("max_snapshot_age_seconds"),
                                )
                            )
                            last_stale_snapshot_log_ts = now_ts
                    else:
                        logger.warning("opportunity_tick_error", extra={"error": error})
                        print(f"tick_error={error}")
                else:
                    asof_ts = int(summary.get("asof_ts") or 0)
                    takes = int(summary.get("takes") or 0)
                    inserted = int(summary.get("inserted") or 0)
                    event_publish_failures_total += await _publish_take_events(
                        rows=summary.get("take_rows") or [],
                        event_sink=event_sink,
                        asof_ts=asof_ts,
                    )
                    ev_invariant_failed = int(
                        (summary.get("counters") or {}).get("ev_invariant_failed") or 0
                    )
                    if (
                        ev_invariant_failed > 0
                        and last_ev_invariant_warn_asof_ts != asof_ts
                    ):
                        logger.warning(
                            "opportunity_ev_invariant_failed",
                            extra={
                                "asof_ts": asof_ts,
                                "count": ev_invariant_failed,
                                "max_diff": float(
                                    (summary.get("counters") or {}).get(
                                        "ev_invariant_max_diff"
                                    )
                                    or 0.0
                                ),
                                "snapshots": int(summary.get("snapshots") or 0),
                            },
                        )
                        last_ev_invariant_warn_asof_ts = asof_ts
                    should_print_tick = takes > 0 or inserted > 0
                    if not should_print_tick:
                        if last_stdout_asof_ts == asof_ts:
                            should_print_tick = False
                        elif (
                            last_stdout_log_ts is None
                            or (asof_ts - last_stdout_log_ts)
                            >= OPPORTUNITY_STDOUT_HEARTBEAT_SECONDS
                        ):
                            should_print_tick = True
                    if should_print_tick:
                        print(
                            "opportunity_tick ts={asof_ts} snapshots={snapshots} "
                            "takes={takes} passes={passes} inserted={inserted} "
                            "open_positions={open_positions} event_publish_failures_total={event_failures}".format(
                                event_failures=event_publish_failures_total,
                                **summary,
                            )
                        )
                        last_stdout_asof_ts = asof_ts
                        if asof_ts > 0:
                            last_stdout_log_ts = asof_ts
                    if not args.disable_decision_log:
                        try:
                            _write_decision_log(
                                path=decision_log_path,
                                summary=summary,
                                min_ev=args.min_ev,
                                cost_buffer=args.cost_buffer,
                                pass_reason_limit=args.decision_pass_reason_limit,
                                pass_sample_limit=args.decision_pass_sample_limit,
                            )
                        except OSError as exc:
                            logger.warning(
                                "decision_log_write_failed",
                                extra={"path": str(decision_log_path), "error": str(exc)},
                            )
                    if args.debug:
                        print(f"counters={summary['counters']}")

                if args.once:
                    break
                await asyncio.sleep(args.interval_seconds)
        finally:
            await event_sink.close()
    return 0


async def _run_events_mode(
    *,
    args: argparse.Namespace,
    settings: Any,
    logger: Any,
    decision_log_path: Path,
    bus_url: str,
) -> int:
    print("state_source=events")
    print(f"event_bus={bus_url}")
    if not args.disable_decision_log:
        print(f"Decision log path: {decision_log_path}")
    nc, _ = await connect_jetstream(bus_url)
    event_sink = await EventPublisher.create(
        jsonl_path=args.events_jsonl_path,
        bus_url=bus_url if args.events_bus else None,
    )
    edge_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=max(args.events_queue_max, 1000))
    contract_queue: asyncio.Queue[bytes] = asyncio.Queue(
        maxsize=max(args.events_queue_max // 4, 1000)
    )
    dropped_edge_events = 0
    dropped_contract_events = 0
    state = _OpportunityEventState(max_asof_batches=args.events_max_asof_batches)

    async def _on_edge_msg(msg: Any) -> None:
        nonlocal dropped_edge_events
        try:
            edge_queue.put_nowait(msg.data)
        except asyncio.QueueFull:
            dropped_edge_events += 1

    async def _on_contract_msg(msg: Any) -> None:
        nonlocal dropped_contract_events
        try:
            contract_queue.put_nowait(msg.data)
        except asyncio.QueueFull:
            dropped_contract_events += 1

    edge_sub = await nc.subscribe(args.events_edge_subject, cb=_on_edge_msg)
    contract_sub = await nc.subscribe(args.events_contract_subject, cb=_on_contract_msg)
    print("opportunity_consumer_mode=core_nats")

    event_publish_failures_total = 0
    total_events_applied = 0
    total_event_parse_errors = 0
    last_stdout_asof_ts: int | None = None
    last_stdout_log_ts: int | None = None
    last_ev_invariant_warn_asof_ts: int | None = None
    last_stale_snapshot_log_ts: int | None = None
    try:
        while True:
            batch_applied = 0
            batch_parse_errors = 0
            for _ in range(8):
                edge_applied, edge_errors = _consume_events_from_queue(
                    queue=edge_queue,
                    state=state,
                    max_batch=args.events_fetch_batch,
                )
                contract_applied, contract_errors = _consume_events_from_queue(
                    queue=contract_queue,
                    state=state,
                    max_batch=args.events_fetch_batch,
                )
                cycle_applied = edge_applied + contract_applied
                cycle_errors = edge_errors + contract_errors
                batch_applied += cycle_applied
                batch_parse_errors += cycle_errors
                if cycle_applied == 0 and cycle_errors == 0:
                    break
            total_events_applied += batch_applied
            total_event_parse_errors += batch_parse_errors

            asof_ts, snapshots = state.latest_batch()
            if asof_ts is None:
                summary: dict[str, Any] = {"error": "no_edge_snapshots"}
            else:
                max_snapshot_age_seconds = max(int(args.max_snapshot_age_seconds or 0), 0)
                now_ts = int(time.time())
                snapshot_age_seconds = max(0, now_ts - int(asof_ts))
                if (
                    max_snapshot_age_seconds > 0
                    and snapshot_age_seconds > max_snapshot_age_seconds
                ):
                    summary = {
                        "error": "stale_edge_snapshots",
                        "asof_ts": asof_ts,
                        "snapshot_age_seconds": snapshot_age_seconds,
                        "max_snapshot_age_seconds": max_snapshot_age_seconds,
                    }
                else:
                    recent_take_keys = state.recent_take_keys(
                        now_ts=int(asof_ts),
                        cooldown_seconds=max(int(args.take_cooldown_seconds or 0), 0),
                    )
                    summary, _ = _build_summary_from_snapshots(
                        snapshots=snapshots,
                        asof_ts=int(asof_ts),
                        args=args,
                        open_take_keys=state.open_take_keys,
                        open_qty_before=state.open_qty,
                        recent_take_keys=recent_take_keys,
                    )
                    if "error" not in summary:
                        state.record_takes(summary.get("take_rows") or [], int(asof_ts))
                        event_publish_failures_total += await _publish_take_events(
                            rows=summary.get("take_rows") or [],
                            event_sink=event_sink,
                            asof_ts=int(asof_ts),
                        )
                        summary["event_publish_failures_total"] = event_publish_failures_total
                        summary["events_applied_total"] = total_events_applied
                        summary["event_parse_errors_total"] = total_event_parse_errors
                        summary["edge_queue_size"] = edge_queue.qsize()
                        summary["contract_queue_size"] = contract_queue.qsize()
                        summary["dropped_edge_events"] = dropped_edge_events
                        summary["dropped_contract_events"] = dropped_contract_events

            if "error" in summary:
                error = str(summary.get("error") or "unknown")
                if error == "stale_edge_snapshots":
                    now_ts = int(time.time())
                    should_log = (
                        last_stale_snapshot_log_ts is None
                        or (now_ts - last_stale_snapshot_log_ts) >= 60
                    )
                    if should_log:
                        logger.warning(
                            "opportunity_tick_error",
                            extra={
                                "error": error,
                                "asof_ts": summary.get("asof_ts"),
                                "snapshot_age_seconds": summary.get(
                                    "snapshot_age_seconds"
                                ),
                                "max_snapshot_age_seconds": summary.get(
                                    "max_snapshot_age_seconds"
                                ),
                            },
                        )
                        print(
                            "tick_error={error} asof_ts={asof_ts} "
                            "snapshot_age_s={snapshot_age_s} max_snapshot_age_s={max_age}".format(
                                error=error,
                                asof_ts=summary.get("asof_ts"),
                                snapshot_age_s=summary.get("snapshot_age_seconds"),
                                max_age=summary.get("max_snapshot_age_seconds"),
                            )
                        )
                        last_stale_snapshot_log_ts = now_ts
                else:
                    logger.warning("opportunity_tick_error", extra={"error": error})
                    print(f"tick_error={error}")
            else:
                asof_ts_val = int(summary.get("asof_ts") or 0)
                takes = int(summary.get("takes") or 0)
                inserted = int(summary.get("inserted") or 0)
                ev_invariant_failed = int(
                    (summary.get("counters") or {}).get("ev_invariant_failed") or 0
                )
                if (
                    ev_invariant_failed > 0
                    and last_ev_invariant_warn_asof_ts != asof_ts_val
                ):
                    logger.warning(
                        "opportunity_ev_invariant_failed",
                        extra={
                            "asof_ts": asof_ts_val,
                            "count": ev_invariant_failed,
                            "max_diff": float(
                                (summary.get("counters") or {}).get("ev_invariant_max_diff")
                                or 0.0
                            ),
                            "snapshots": int(summary.get("snapshots") or 0),
                        },
                    )
                    last_ev_invariant_warn_asof_ts = asof_ts_val
                should_print_tick = takes > 0 or inserted > 0
                if not should_print_tick:
                    if last_stdout_asof_ts == asof_ts_val:
                        should_print_tick = False
                    elif (
                        last_stdout_log_ts is None
                        or (asof_ts_val - last_stdout_log_ts)
                        >= OPPORTUNITY_STDOUT_HEARTBEAT_SECONDS
                    ):
                        should_print_tick = True
                if should_print_tick:
                    print(
                        "opportunity_tick ts={asof_ts} snapshots={snapshots} "
                        "takes={takes} passes={passes} inserted={inserted} "
                        "open_positions={open_positions} event_publish_failures_total={event_failures} "
                        "events_applied_total={events_applied_total} "
                        "event_parse_errors_total={event_parse_errors_total}".format(
                            event_failures=event_publish_failures_total,
                            **summary,
                        )
                    )
                    last_stdout_asof_ts = asof_ts_val
                    if asof_ts_val > 0:
                        last_stdout_log_ts = asof_ts_val
                if not args.disable_decision_log:
                    try:
                        _write_decision_log(
                            path=decision_log_path,
                            summary=summary,
                            min_ev=args.min_ev,
                            cost_buffer=args.cost_buffer,
                            pass_reason_limit=args.decision_pass_reason_limit,
                            pass_sample_limit=args.decision_pass_sample_limit,
                        )
                    except OSError as exc:
                        logger.warning(
                            "decision_log_write_failed",
                            extra={"path": str(decision_log_path), "error": str(exc)},
                        )
                if args.debug:
                    print(f"counters={summary['counters']}")

            if args.once:
                break
            await asyncio.sleep(args.interval_seconds)
    finally:
        try:
            await edge_sub.unsubscribe()
        except Exception as unsub_exc:
            print(
                "opportunity_edge_unsubscribe_error type={type_name} err={err}".format(
                    type_name=type(unsub_exc).__name__,
                    err=unsub_exc,
                )
            )
        try:
            await contract_sub.unsubscribe()
        except Exception as unsub_exc:
            print(
                "opportunity_contract_unsubscribe_error type={type_name} err={err}".format(
                    type_name=type(unsub_exc).__name__,
                    err=unsub_exc,
                )
            )
        await event_sink.close()
        await nc.drain()
    return 0


async def _run_loop() -> int:
    args = _parse_args()
    settings = load_settings()
    if args.max_spot_age is None:
        args.max_spot_age = int(settings.coinbase_stale_seconds)
    if args.max_quote_age is None:
        args.max_quote_age = int(settings.coinbase_stale_seconds)
    if args.take_cooldown_seconds is None:
        args.take_cooldown_seconds = max(int(settings.no_new_entries_last_seconds), 0)
    if args.max_takes_per_tick is None:
        args.max_takes_per_tick = max(int(settings.max_open_positions), 0)
    logger = setup_logger(settings.log_path)
    decision_log_path = (
        Path(args.decision_log_path)
        if args.decision_log_path
        else settings.log_path.parent / "trader_tape.log"
    )
    decision_log_path.parent.mkdir(parents=True, exist_ok=True)
    bus_url = (
        args.events_bus_url
        if args.events_bus_url
        else (
            settings.bus_url if (args.events_bus or args.state_source == "events") else None
        )
    )
    if args.state_source == "events":
        if not bus_url:
            raise RuntimeError("BUS_URL is required when --state-source events")
        return await _run_events_mode(
            args=args,
            settings=settings,
            logger=logger,
            decision_log_path=decision_log_path,
            bus_url=bus_url,
        )
    return await _run_sqlite_mode(
        args=args,
        settings=settings,
        logger=logger,
        decision_log_path=decision_log_path,
        bus_url=bus_url,
    )


def main() -> int:
    return asyncio.run(_run_loop())


if __name__ == "__main__":
    raise SystemExit(main())
