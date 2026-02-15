from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.kalshi.fees import taker_fee_dollars
from kalshi_bot.persistence import PostgresEventRepository


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report currently open shadow positions from TAKE signals."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of open positions to print.",
    )
    parser.add_argument(
        "--settled-limit",
        type=int,
        default=30,
        help="Maximum number of settled positions to print.",
    )
    parser.add_argument(
        "--color",
        choices=["auto", "always", "never"],
        default="auto",
        help="Colorize output (default: auto).",
    )
    parser.add_argument(
        "--pg-dsn",
        type=str,
        default=None,
        help="Use Postgres source when provided (defaults to PG_DSN).",
    )
    return parser.parse_args()


EST = timezone(timedelta(hours=-5), name="EST")


def _fmt_ts(ts: int | None) -> str:
    if ts is None:
        return "NA"
    try:
        return datetime.fromtimestamp(ts, tz=EST).strftime("%Y-%m-%dT%H:%M:%S EST")
    except (TypeError, ValueError, OSError):
        return "NA"


def _price_used_cents(raw_json: str | None) -> float | None:
    if not raw_json:
        return None
    try:
        payload = json.loads(raw_json)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    value = payload.get("price_used_cents")
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _fmt_cents(value: float | None) -> str:
    if value is None:
        return "NA"
    return f"{value:.2f}"


def _fmt_pnl(value: float | None) -> str:
    if value is None:
        return "NA"
    return f"{value:.4f}"


def _use_color(mode: str) -> bool:
    if mode == "always":
        return True
    if mode == "never":
        return False
    return sys.stdout.isatty() and not os.environ.get("NO_COLOR")


def _paint(text: str, code: str, enabled: bool) -> str:
    if not enabled:
        return text
    return f"\x1b[{code}m{text}\x1b[0m"


def _paint_pnl(value: float | None, enabled: bool, width: int = 8) -> str:
    text = _fmt_pnl(value).rjust(width)
    if value is None or not enabled:
        return text
    if value > 0:
        return _paint(text, "32", True)
    if value < 0:
        return _paint(text, "31", True)
    return _paint(text, "33", True)


async def _load_open_rows(conn: aiosqlite.Connection) -> list[tuple[Any, ...]]:
    cursor = await conn.execute(
        """
        WITH latest_quotes AS (
            SELECT q.market_id, q.ts AS quote_ts, q.yes_bid, q.yes_ask,
                   q.no_bid, q.no_ask, q.yes_mid, q.no_mid
            FROM kalshi_quotes q
            JOIN (
                SELECT market_id, MAX(ts) AS max_ts
                FROM kalshi_quotes
                GROUP BY market_id
            ) latest
              ON latest.market_id = q.market_id AND latest.max_ts = q.ts
        )
        SELECT o.market_id, o.side, o.ts_eval, o.settlement_ts,
               o.best_yes_ask, o.best_no_ask, o.raw_json,
               o.ev_raw, o.p_model, o.p_market,
               c.outcome, c.settled_ts,
               lq.quote_ts, lq.yes_bid, lq.yes_ask, lq.no_bid, lq.no_ask,
               lq.yes_mid, lq.no_mid
        FROM opportunities o
        LEFT JOIN kalshi_contracts c
          ON c.ticker = o.market_id
        LEFT JOIN latest_quotes lq
          ON lq.market_id = o.market_id
        WHERE o.would_trade = 1
          AND (c.outcome IS NULL)
        ORDER BY o.ts_eval ASC
        """
    )
    return [tuple(row) for row in await cursor.fetchall()]


async def _load_settled_rows(conn: aiosqlite.Connection) -> list[tuple[Any, ...]]:
    cursor = await conn.execute(
        """
        SELECT o.market_id, o.side, o.ts_eval, o.settlement_ts,
               o.best_yes_ask, o.best_no_ask, o.raw_json,
               o.ev_raw, o.p_model, o.p_market,
               c.outcome, c.settled_ts
        FROM opportunities o
        LEFT JOIN kalshi_contracts c
          ON c.ticker = o.market_id
        WHERE o.would_trade = 1
          AND c.outcome IN (0, 1)
        ORDER BY COALESCE(c.settled_ts, o.settlement_ts, o.ts_eval) DESC, o.ts_eval DESC
        """
    )
    return [tuple(row) for row in await cursor.fetchall()]


def _load_open_rows_postgres(conn: Any) -> list[tuple[Any, ...]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH latest_fill AS (
                SELECT DISTINCT ON (f.market_id)
                    f.market_id,
                    f.order_id,
                    f.side,
                    f.ts_fill,
                    f.price_cents,
                    f.action
                FROM event_store.execution_fill_latest f
                WHERE f.paper = TRUE
                ORDER BY f.market_id, f.ts_fill DESC
            )
            SELECT
                lf.market_id,
                lf.side,
                lf.ts_fill AS ts_eval,
                COALESCE(c.close_ts, c.expected_expiration_ts, c.expiration_ts, c.settled_ts) AS settlement_ts,
                CASE WHEN lf.side = 'YES' THEN lf.price_cents ELSE NULL END AS best_yes_ask,
                CASE WHEN lf.side = 'NO' THEN lf.price_cents ELSE NULL END AS best_no_ask,
                NULL::TEXT AS raw_json,
                NULLIF(er.payload_json->>'ev_raw', '')::DOUBLE PRECISION AS ev_raw,
                NULLIF(er.payload_json->>'p_model', '')::DOUBLE PRECISION AS p_model,
                NULLIF(er.payload_json->>'p_market', '')::DOUBLE PRECISION AS p_market,
                c.outcome,
                c.settled_ts,
                q.ts AS quote_ts,
                q.yes_bid,
                q.yes_ask,
                q.no_bid,
                q.no_ask,
                q.yes_mid,
                q.no_mid
            FROM latest_fill lf
            LEFT JOIN event_store.execution_order_latest eo
              ON eo.order_id = lf.order_id
            LEFT JOIN event_store.events_raw er
              ON er.event_type = 'opportunity_decision'
             AND er.idempotency_key = eo.opportunity_idempotency_key
            LEFT JOIN event_store.state_contract_latest c
              ON c.ticker = lf.market_id
            LEFT JOIN event_store.state_quote_latest q
              ON q.market_id = lf.market_id
            WHERE lf.action = 'open'
              AND c.outcome IS NULL
            ORDER BY lf.ts_fill ASC
            """
        )
        return cur.fetchall()


def _load_settled_rows_postgres(conn: Any) -> list[tuple[Any, ...]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                f.market_id,
                f.side,
                f.ts_fill AS ts_eval,
                COALESCE(c.close_ts, c.expected_expiration_ts, c.expiration_ts, c.settled_ts) AS settlement_ts,
                CASE WHEN f.side = 'YES' THEN f.price_cents ELSE NULL END AS best_yes_ask,
                CASE WHEN f.side = 'NO' THEN f.price_cents ELSE NULL END AS best_no_ask,
                NULL::TEXT AS raw_json,
                NULLIF(er.payload_json->>'ev_raw', '')::DOUBLE PRECISION AS ev_raw,
                NULLIF(er.payload_json->>'p_model', '')::DOUBLE PRECISION AS p_model,
                NULLIF(er.payload_json->>'p_market', '')::DOUBLE PRECISION AS p_market,
                c.outcome,
                c.settled_ts
            FROM event_store.execution_fill_latest f
            LEFT JOIN event_store.execution_order_latest eo
              ON eo.order_id = f.order_id
            LEFT JOIN event_store.events_raw er
              ON er.event_type = 'opportunity_decision'
             AND er.idempotency_key = eo.opportunity_idempotency_key
            JOIN event_store.state_contract_latest c
              ON c.ticker = f.market_id
            WHERE f.paper = TRUE
              AND f.action = 'open'
              AND c.outcome IN (0, 1)
            ORDER BY COALESCE(c.settled_ts, c.expiration_ts, c.close_ts, f.ts_fill) DESC,
                     f.ts_fill DESC
            """
        )
        return cur.fetchall()


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()
    color = _use_color(args.color)
    rows: list[tuple[Any, ...]]
    settled_rows: list[tuple[Any, ...]]
    pg_dsn = args.pg_dsn or settings.pg_dsn
    now_ts = int(time.time())
    if pg_dsn:
        print("backend=postgres")
        try:
            repo = PostgresEventRepository(pg_dsn)
            repo.ensure_schema()
            repo.close()
            try:
                import psycopg
            except ModuleNotFoundError as exc:
                raise RuntimeError(
                    "psycopg is required for Postgres open-position reporting. "
                    "Install with: pip install psycopg[binary]"
                ) from exc
            with psycopg.connect(pg_dsn) as pg_conn:
                rows = _load_open_rows_postgres(pg_conn)
                settled_rows = _load_settled_rows_postgres(pg_conn)
        except Exception as exc:
            print(f"ERROR: postgres report query failed: {exc}")
            return 1
    else:
        print("backend=sqlite")
        print(f"DB path: {settings.db_path}")
        db_path = Path(settings.db_path).resolve()
        db_uri = f"file:{db_path}?mode=ro"
        try:
            async with aiosqlite.connect(db_uri, uri=True) as sqlite_conn:
                await sqlite_conn.execute("PRAGMA foreign_keys = ON;")
                await sqlite_conn.execute("PRAGMA busy_timeout = 1000;")
                rows = await _load_open_rows(sqlite_conn)
                settled_rows = await _load_settled_rows(sqlite_conn)
        except sqlite3.OperationalError as exc:
            print(f"ERROR: could not open DB read-only: {exc}")
            return 1

    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for (
        market_id,
        side,
        ts_eval,
        settlement_ts,
        best_yes_ask,
        best_no_ask,
        raw_json,
        ev_raw,
        p_model,
        p_market,
        _outcome,
        _settled_ts,
        quote_ts,
        yes_bid,
        yes_ask,
        no_bid,
        no_ask,
        yes_mid,
        no_mid,
    ) in rows:
        if not isinstance(market_id, str) or not isinstance(side, str):
            continue
        key = (market_id, side)
        entry_price = _price_used_cents(raw_json)
        if entry_price is None:
            if side == "YES":
                entry_price = _safe_float(best_yes_ask)
            elif side == "NO":
                entry_price = _safe_float(best_no_ask)

        state = grouped.get(key)
        if state is None:
            state = {
                "market_id": market_id,
                "side": side,
                "qty": 0,
                "entry_sum_cents": 0.0,
                "entry_count": 0,
                "ev_sum": 0.0,
                "ev_count": 0,
                "p_model_sum": 0.0,
                "p_model_count": 0,
                "p_market_sum": 0.0,
                "p_market_count": 0,
                "first_ts": None,
                "last_ts": None,
                "settlement_ts": settlement_ts,
                "quote_ts": quote_ts,
                "yes_bid": _safe_float(yes_bid),
                "yes_ask": _safe_float(yes_ask),
                "no_bid": _safe_float(no_bid),
                "no_ask": _safe_float(no_ask),
                "yes_mid": _safe_float(yes_mid),
                "no_mid": _safe_float(no_mid),
            }
            grouped[key] = state

        state["qty"] += 1
        if entry_price is not None:
            state["entry_sum_cents"] += float(entry_price)
            state["entry_count"] += 1
        ev_entry = _safe_float(ev_raw)
        if ev_entry is not None:
            state["ev_sum"] += float(ev_entry)
            state["ev_count"] += 1
        p_model_entry = _safe_float(p_model)
        if p_model_entry is not None:
            state["p_model_sum"] += float(p_model_entry)
            state["p_model_count"] += 1
        p_market_entry = _safe_float(p_market)
        if p_market_entry is not None:
            state["p_market_sum"] += float(p_market_entry)
            state["p_market_count"] += 1
        eval_ts = int(ts_eval) if ts_eval is not None else None
        if eval_ts is not None:
            state["first_ts"] = eval_ts if state["first_ts"] is None else min(state["first_ts"], eval_ts)
            state["last_ts"] = eval_ts if state["last_ts"] is None else max(state["last_ts"], eval_ts)

    positions = list(grouped.values())
    positions.sort(key=lambda row: row.get("last_ts") or 0, reverse=True)
    if args.limit > 0:
        positions = positions[: args.limit]

    total_qty = sum(int(row["qty"]) for row in positions)
    mtm_total = 0.0
    mtm_count = 0
    enriched_open: list[dict[str, Any]] = []
    for row in positions:
        qty = int(row["qty"])
        avg_entry = (
            row["entry_sum_cents"] / row["entry_count"]
            if row["entry_count"] > 0
            else None
        )
        ev_entry = (
            row["ev_sum"] / row["ev_count"] if row.get("ev_count", 0) > 0 else None
        )
        p_model_entry = (
            row["p_model_sum"] / row["p_model_count"]
            if row.get("p_model_count", 0) > 0
            else None
        )
        p_market_entry = (
            row["p_market_sum"] / row["p_market_count"]
            if row.get("p_market_count", 0) > 0
            else None
        )
        if row["side"] == "YES":
            liq_price = (
                row["yes_bid"] if row["yes_bid"] is not None else row["yes_mid"]
            )
        else:
            liq_price = (
                row["no_bid"] if row["no_bid"] is not None else row["no_mid"]
            )

        unrealized_total = None
        if avg_entry is not None and liq_price is not None:
            unrealized_total = ((liq_price - avg_entry) / 100.0) * qty
            mtm_total += unrealized_total
            mtm_count += 1

        quote_age = None
        if row.get("quote_ts") is not None:
            quote_age = max(now_ts - int(row["quote_ts"]), 0)
        age_minutes = None
        if row.get("first_ts") is not None:
            age_minutes = (now_ts - int(row["first_ts"])) / 60.0

        enriched_open.append(
            {
                **row,
                "avg_entry_c": avg_entry,
                "ev_entry": ev_entry,
                "p_model_entry": p_model_entry,
                "p_market_entry": p_market_entry,
                "liq_c": liq_price,
                "u_pnl_total": unrealized_total,
                "quote_age_s": quote_age,
                "age_m": age_minutes,
            }
        )

    winners = [row for row in enriched_open if (row.get("u_pnl_total") or 0.0) > 0]
    losers = [row for row in enriched_open if (row.get("u_pnl_total") or 0.0) < 0]
    flat = [row for row in enriched_open if row.get("u_pnl_total") in (0.0, None)]
    winners.sort(key=lambda row: row.get("u_pnl_total") or 0.0, reverse=True)
    losers.sort(key=lambda row: row.get("u_pnl_total") or 0.0)
    flat.sort(key=lambda row: row.get("last_ts") or 0, reverse=True)

    print(
        "open_positions={positions} open_contracts={contracts} "
        "unrealized_pnl_total={upnl} marked_positions={marked} "
        "winners={winners} losers={losers} flat_or_na={flat}".format(
            positions=len(positions),
            contracts=total_qty,
            upnl=f"{mtm_total:.4f}" if mtm_count > 0 else "NA",
            marked=mtm_count,
            winners=len(winners),
            losers=len(losers),
            flat=len(flat),
        )
    )

    def _print_open_section(title: str, items: list[dict[str, Any]]) -> None:
        if not items:
            return
        print(_paint(f"{title}:", "1", color))
        print(
            "  {market:<31} {side:<4} {qty:>3} {upnl:>8} {entry:>7} {liq:>7} {ev:>8} {p_model:>8} {p_market:>8} {qage:>5} {age:>6} {opened:<23} {settle:<23}".format(
                market="market",
                side="side",
                qty="qty",
                upnl="uPnL",
                entry="entryC",
                liq="liqC",
                ev="evEnt",
                p_model="pModel",
                p_market="pMkt",
                qage="qAge",
                age="age_m",
                opened="opened",
                settle="settle",
            )
        )
        for row in items:
            print(
                "  {market:<31} {side:<4} {qty:>3} {upnl} {avg_entry:>7} {liq:>7} {ev_entry:>8} {p_model_entry:>8} {p_market_entry:>8} {quote_age:>5} {age_m:>6} {opened:<23} {settle:<23}".format(
                    market=str(row["market_id"])[:31],
                    side=str(row["side"]),
                    qty=int(row["qty"]),
                    upnl=_paint_pnl(_safe_float(row.get("u_pnl_total")), color),
                    avg_entry=_fmt_cents(_safe_float(row.get("avg_entry_c"))),
                    liq=_fmt_cents(_safe_float(row.get("liq_c"))),
                    ev_entry=(
                        f"{float(row['ev_entry']):.4f}"
                        if row.get("ev_entry") is not None
                        else "NA"
                    ),
                    p_model_entry=(
                        f"{float(row['p_model_entry']):.4f}"
                        if row.get("p_model_entry") is not None
                        else "NA"
                    ),
                    p_market_entry=(
                        f"{float(row['p_market_entry']):.4f}"
                        if row.get("p_market_entry") is not None
                        else "NA"
                    ),
                    quote_age=(
                        str(int(row["quote_age_s"]))
                        if row.get("quote_age_s") is not None
                        else "NA"
                    ),
                    opened=_fmt_ts(row.get("first_ts")),
                    age_m=(
                        f"{float(row['age_m']):.1f}".rjust(6)
                        if row.get("age_m") is not None
                        else "NA".rjust(6)
                    ),
                    settle=_fmt_ts(row.get("settlement_ts")),
                )
            )

    _print_open_section("open_winners", winners)
    _print_open_section("open_losers", losers)
    _print_open_section("open_flat_or_na", flat)

    settled_grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for (
        market_id,
        side,
        ts_eval,
        settlement_ts,
        best_yes_ask,
        best_no_ask,
        raw_json,
        ev_raw,
        p_model,
        p_market,
        outcome,
        settled_ts,
    ) in settled_rows:
        if not isinstance(market_id, str) or not isinstance(side, str):
            continue
        if outcome not in (0, 1):
            continue
        key = (market_id, side)
        entry_price = _price_used_cents(raw_json)
        if entry_price is None:
            if side == "YES":
                entry_price = _safe_float(best_yes_ask)
            elif side == "NO":
                entry_price = _safe_float(best_no_ask)
        realized = None
        if entry_price is not None:
            fee = taker_fee_dollars(float(entry_price), 1)
            fee_value = fee if fee is not None else 0.0
            if side == "YES":
                realized = int(outcome) - (float(entry_price) / 100.0) - fee_value
            elif side == "NO":
                realized = (
                    (1 - int(outcome)) - (float(entry_price) / 100.0) - fee_value
                )
        state = settled_grouped.get(key)
        if state is None:
            state = {
                "market_id": market_id,
                "side": side,
                "qty": 0,
                "entry_sum_cents": 0.0,
                "entry_count": 0,
                "ev_sum": 0.0,
                "ev_count": 0,
                "p_model_sum": 0.0,
                "p_model_count": 0,
                "p_market_sum": 0.0,
                "p_market_count": 0,
                "realized_total": 0.0,
                "realized_count": 0,
                "first_ts": None,
                "last_ts": None,
                "settlement_ts": settlement_ts,
                "settled_ts": settled_ts,
                "outcome": int(outcome),
            }
            settled_grouped[key] = state
        state["qty"] += 1
        if entry_price is not None:
            state["entry_sum_cents"] += float(entry_price)
            state["entry_count"] += 1
        ev_entry = _safe_float(ev_raw)
        if ev_entry is not None:
            state["ev_sum"] += float(ev_entry)
            state["ev_count"] += 1
        p_model_entry = _safe_float(p_model)
        if p_model_entry is not None:
            state["p_model_sum"] += float(p_model_entry)
            state["p_model_count"] += 1
        p_market_entry = _safe_float(p_market)
        if p_market_entry is not None:
            state["p_market_sum"] += float(p_market_entry)
            state["p_market_count"] += 1
        if realized is not None:
            state["realized_total"] += float(realized)
            state["realized_count"] += 1
        eval_ts = int(ts_eval) if ts_eval is not None else None
        if eval_ts is not None:
            state["first_ts"] = eval_ts if state["first_ts"] is None else min(state["first_ts"], eval_ts)
            state["last_ts"] = eval_ts if state["last_ts"] is None else max(state["last_ts"], eval_ts)
        if state.get("settled_ts") is None and settled_ts is not None:
            state["settled_ts"] = settled_ts

    settled_positions = list(settled_grouped.values())
    settled_positions.sort(
        key=lambda row: row.get("settled_ts") or row.get("settlement_ts") or 0,
        reverse=True,
    )
    total_settled_positions = len(settled_positions)
    if args.settled_limit > 0:
        settled_positions = settled_positions[: args.settled_limit]

    settled_total_qty = sum(int(row["qty"]) for row in settled_positions)
    settled_realized_total = 0.0
    settled_realized_count = 0
    print(
        f"settled_positions={len(settled_positions)} settled_contracts={settled_total_qty}"
    )
    print(
        "  {market:<31} {side:<4} {qty:>3} {outcome:>7} {entry:>7} {ev:>8} {p_model:>8} {p_market:>8} {realized:>8} {opened:<23} {settled:<23}".format(
            market="market",
            side="side",
            qty="qty",
            outcome="outcome",
            entry="entryC",
            ev="evEnt",
            p_model="pModel",
            p_market="pMkt",
            realized="realized",
            opened="opened",
            settled="settled",
        )
    )
    for row in settled_positions:
        qty = int(row["qty"])
        avg_entry = (
            row["entry_sum_cents"] / row["entry_count"]
            if row["entry_count"] > 0
            else None
        )
        ev_entry = (
            row["ev_sum"] / row["ev_count"] if row.get("ev_count", 0) > 0 else None
        )
        p_model_entry = (
            row["p_model_sum"] / row["p_model_count"]
            if row.get("p_model_count", 0) > 0
            else None
        )
        p_market_entry = (
            row["p_market_sum"] / row["p_market_count"]
            if row.get("p_market_count", 0) > 0
            else None
        )
        realized_total = (
            row["realized_total"] if row["realized_count"] > 0 else None
        )
        if realized_total is not None:
            settled_realized_total += float(realized_total)
            settled_realized_count += 1
        print(
            "  {market:<31} {side:<4} {qty:>3} {outcome:>7} {avg_entry:>7} {ev_entry:>8} {p_model_entry:>8} {p_market_entry:>8} {realized} {opened:<23} {settled:<23}".format(
                market=str(row["market_id"])[:31],
                side=str(row["side"]),
                qty=qty,
                outcome=row.get("outcome"),
                avg_entry=(
                    f"{avg_entry:.2f}" if avg_entry is not None else "NA"
                ),
                ev_entry=(f"{ev_entry:.4f}" if ev_entry is not None else "NA"),
                p_model_entry=(
                    f"{p_model_entry:.4f}" if p_model_entry is not None else "NA"
                ),
                p_market_entry=(
                    f"{p_market_entry:.4f}" if p_market_entry is not None else "NA"
                ),
                realized=_paint_pnl(realized_total, color),
                opened=_fmt_ts(row.get("first_ts")),
                settled=_fmt_ts(row.get("settled_ts") or row.get("settlement_ts")),
            )
        )
    if settled_realized_count > 0:
        print(
            f"settled_marked_positions={settled_realized_count} settled_realized_total={settled_realized_total:.4f}"
        )
    else:
        print("settled_marked_positions=0 settled_realized_total=NA")
    if args.settled_limit > 0 and total_settled_positions > len(settled_positions):
        print(
            "settled_output_truncated=true shown={shown} total={total} "
            "hint='use --settled-limit 0 for all or a larger number'".format(
                shown=len(settled_positions),
                total=total_settled_positions,
            )
        )

    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
