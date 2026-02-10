from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.kalshi.fees import taker_fee_dollars


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
        default=100,
        help="Maximum number of settled positions to print.",
    )
    return parser.parse_args()


def _fmt_ts(ts: int | None) -> str:
    if ts is None:
        return "NA"
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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
    return await cursor.fetchall()


async def _load_settled_rows(conn: aiosqlite.Connection) -> list[tuple[Any, ...]]:
    cursor = await conn.execute(
        """
        SELECT o.market_id, o.side, o.ts_eval, o.settlement_ts,
               o.best_yes_ask, o.best_no_ask, o.raw_json,
               c.outcome, c.settled_ts
        FROM opportunities o
        LEFT JOIN kalshi_contracts c
          ON c.ticker = o.market_id
        WHERE o.would_trade = 1
          AND c.outcome IN (0, 1)
        ORDER BY COALESCE(c.settled_ts, o.settlement_ts, o.ts_eval) DESC, o.ts_eval DESC
        """
    )
    return await cursor.fetchall()


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()

    print(f"DB path: {settings.db_path}")
    db_path = Path(settings.db_path).resolve()
    db_uri = f"file:{db_path}?mode=ro"

    now_ts = int(time.time())
    try:
        async with aiosqlite.connect(db_uri, uri=True) as conn:
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.execute("PRAGMA busy_timeout = 1000;")
            rows = await _load_open_rows(conn)
            settled_rows = await _load_settled_rows(conn)
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
        print(f"{title}:")
        for row in items:
            print(
                "  market={market} side={side} qty={qty} uPnL_total={upnl} "
                "avg_entry_c={avg_entry} liq_c={liq} quote_age_s={quote_age} "
                "opened={opened} age_m={age_m} settle={settle}".format(
                    market=row["market_id"],
                    side=row["side"],
                    qty=int(row["qty"]),
                    upnl=_fmt_pnl(_safe_float(row.get("u_pnl_total"))),
                    avg_entry=_fmt_cents(_safe_float(row.get("avg_entry_c"))),
                    liq=_fmt_cents(_safe_float(row.get("liq_c"))),
                    quote_age=(
                        str(int(row["quote_age_s"]))
                        if row.get("quote_age_s") is not None
                        else "NA"
                    ),
                    opened=_fmt_ts(row.get("first_ts")),
                    age_m=(
                        f"{float(row['age_m']):.1f}"
                        if row.get("age_m") is not None
                        else "NA"
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
    if args.settled_limit > 0:
        settled_positions = settled_positions[: args.settled_limit]

    settled_total_qty = sum(int(row["qty"]) for row in settled_positions)
    settled_realized_total = 0.0
    settled_realized_count = 0
    print(
        f"settled_positions={len(settled_positions)} settled_contracts={settled_total_qty}"
    )
    for row in settled_positions:
        qty = int(row["qty"])
        avg_entry = (
            row["entry_sum_cents"] / row["entry_count"]
            if row["entry_count"] > 0
            else None
        )
        realized_total = (
            row["realized_total"] if row["realized_count"] > 0 else None
        )
        if realized_total is not None:
            settled_realized_total += float(realized_total)
            settled_realized_count += 1
        print(
            "market={market} side={side} qty={qty} outcome={outcome} avg_entry_c={avg_entry} "
            "realized_total={realized} opened={opened} settled={settled}".format(
                market=row["market_id"],
                side=row["side"],
                qty=qty,
                outcome=row.get("outcome"),
                avg_entry=f"{avg_entry:.2f}" if avg_entry is not None else "NA",
                realized=f"{realized_total:.4f}" if realized_total is not None else "NA",
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

    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
