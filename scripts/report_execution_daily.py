from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from kalshi_bot.config import load_settings
from kalshi_bot.kalshi.fees import taker_fee_dollars


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report daily execution PnL and fill quality from Postgres."
    )
    parser.add_argument(
        "--since-seconds",
        type=int,
        default=24 * 3600,
        help="Window length when --day is not set.",
    )
    parser.add_argument(
        "--day",
        type=str,
        default=None,
        help="UTC date in YYYY-MM-DD format; overrides --since-seconds.",
    )
    parser.add_argument(
        "--pg-dsn",
        type=str,
        default=None,
        help="Postgres DSN override (defaults to PG_DSN).",
    )
    return parser.parse_args()


def _day_start_ts(day_text: str) -> int:
    dt = datetime.strptime(day_text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _window(args: argparse.Namespace, now_ts: int) -> tuple[int, int, str]:
    if args.day:
        start_ts = _day_start_ts(args.day)
        end_ts = int((datetime.fromtimestamp(start_ts, tz=timezone.utc) + timedelta(days=1)).timestamp())
        label = f"day={args.day} (UTC)"
        return start_ts, end_ts, label
    start_ts = now_ts - max(int(args.since_seconds), 1)
    return start_ts, now_ts, f"since_seconds={int(args.since_seconds)}"


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _realized_pnl(outcome: int, side: str, entry_price_cents: float) -> float:
    fee = taker_fee_dollars(entry_price_cents, 1) or 0.0
    entry = entry_price_cents / 100.0
    if side == "YES":
        return outcome - entry - fee
    return (1 - outcome) - entry - fee


def _fmt_dt(ts: int | None) -> str:
    if ts is None:
        return "NA"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _compute_report(conn: Any, *, start_ts: int, end_ts: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ts_order, status, reason, market_id, side, action, quantity, price_cents
            FROM event_store.execution_order_latest
            WHERE paper = TRUE
              AND ts_order >= %s
              AND ts_order < %s
            ORDER BY ts_order ASC
            """,
            (start_ts, end_ts),
        )
        orders = cur.fetchall()

        cur.execute(
            """
            SELECT ts_fill, order_id, market_id, side, action, quantity, price_cents, outcome, reason
            FROM event_store.execution_fill_latest
            WHERE paper = TRUE
              AND ts_fill >= %s
              AND ts_fill < %s
            ORDER BY ts_fill ASC
            """,
            (start_ts, end_ts),
        )
        fills = cur.fetchall()

        cur.execute(
            """
            SELECT
                f.market_id,
                f.side,
                f.ts_fill AS ts_open,
                f.price_cents AS entry_cents,
                COALESCE(c.settled_ts, c.expiration_ts, c.close_ts) AS settled_ts,
                c.outcome
            FROM event_store.execution_fill_latest f
            JOIN event_store.state_contract_latest c
              ON c.ticker = f.market_id
            WHERE f.paper = TRUE
              AND f.action = 'open'
              AND c.outcome IN (0, 1)
              AND COALESCE(c.settled_ts, c.expiration_ts, c.close_ts) >= %s
              AND COALESCE(c.settled_ts, c.expiration_ts, c.close_ts) < %s
            ORDER BY COALESCE(c.settled_ts, c.expiration_ts, c.close_ts) ASC
            """,
            (start_ts, end_ts),
        )
        settled = cur.fetchall()

        cur.execute(
            """
            SELECT
                o.order_id,
                o.price_cents AS order_price_cents,
                f.price_cents AS fill_price_cents
            FROM event_store.execution_order_latest o
            LEFT JOIN event_store.execution_fill_latest f
              ON f.order_id = o.order_id AND f.action = 'open' AND f.paper = TRUE
            WHERE o.paper = TRUE
              AND o.status = 'filled'
              AND o.ts_order >= %s
              AND o.ts_order < %s
            """,
            (start_ts, end_ts),
        )
        filled_pairs = cur.fetchall()

        cur.execute(
            """
            SELECT COUNT(*)
            FROM event_store.execution_fill_latest f
            LEFT JOIN event_store.state_contract_latest c
              ON c.ticker = f.market_id
            WHERE f.paper = TRUE
              AND f.action = 'open'
              AND c.outcome IS NULL
            """
        )
        open_positions = int(cur.fetchone()[0] or 0)

    total_orders = len(orders)
    rejected_orders = sum(1 for row in orders if str(row[1]) == "rejected")
    filled_orders = sum(1 for row in orders if str(row[1]) == "filled")
    reject_rate = (rejected_orders / total_orders) if total_orders > 0 else None
    reject_reasons: dict[str, int] = defaultdict(int)
    for row in orders:
        if str(row[1]) != "rejected":
            continue
        reason = str(row[2] or "unknown")
        reject_reasons[reason] += 1

    open_fills = sum(1 for row in fills if str(row[4]) == "open")
    close_fills = sum(1 for row in fills if str(row[4]) == "close")

    realized_total = 0.0
    realized_count = 0
    win_count = 0
    hold_minutes_sum = 0.0
    hold_minutes_count = 0
    by_day: dict[str, dict[str, float]] = defaultdict(lambda: {"count": 0.0, "pnl": 0.0, "wins": 0.0})
    for market_id, side, ts_open, entry_cents, settled_ts, outcome in settled:
        outcome_i = int(outcome)
        entry = _safe_float(entry_cents)
        if side not in {"YES", "NO"} or entry is None:
            continue
        pnl = _realized_pnl(outcome_i, side, entry)
        realized_total += pnl
        realized_count += 1
        if pnl > 0:
            win_count += 1
        if ts_open is not None and settled_ts is not None:
            hold_minutes_sum += max(int(settled_ts) - int(ts_open), 0) / 60.0
            hold_minutes_count += 1
        day_key = datetime.fromtimestamp(int(settled_ts), tz=timezone.utc).strftime("%Y-%m-%d")
        by_day[day_key]["count"] += 1
        by_day[day_key]["pnl"] += pnl
        if pnl > 0:
            by_day[day_key]["wins"] += 1

    slippage_abs_sum = 0.0
    slippage_count = 0
    missing_open_fill_for_filled_order = 0
    for _, order_price_cents, fill_price_cents in filled_pairs:
        order_px = _safe_float(order_price_cents)
        fill_px = _safe_float(fill_price_cents)
        if fill_px is None:
            missing_open_fill_for_filled_order += 1
            continue
        if order_px is None:
            continue
        slippage_abs_sum += abs(fill_px - order_px)
        slippage_count += 1
    avg_abs_slippage = (
        slippage_abs_sum / slippage_count if slippage_count > 0 else None
    )

    print("backend=postgres")
    print(
        "window_start={start} window_end={end}".format(
            start=_fmt_dt(start_ts),
            end=_fmt_dt(end_ts),
        )
    )
    print(
        "orders_total={total} filled={filled} rejected={rejected} reject_rate={rate}".format(
            total=total_orders,
            filled=filled_orders,
            rejected=rejected_orders,
            rate=f"{reject_rate:.3f}" if reject_rate is not None else "NA",
        )
    )
    if reject_reasons:
        print("reject_reasons:")
        for reason, count in sorted(reject_reasons.items(), key=lambda item: item[1], reverse=True):
            print(f"  {reason}: {count}")

    print(
        "fills_open={opens} fills_close={closes} open_positions_now={open_now}".format(
            opens=open_fills,
            closes=close_fills,
            open_now=open_positions,
        )
    )
    print(
        "fill_quality avg_abs_slippage_cents={slippage} "
        "missing_open_fill_for_filled_order={missing}".format(
            slippage=f"{avg_abs_slippage:.3f}" if avg_abs_slippage is not None else "NA",
            missing=missing_open_fill_for_filled_order,
        )
    )
    print(
        "settled_positions={count} settled_realized_pnl={pnl} win_rate={win_rate} "
        "avg_hold_minutes={hold}".format(
            count=realized_count,
            pnl=f"{realized_total:.4f}",
            win_rate=(f"{(win_count / realized_count):.3f}" if realized_count > 0 else "NA"),
            hold=(f"{(hold_minutes_sum / hold_minutes_count):.1f}" if hold_minutes_count > 0 else "NA"),
        )
    )
    if by_day:
        print("settled_by_day:")
        for day in sorted(by_day.keys()):
            info = by_day[day]
            wins = int(info["wins"])
            count = int(info["count"])
            print(
                "  {day} count={count} pnl={pnl:.4f} wins={wins}".format(
                    day=day,
                    count=count,
                    pnl=float(info["pnl"]),
                    wins=wins,
                )
            )


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()
    pg_dsn = args.pg_dsn or settings.pg_dsn
    if not pg_dsn:
        print("ERROR: PG_DSN is required")
        return 2
    now_ts = int(datetime.now(tz=timezone.utc).timestamp())
    start_ts, end_ts, label = _window(args, now_ts)
    print(f"window={label}")

    try:
        try:
            import psycopg
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "psycopg is required for Postgres execution reporting. "
                "Install with: pip install psycopg[binary]"
            ) from exc
        with psycopg.connect(pg_dsn) as conn:
            _compute_report(conn, start_ts=start_ts, end_ts=end_ts)
    except Exception as exc:
        print(f"ERROR: postgres execution report failed: {exc}")
        return 1

    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
