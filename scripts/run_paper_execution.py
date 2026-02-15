from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kalshi_bot.config import load_settings
from kalshi_bot.events import (
    ContractUpdateEvent,
    EventPublisher,
    ExecutionFillEvent,
    ExecutionFillPayload,
    ExecutionOrderEvent,
    ExecutionOrderPayload,
    OpportunityDecisionEvent,
    connect_jetstream,
    parse_event_dict,
)

EVENTS_QUEUE_MAX_DEFAULT = 50000


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Paper execution service for opportunity_decision events."
    )
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--log-every-seconds", type=float, default=10.0)
    parser.add_argument("--max-open-positions", type=int, default=None)
    parser.add_argument("--take-cooldown-seconds", type=int, default=None)
    parser.add_argument(
        "--kill-switch-path",
        type=str,
        default="data/kill_switch",
        help="If this file exists, new entries are rejected.",
    )
    parser.add_argument(
        "--events-jsonl-path",
        type=str,
        default=None,
        help="Optional JSONL sink path for emitted execution events.",
    )
    parser.add_argument(
        "--events-bus-url",
        type=str,
        default=None,
        help="JetStream URL override (default BUS_URL).",
    )
    parser.add_argument(
        "--events-opportunity-subject",
        type=str,
        default="strategy.opportunity_decisions",
    )
    parser.add_argument(
        "--events-contract-subject",
        type=str,
        default="market.contract_updates",
    )
    parser.add_argument(
        "--events-fetch-batch",
        type=int,
        default=500,
        help="Maximum queued messages to apply per cycle.",
    )
    parser.add_argument(
        "--events-queue-max",
        type=int,
        default=EVENTS_QUEUE_MAX_DEFAULT,
        help="Maximum in-memory queue size per subscribed subject.",
    )
    parser.add_argument("--debug", action="store_true")
    parser.add_argument(
        "--alert-reject-rate-threshold",
        type=float,
        default=0.50,
        help="Alert when window reject-rate exceeds this fraction.",
    )
    parser.add_argument(
        "--alert-reject-rate-min-orders",
        type=int,
        default=10,
        help="Minimum processed orders in the window before reject-rate alerting.",
    )
    parser.add_argument(
        "--alert-cooldown-seconds",
        type=float,
        default=300.0,
        help="Minimum seconds between repeated reject-rate alerts.",
    )
    return parser.parse_args()


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _price_from_decision_payload(payload: Any) -> float | None:
    side = str(payload.side or "")
    if side == "YES":
        price = _safe_float(payload.best_yes_ask)
    elif side == "NO":
        price = _safe_float(payload.best_no_ask)
    else:
        price = None
    if price is not None:
        return price
    raw_json = payload.raw_json
    if not isinstance(raw_json, str) or not raw_json:
        return None
    try:
        metadata = json.loads(raw_json)
    except json.JSONDecodeError:
        return None
    if not isinstance(metadata, dict):
        return None
    return _safe_float(metadata.get("price_used_cents"))


@dataclass(slots=True)
class _OpenPosition:
    market_id: str
    side: str
    ts_open: int
    quantity: int = 1


@dataclass(slots=True)
class _Counters:
    processed: int = 0
    accepted: int = 0
    rejected: int = 0
    duplicate_decisions: int = 0
    non_take_decisions: int = 0
    parse_errors: int = 0
    event_publish_failures: int = 0
    position_closed: int = 0
    dropped_opportunity_events: int = 0
    dropped_contract_events: int = 0


class _PaperExecutionState:
    def __init__(self) -> None:
        self.open_positions: dict[str, _OpenPosition] = {}
        self._recent_takes: deque[tuple[int, tuple[str, str]]] = deque()
        self._seen_decisions: set[str] = set()

    def _prune_recent(self, *, now_ts: int, cooldown_seconds: int) -> None:
        if cooldown_seconds <= 0:
            self._recent_takes.clear()
            return
        cutoff = now_ts - cooldown_seconds
        while self._recent_takes and self._recent_takes[0][0] < cutoff:
            self._recent_takes.popleft()

    def has_seen_decision(self, key: str) -> bool:
        return key in self._seen_decisions

    def mark_seen_decision(self, key: str) -> None:
        self._seen_decisions.add(key)

    def reject_reason(
        self,
        *,
        market_id: str,
        side: str,
        now_ts: int,
        cooldown_seconds: int,
        max_open_positions: int,
        kill_switch_active: bool,
    ) -> str | None:
        if kill_switch_active:
            return "kill_switch_active"
        if side not in {"YES", "NO"}:
            return "missing_side"

        open_pos = self.open_positions.get(market_id)
        if open_pos is not None:
            if open_pos.side == side:
                return "position_open"
            return "position_open_opposite_side"

        self._prune_recent(now_ts=now_ts, cooldown_seconds=cooldown_seconds)
        if cooldown_seconds > 0:
            key = (market_id, side)
            if any(existing == key for _, existing in self._recent_takes):
                return "cooldown_active"

        if max_open_positions > 0 and len(self.open_positions) >= max_open_positions:
            return "max_open_positions"
        return None

    def accept(self, *, market_id: str, side: str, ts_open: int) -> None:
        self.open_positions[market_id] = _OpenPosition(
            market_id=market_id,
            side=side,
            ts_open=ts_open,
            quantity=1,
        )
        self._recent_takes.append((ts_open, (market_id, side)))

    def close_market(self, market_id: str) -> _OpenPosition | None:
        return self.open_positions.pop(market_id, None)


def _decision_key(event: OpportunityDecisionEvent) -> str:
    if event.idempotency_key:
        return str(event.idempotency_key)
    payload = event.payload
    return (
        f"{payload.ts_eval}:{payload.market_id}:"
        f"{payload.side or 'NA'}:{payload.strategy_version or 'v1'}"
    )


async def _publish_order(
    *,
    event_sink: EventPublisher,
    ts_order: int,
    order_id: str,
    opportunity: OpportunityDecisionEvent,
    status: str,
    reason: str | None,
) -> None:
    payload = opportunity.payload
    await event_sink.publish(
        ExecutionOrderEvent(
            source="run_paper_execution",
            payload=ExecutionOrderPayload(
                ts_order=ts_order,
                order_id=order_id,
                market_id=payload.market_id,
                side=str(payload.side or ""),
                action="open",
                quantity=1,
                price_cents=_price_from_decision_payload(payload),
                status=status,
                reason=reason,
                opportunity_idempotency_key=opportunity.idempotency_key,
                paper=True,
            ),
        )
    )


async def _publish_open_fill(
    *,
    event_sink: EventPublisher,
    ts_fill: int,
    fill_id: str,
    order_id: str,
    opportunity: OpportunityDecisionEvent,
) -> None:
    payload = opportunity.payload
    await event_sink.publish(
        ExecutionFillEvent(
            source="run_paper_execution",
            payload=ExecutionFillPayload(
                ts_fill=ts_fill,
                fill_id=fill_id,
                order_id=order_id,
                market_id=payload.market_id,
                side=str(payload.side or ""),
                action="open",
                quantity=1,
                price_cents=_price_from_decision_payload(payload),
                outcome=None,
                reason=None,
                paper=True,
            ),
        )
    )


async def _publish_close_fill(
    *,
    event_sink: EventPublisher,
    ts_fill: int,
    market_id: str,
    side: str,
    outcome: int | None,
    reason: str,
) -> None:
    price_cents: float | None = None
    if outcome in (0, 1):
        if side == "YES":
            price_cents = 100.0 if int(outcome) == 1 else 0.0
        elif side == "NO":
            price_cents = 100.0 if int(outcome) == 0 else 0.0
    await event_sink.publish(
        ExecutionFillEvent(
            source="run_paper_execution",
            payload=ExecutionFillPayload(
                ts_fill=ts_fill,
                fill_id=f"settle:{market_id}:{side}:{ts_fill}",
                order_id=f"settle:{market_id}:{side}",
                market_id=market_id,
                side=side,
                action="close",
                quantity=1,
                price_cents=price_cents,
                outcome=outcome,
                reason=reason,
                paper=True,
            ),
        )
    )


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()

    bus_url = args.events_bus_url or settings.bus_url
    if not bus_url:
        raise RuntimeError("BUS_URL is required for paper execution service")

    max_open_positions = (
        max(int(args.max_open_positions), 0)
        if args.max_open_positions is not None
        else max(int(settings.max_open_positions), 0)
    )
    take_cooldown_seconds = (
        max(int(args.take_cooldown_seconds), 0)
        if args.take_cooldown_seconds is not None
        else max(int(settings.no_new_entries_last_seconds), 0)
    )
    kill_switch_path = Path(args.kill_switch_path)

    print("state_source=events")
    print(f"event_bus={bus_url}")
    print("execution_mode=paper")
    print(
        "risk_controls=max_open_positions:{max_open} cooldown_s:{cooldown} "
        "kill_switch_path:{path}".format(
            max_open=max_open_positions,
            cooldown=take_cooldown_seconds,
            path=kill_switch_path,
        )
    )

    nc, _ = await connect_jetstream(bus_url)
    event_sink = await EventPublisher.create(
        jsonl_path=args.events_jsonl_path,
        bus_url=bus_url,
    )
    opportunity_queue: asyncio.Queue[bytes] = asyncio.Queue(
        maxsize=max(int(args.events_queue_max), 1000)
    )
    contract_queue: asyncio.Queue[bytes] = asyncio.Queue(
        maxsize=max(int(args.events_queue_max // 4), 1000)
    )

    counters = _Counters()
    state = _PaperExecutionState()
    last_log_ts = time.monotonic()
    last_heartbeat_log_ts = last_log_ts
    last_log_snapshot: tuple[int, ...] | None = None
    last_reject_alert_log_ts = 0.0
    last_alert_baseline_processed = 0
    last_alert_baseline_rejected = 0

    async def _on_opportunity(msg: Any) -> None:
        try:
            opportunity_queue.put_nowait(msg.data)
        except asyncio.QueueFull:
            counters.dropped_opportunity_events += 1

    async def _on_contract(msg: Any) -> None:
        try:
            contract_queue.put_nowait(msg.data)
        except asyncio.QueueFull:
            counters.dropped_contract_events += 1

    opportunity_sub = await nc.subscribe(
        args.events_opportunity_subject,
        cb=_on_opportunity,
    )
    contract_sub = await nc.subscribe(
        args.events_contract_subject,
        cb=_on_contract,
    )
    print("execution_consumer_mode=core_nats")

    try:
        while True:
            cycle_count = max(int(args.events_fetch_batch), 1)
            for _ in range(cycle_count):
                msg_data: bytes | None = None
                is_contract = False
                try:
                    msg_data = opportunity_queue.get_nowait()
                except asyncio.QueueEmpty:
                    try:
                        msg_data = contract_queue.get_nowait()
                        is_contract = True
                    except asyncio.QueueEmpty:
                        break

                try:
                    raw = json.loads(msg_data.decode("utf-8"))
                    event = parse_event_dict(raw)
                except Exception:
                    counters.parse_errors += 1
                    continue

                if isinstance(event, ContractUpdateEvent):
                    contract_payload = event.payload
                    if (
                        contract_payload.outcome is None
                        and contract_payload.settled_ts is None
                    ):
                        continue
                    position = state.close_market(str(contract_payload.ticker))
                    if position is None:
                        continue
                    try:
                        await _publish_close_fill(
                            event_sink=event_sink,
                            ts_fill=int(contract_payload.settled_ts or time.time()),
                            market_id=position.market_id,
                            side=position.side,
                            outcome=_safe_int(contract_payload.outcome),
                            reason="settled",
                        )
                    except Exception:
                        counters.event_publish_failures += 1
                    counters.position_closed += 1
                    continue

                if not isinstance(event, OpportunityDecisionEvent):
                    continue
                if is_contract:
                    continue
                opportunity_payload = event.payload
                if not opportunity_payload.would_trade:
                    counters.non_take_decisions += 1
                    continue
                if not opportunity_payload.eligible:
                    counters.non_take_decisions += 1
                    continue

                counters.processed += 1
                decision_key = _decision_key(event)
                if state.has_seen_decision(decision_key):
                    counters.duplicate_decisions += 1
                    continue

                now_ts = int(time.time())
                market_id = str(opportunity_payload.market_id)
                side = str(opportunity_payload.side or "")
                reject_reason = state.reject_reason(
                    market_id=market_id,
                    side=side,
                    now_ts=now_ts,
                    cooldown_seconds=take_cooldown_seconds,
                    max_open_positions=max_open_positions,
                    kill_switch_active=kill_switch_path.exists(),
                )
                order_id = f"paper:{decision_key}"
                if reject_reason is not None:
                    try:
                        await _publish_order(
                            event_sink=event_sink,
                            ts_order=now_ts,
                            order_id=order_id,
                            opportunity=event,
                            status="rejected",
                            reason=reject_reason,
                        )
                    except Exception:
                        counters.event_publish_failures += 1
                        continue
                    state.mark_seen_decision(decision_key)
                    counters.rejected += 1
                    continue

                fill_id = f"{order_id}:open"
                try:
                    await _publish_order(
                        event_sink=event_sink,
                        ts_order=now_ts,
                        order_id=order_id,
                        opportunity=event,
                        status="filled",
                        reason=None,
                    )
                    await _publish_open_fill(
                        event_sink=event_sink,
                        ts_fill=now_ts,
                        fill_id=fill_id,
                        order_id=order_id,
                        opportunity=event,
                    )
                except Exception:
                    counters.event_publish_failures += 1
                    continue

                state.accept(market_id=market_id, side=side, ts_open=now_ts)
                state.mark_seen_decision(decision_key)
                counters.accepted += 1

            now = time.monotonic()
            if (now - last_log_ts) >= max(float(args.log_every_seconds), 0.1):
                snapshot = (
                    counters.processed,
                    counters.accepted,
                    counters.rejected,
                    counters.duplicate_decisions,
                    counters.non_take_decisions,
                    counters.parse_errors,
                    counters.event_publish_failures,
                    len(state.open_positions),
                    counters.position_closed,
                    opportunity_queue.qsize(),
                    contract_queue.qsize(),
                    counters.dropped_opportunity_events,
                    counters.dropped_contract_events,
                )
                heartbeat_seconds = max(float(args.log_every_seconds) * 6.0, 60.0)
                should_log = (
                    snapshot != last_log_snapshot
                    or (now - last_heartbeat_log_ts) >= heartbeat_seconds
                )
                if should_log:
                    print(
                        "processed={processed} accepted={accepted} rejected={rejected} "
                        "duplicates={duplicates} non_take={non_take} parse_errors={parse_errors} "
                        "publish_failures={publish_failures} open_positions={open_positions} "
                        "closed_positions={closed_positions} opportunity_q={op_q} contract_q={ct_q} "
                        "dropped_opportunity={dropped_op} dropped_contract={dropped_ct}".format(
                            processed=counters.processed,
                            accepted=counters.accepted,
                            rejected=counters.rejected,
                            duplicates=counters.duplicate_decisions,
                            non_take=counters.non_take_decisions,
                            parse_errors=counters.parse_errors,
                            publish_failures=counters.event_publish_failures,
                            open_positions=len(state.open_positions),
                            closed_positions=counters.position_closed,
                            op_q=opportunity_queue.qsize(),
                            ct_q=contract_queue.qsize(),
                            dropped_op=counters.dropped_opportunity_events,
                            dropped_ct=counters.dropped_contract_events,
                        )
                    )
                    last_log_snapshot = snapshot
                    last_heartbeat_log_ts = now
                threshold = float(args.alert_reject_rate_threshold)
                min_orders = max(int(args.alert_reject_rate_min_orders), 1)
                cooldown = max(float(args.alert_cooldown_seconds), 1.0)
                if threshold >= 0.0:
                    window_processed = (
                        counters.processed - last_alert_baseline_processed
                    )
                    window_rejected = counters.rejected - last_alert_baseline_rejected
                    reject_rate = (
                        (window_rejected / window_processed)
                        if window_processed > 0
                        else None
                    )
                    if (
                        reject_rate is not None
                        and window_processed >= min_orders
                        and reject_rate > threshold
                        and (now - last_reject_alert_log_ts) >= cooldown
                    ):
                        print(
                            "ALERT high_reject_rate rate={rate:.3f} "
                            "processed={processed} rejected={rejected} threshold={threshold:.3f}".format(
                                rate=reject_rate,
                                processed=window_processed,
                                rejected=window_rejected,
                                threshold=threshold,
                            )
                        )
                        last_reject_alert_log_ts = now
                        last_alert_baseline_processed = counters.processed
                        last_alert_baseline_rejected = counters.rejected
                last_log_ts = now

            if args.once:
                break
            await asyncio.sleep(max(float(args.interval_seconds), 0.01))
    finally:
        try:
            await opportunity_sub.unsubscribe()
        except Exception as unsub_exc:
            print(
                "paper_execution_opportunity_unsubscribe_error type={type_name} err={err}".format(
                    type_name=type(unsub_exc).__name__,
                    err=unsub_exc,
                )
            )
        try:
            await contract_sub.unsubscribe()
        except Exception as unsub_exc:
            print(
                "paper_execution_contract_unsubscribe_error type={type_name} err={err}".format(
                    type_name=type(unsub_exc).__name__,
                    err=unsub_exc,
                )
            )
        await event_sink.close()
        await nc.drain()

    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
