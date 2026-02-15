from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import aiosqlite

from kalshi_bot.app.live_stack_health import (
    collect_live_health,
    collect_live_health_postgres,
    format_live_health,
)
from kalshi_bot.config import load_settings
from kalshi_bot.data import init_db
from kalshi_bot.kalshi.btc_markets import BTC_SERIES_TICKERS
from kalshi_bot.kalshi.market_filters import normalize_db_status, normalize_series


@dataclass(frozen=True)
class Component:
    name: str
    cmd: list[str]


@dataclass(frozen=True)
class PeriodicJob:
    component: Component
    interval_seconds: int
    initial_delay_seconds: int
    align_period_seconds: int | None = None
    align_offset_seconds: float = 0.0


class ProcessRegistry:
    def __init__(self) -> None:
        self._procs: set[asyncio.subprocess.Process] = set()
        self._lock = asyncio.Lock()

    async def add(self, proc: asyncio.subprocess.Process) -> None:
        async with self._lock:
            self._procs.add(proc)

    async def discard(self, proc: asyncio.subprocess.Process) -> None:
        async with self._lock:
            self._procs.discard(proc)

    async def terminate_all(self, timeout_seconds: float = 10.0) -> None:
        async with self._lock:
            procs = list(self._procs)
        if not procs:
            return
        for proc in procs:
            if proc.returncode is None:
                proc.terminate()
        try:
            await asyncio.wait_for(
                asyncio.gather(*(proc.wait() for proc in procs), return_exceptions=True),
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError:
            for proc in procs:
                if proc.returncode is None:
                    proc.kill()
            await asyncio.gather(*(proc.wait() for proc in procs), return_exceptions=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the live data + edge + scoring stack."
    )
    parser.add_argument("--python-bin", type=str, default=sys.executable)
    parser.add_argument("--status", type=str, default="active")
    parser.add_argument("--series", action="append", default=None)
    parser.add_argument("--product-id", type=str, default=None)
    parser.add_argument("--disable-coinbase", action="store_true")
    parser.add_argument("--disable-quotes", action="store_true")
    parser.add_argument("--disable-edges", action="store_true")
    parser.add_argument("--disable-opportunities", action="store_true")
    parser.add_argument(
        "--enable-execution",
        action="store_true",
        help="Run paper execution service (Phase E).",
    )
    parser.add_argument(
        "--edge-state-source",
        type=str,
        choices=["sqlite", "events"],
        default="events",
        help="Edge runner state source: legacy SQLite or JetStream market events.",
    )
    parser.add_argument(
        "--opportunity-state-source",
        type=str,
        choices=["sqlite", "events"],
        default=None,
        help="Opportunity runner state source (defaults to edge-state-source).",
    )
    parser.add_argument(
        "--edges-events-consumer-durable",
        type=str,
        default=None,
        help=(
            "Optional durable name for edge market-event consumer in events mode. "
            "Use a stable name to avoid full startup replay."
        ),
    )
    parser.add_argument("--component-debug", action="store_true")
    parser.add_argument("--disable-event-shadow", action="store_true")
    parser.add_argument(
        "--enable-event-bus",
        action="store_true",
        help="Publish events to JetStream in producer components.",
    )
    parser.add_argument(
        "--event-bus-url",
        type=str,
        default=None,
        help="JetStream URL override (default BUS_URL).",
    )
    parser.add_argument(
        "--events-dir",
        type=str,
        default="logs/events",
        help="Directory for per-component shadow event JSONL logs.",
    )

    parser.add_argument("--coinbase-seconds", type=int, default=86400)
    parser.add_argument("--coinbase-publish-only", action="store_true")
    parser.add_argument("--quote-seconds", type=int, default=86400)
    parser.add_argument(
        "--quotes-reload-markets-every-seconds",
        type=int,
        default=120,
        help=(
            "When using WS quotes, periodically reload market subscriptions at this cadence. "
            "Set <=0 to disable."
        ),
    )
    parser.add_argument(
        "--quotes-reload-markets-align-period-seconds",
        type=int,
        default=0,
        help=(
            "If >0, align quote WS resubscribe to wall-clock boundaries "
            "(for example 900 for 00/15/30/45)."
        ),
    )
    parser.add_argument(
        "--quotes-reload-markets-align-offset-seconds",
        type=float,
        default=0.0,
        help="Offset seconds after each quote WS aligned boundary.",
    )
    parser.add_argument("--quotes-publish-only", action="store_true")
    parser.add_argument(
        "--quote-source",
        type=str,
        choices=["ws", "rest"],
        default="ws",
    )
    parser.add_argument("--quote-interval", type=float, default=5.0)
    parser.add_argument("--quote-concurrency", type=int, default=5)
    parser.add_argument("--edge-interval-seconds", type=int, default=10)
    parser.add_argument("--max-horizon-seconds", type=int, default=6 * 3600)
    parser.add_argument("--opportunity-interval-seconds", type=int, default=10)
    parser.add_argument("--execution-interval-seconds", type=float, default=1.0)
    parser.add_argument("--execution-log-every-seconds", type=float, default=10.0)
    parser.add_argument("--execution-max-open-positions", type=int, default=None)
    parser.add_argument("--execution-take-cooldown-seconds", type=int, default=None)
    parser.add_argument(
        "--execution-alert-reject-rate-threshold",
        type=float,
        default=0.50,
    )
    parser.add_argument(
        "--execution-alert-reject-rate-min-orders",
        type=int,
        default=10,
    )
    parser.add_argument(
        "--execution-alert-cooldown-seconds",
        type=float,
        default=300.0,
    )
    parser.add_argument(
        "--execution-kill-switch-path",
        type=str,
        default="data/kill_switch",
    )

    parser.add_argument("--scoring-every-minutes", type=int, default=10)
    parser.add_argument("--scoring-limit", type=int, default=2000)
    parser.add_argument("--disable-scoring", action="store_true")
    parser.add_argument("--report-every-minutes", type=int, default=60)
    parser.add_argument("--report-since-seconds", type=int, default=7 * 24 * 3600)
    parser.add_argument("--disable-report", action="store_true")
    parser.add_argument("--execution-report-every-minutes", type=int, default=60)
    parser.add_argument("--execution-report-since-seconds", type=int, default=24 * 3600)
    parser.add_argument("--disable-execution-report", action="store_true")
    parser.add_argument("--disable-refresh-jobs", action="store_true")
    parser.add_argument("--disable-refresh-settlements", action="store_true")
    parser.add_argument("--refresh-markets-every-minutes", type=int, default=15)
    parser.add_argument("--refresh-contracts-every-minutes", type=int, default=30)
    parser.add_argument("--refresh-settlements-every-minutes", type=int, default=15)
    parser.add_argument(
        "--refresh-markets-align-period-seconds",
        type=int,
        default=0,
        help=(
            "If >0, run refresh_markets on wall-clock aligned boundaries "
            "(for example 900 for quarter-hour cadence)."
        ),
    )
    parser.add_argument(
        "--refresh-markets-align-offset-seconds",
        type=float,
        default=0.0,
        help="Offset seconds after each aligned boundary for refresh_markets.",
    )
    parser.add_argument(
        "--refresh-contracts-align-period-seconds",
        type=int,
        default=0,
        help=(
            "If >0, run refresh_contracts on wall-clock aligned boundaries "
            "(for example 900 for quarter-hour cadence)."
        ),
    )
    parser.add_argument(
        "--refresh-contracts-align-offset-seconds",
        type=float,
        default=0.0,
        help="Offset seconds after each aligned boundary for refresh_contracts.",
    )
    parser.add_argument("--refresh-markets-initial-delay-seconds", type=int, default=60)
    parser.add_argument(
        "--refresh-contracts-initial-delay-seconds", type=int, default=8 * 60
    )
    parser.add_argument(
        "--refresh-settlements-initial-delay-seconds", type=int, default=11 * 60
    )
    parser.add_argument("--contracts-refresh-age-seconds", type=int, default=6 * 3600)
    parser.add_argument("--settlements-since-seconds", type=int, default=2 * 24 * 3600)
    parser.add_argument("--settlements-limit", type=int, default=600)
    parser.add_argument(
        "--refresh-markets-publish-only",
        action="store_true",
        help="Run refresh_btc_markets in publish-only mode (no SQLite writes).",
    )
    parser.add_argument(
        "--refresh-contracts-publish-only",
        action="store_true",
        help="Run refresh_kalshi_contracts in publish-only mode (no SQLite writes).",
    )

    parser.add_argument("--health-every-seconds", type=int, default=300)
    parser.add_argument(
        "--disable-health",
        action="store_true",
        help="Disable periodic health checks.",
    )
    parser.add_argument("--health-window-minutes", type=int, default=10)
    parser.add_argument("--health-max-spot-age-seconds", type=int, default=30)
    parser.add_argument("--health-max-quote-age-seconds", type=int, default=120)
    parser.add_argument("--health-max-snapshot-age-seconds", type=int, default=60)
    parser.add_argument(
        "--health-max-persistence-market-lag",
        type=int,
        default=None,
        help=(
            "Alert when persistence market consumer lag exceeds this threshold. "
            "Defaults to BUS_CONSUMER_LAG_ALERT_THRESHOLD when omitted."
        ),
    )
    parser.add_argument(
        "--health-persistence-metrics-log-path",
        type=str,
        default="logs/persistence/metrics.log",
        help="JSONL metrics path emitted by run_persistence_service.",
    )
    parser.add_argument(
        "--health-max-execution-reject-rate",
        type=float,
        default=0.50,
        help="Alert when execution reject-rate in health window exceeds this fraction.",
    )
    parser.add_argument(
        "--health-reject-rate-min-orders",
        type=int,
        default=10,
        help="Minimum execution orders in health window before reject-rate alerts apply.",
    )
    parser.add_argument("--health-alert-cooldown-seconds", type=int, default=300)
    return parser.parse_args()


def _series_args(series: list[str]) -> list[str]:
    args: list[str] = []
    for value in series:
        args.extend(["--series", value])
    return args


def _seconds_until_next_alignment(
    *,
    now_ts: float,
    period_seconds: int,
    offset_seconds: float,
) -> float:
    period = max(int(period_seconds), 1)
    offset = float(offset_seconds) % period
    base = int((now_ts - offset) // period) * period + offset
    next_ts = base + period
    if next_ts <= now_ts:
        next_ts += period
    return max(next_ts - now_ts, 0.0)


def _with_py_path(env: Mapping[str, str], src_path: Path) -> dict[str, str]:
    updated = dict(env)
    existing = updated.get("PYTHONPATH")
    if existing:
        updated["PYTHONPATH"] = f"{src_path}{os.pathsep}{existing}"
    else:
        updated["PYTHONPATH"] = str(src_path)
    return updated


async def _read_stream(
    stream: asyncio.StreamReader | None, prefix: str, stop_event: asyncio.Event
) -> None:
    if stream is None:
        return
    while not stop_event.is_set():
        line = await stream.readline()
        if not line:
            return
        text = line.decode("utf-8", errors="replace").rstrip()
        if text:
            print(f"[{prefix}] {text}")


async def _sleep_until_stop(stop_event: asyncio.Event, seconds: float) -> None:
    if seconds <= 0:
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        return


async def _run_component_loop(
    component: Component,
    *,
    cwd: Path,
    env: dict[str, str],
    registry: ProcessRegistry,
    stop_event: asyncio.Event,
) -> None:
    backoff_seconds = 1.0
    while not stop_event.is_set():
        print(f"[stack] starting {component.name}: {' '.join(component.cmd)}")
        try:
            proc = await asyncio.create_subprocess_exec(
                *component.cmd,
                cwd=str(cwd),
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception as exc:
            print(f"[stack] failed to start {component.name}: {exc}")
            await _sleep_until_stop(stop_event, backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, 60.0)
            continue

        await registry.add(proc)
        stdout_task = asyncio.create_task(
            _read_stream(proc.stdout, component.name, stop_event)
        )
        stderr_task = asyncio.create_task(
            _read_stream(proc.stderr, component.name, stop_event)
        )
        return_code = await proc.wait()
        await registry.discard(proc)
        await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)

        if stop_event.is_set():
            return

        if return_code == 0:
            print(f"[stack] {component.name} exited cleanly; restarting")
            backoff_seconds = 1.0
            await _sleep_until_stop(stop_event, 1.0)
        else:
            print(
                f"[stack] {component.name} exited with code={return_code}; "
                f"restarting in {backoff_seconds:.1f}s"
            )
            await _sleep_until_stop(stop_event, backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, 60.0)


async def _run_once(
    component: Component,
    *,
    cwd: Path,
    env: dict[str, str],
    stop_event: asyncio.Event,
) -> int:
    if stop_event.is_set():
        return 0
    print(f"[stack] running {component.name}: {' '.join(component.cmd)}")
    proc = await asyncio.create_subprocess_exec(
        *component.cmd,
        cwd=str(cwd),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_task = asyncio.create_task(_read_stream(proc.stdout, component.name, stop_event))
    stderr_task = asyncio.create_task(_read_stream(proc.stderr, component.name, stop_event))
    return_code = await proc.wait()
    await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
    if return_code != 0:
        print(f"[stack] {component.name} failed with code={return_code}")
    return return_code


async def _run_periodic_job(
    component: Component,
    *,
    interval_seconds: int,
    initial_delay_seconds: int = 0,
    align_period_seconds: int | None = None,
    align_offset_seconds: float = 0.0,
    cwd: Path,
    env: dict[str, str],
    stop_event: asyncio.Event,
) -> None:
    interval = max(interval_seconds, 1)
    if initial_delay_seconds > 0:
        await _sleep_until_stop(stop_event, float(initial_delay_seconds))
    while not stop_event.is_set():
        start = time.monotonic()
        try:
            await _run_once(component, cwd=cwd, env=env, stop_event=stop_event)
        except Exception as exc:
            print(f"[stack] {component.name} raised: {exc}")
        if align_period_seconds is not None and align_period_seconds > 0:
            wait_seconds = _seconds_until_next_alignment(
                now_ts=time.time(),
                period_seconds=align_period_seconds,
                offset_seconds=align_offset_seconds,
            )
        else:
            elapsed = time.monotonic() - start
            wait_seconds = max(0.0, interval - elapsed)
        await _sleep_until_stop(stop_event, wait_seconds)


async def _run_health_loop(
    *,
    db_path: Path,
    pg_dsn: str | None,
    product_id: str,
    max_horizon_seconds: int,
    every_seconds: int,
    window_minutes: int,
    max_spot_age_seconds: int | None,
    max_quote_age_seconds: int | None,
    max_snapshot_age_seconds: int | None,
    max_persistence_market_lag: int | None,
    persistence_metrics_log_path: Path | None,
    max_execution_reject_rate: float | None,
    reject_rate_min_orders: int,
    alert_cooldown_seconds: int,
    stop_event: asyncio.Event,
) -> None:
    interval = max(every_seconds, 1)
    cooldown = max(alert_cooldown_seconds, 1)
    last_alert_ts_by_key: dict[str, int] = {}

    def _should_alert(key: str, now_ts: int) -> bool:
        last = last_alert_ts_by_key.get(key)
        if last is None or (now_ts - last) >= cooldown:
            last_alert_ts_by_key[key] = now_ts
            return True
        return False

    def _age_exceeded(age_value: int | None, threshold: int | None) -> bool:
        if threshold is None or threshold <= 0:
            return False
        if age_value is None:
            return True
        return age_value > threshold

    def _read_latest_persistence_lag(
        path: Path, *, max_bytes: int = 65536
    ) -> dict[str, int] | None:
        if not path.exists():
            return None
        try:
            with path.open("rb") as handle:
                handle.seek(0, os.SEEK_END)
                size = handle.tell()
                start = max(0, size - max_bytes)
                handle.seek(start)
                chunk = handle.read().decode("utf-8", errors="replace")
        except OSError:
            return None
        lines = [line for line in chunk.splitlines() if line.strip()]
        for line in reversed(lines):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("kind") != "persistence_metrics":
                continue
            lag = row.get("consumer_lag")
            if not isinstance(lag, dict):
                continue
            out: dict[str, int] = {}
            for key in (
                "svc_persistence_market",
                "svc_persistence_strategy",
                "svc_persistence_execution",
            ):
                value = lag.get(key)
                if value is None:
                    continue
                try:
                    out[key] = int(value)
                except (TypeError, ValueError):
                    continue
            return out or None
        return None

    while not stop_event.is_set():
        now_ts = int(time.time())
        try:
            if pg_dsn:
                summary = await collect_live_health_postgres(
                    pg_dsn=pg_dsn,
                    now_ts=now_ts,
                    product_id=product_id,
                    window_minutes=window_minutes,
                    max_horizon_seconds=max_horizon_seconds,
                )
            else:
                async with aiosqlite.connect(db_path) as conn:
                    await conn.execute("PRAGMA busy_timeout = 5000;")
                    summary = await collect_live_health(
                        conn,
                        now_ts=now_ts,
                        product_id=product_id,
                        window_minutes=window_minutes,
                        max_horizon_seconds=max_horizon_seconds,
                    )
            print(f"[health] {format_live_health(summary)}")
            spot_age = summary.get("spot_tick_age_seconds")
            quote_age = summary.get("quote_age_seconds")
            snapshot_age = summary.get("snapshot_age_seconds")
            if _age_exceeded(spot_age, max_spot_age_seconds) and _should_alert(
                "spot_stale", now_ts
            ):
                print(
                    "[health] ALERT stale_spot age_s={age} threshold_s={threshold}".format(
                        age=spot_age, threshold=max_spot_age_seconds
                    )
                )
            if _age_exceeded(quote_age, max_quote_age_seconds) and _should_alert(
                "quote_stale", now_ts
            ):
                print(
                    "[health] ALERT stale_quote age_s={age} threshold_s={threshold}".format(
                        age=quote_age, threshold=max_quote_age_seconds
                    )
                )
            if _age_exceeded(snapshot_age, max_snapshot_age_seconds) and _should_alert(
                "snapshot_stale", now_ts
            ):
                print(
                    "[health] ALERT stale_snapshot age_s={age} threshold_s={threshold}".format(
                        age=snapshot_age, threshold=max_snapshot_age_seconds
                    )
                )
            if (
                max_execution_reject_rate is not None
                and max_execution_reject_rate >= 0.0
            ):
                orders = summary.get("execution_orders_last_window")
                rejects = summary.get("execution_rejects_last_window")
                reject_rate = summary.get("execution_reject_rate_last_window")
                try:
                    orders_int = int(orders) if orders is not None else 0
                except (TypeError, ValueError):
                    orders_int = 0
                try:
                    reject_rate_float = (
                        float(reject_rate) if reject_rate is not None else None
                    )
                except (TypeError, ValueError):
                    reject_rate_float = None
                if (
                    reject_rate_float is not None
                    and orders_int >= max(int(reject_rate_min_orders), 1)
                    and reject_rate_float > max_execution_reject_rate
                    and _should_alert("execution_reject_rate", now_ts)
                ):
                    print(
                        "[health] ALERT high_execution_reject_rate rate={rate:.3f} "
                        "orders={orders} rejects={rejects} threshold={threshold:.3f}".format(
                            rate=reject_rate_float,
                            orders=orders_int,
                            rejects=rejects if rejects is not None else "NA",
                            threshold=max_execution_reject_rate,
                        )
                    )
            if (
                persistence_metrics_log_path is not None
                and max_persistence_market_lag is not None
                and max_persistence_market_lag > 0
            ):
                lag_snapshot = _read_latest_persistence_lag(
                    persistence_metrics_log_path
                )
                if lag_snapshot is not None:
                    market_lag = lag_snapshot.get("svc_persistence_market")
                    if (
                        market_lag is not None
                        and market_lag > max_persistence_market_lag
                        and _should_alert("persistence_market_lag", now_ts)
                    ):
                        print(
                            "[health] ALERT persistence_market_lag lag={lag} threshold={threshold}".format(
                                lag=market_lag,
                                threshold=max_persistence_market_lag,
                            )
                        )
        except Exception as exc:
            print(f"[health] query_failed={exc}")
        await _sleep_until_stop(stop_event, interval)


async def _run() -> int:
    args = _parse_args()
    if args.opportunity_state_source is None:
        args.opportunity_state_source = args.edge_state_source
    args.status = normalize_db_status(args.status)
    args.series = (
        normalize_series(args.series)
        if args.series is not None
        else list(BTC_SERIES_TICKERS)
    )
    settings = load_settings()
    product_id = args.product_id or settings.coinbase_product_id

    repo_root = Path(__file__).resolve().parents[1]
    src_root = repo_root / "src"
    base_env = _with_py_path(os.environ, src_root)
    # Ensure child component stdout is unbuffered so stack logs stream in real time.
    base_env["PYTHONUNBUFFERED"] = "1"
    events_dir = repo_root / args.events_dir
    if not args.disable_event_shadow:
        events_dir.mkdir(parents=True, exist_ok=True)
    requires_event_bus = (
        args.enable_event_bus
        or args.edge_state_source == "events"
        or args.opportunity_state_source == "events"
        or args.enable_execution
    )
    event_bus_url = (
        args.event_bus_url
        if args.event_bus_url
        else (settings.bus_url if requires_event_bus else None)
    )

    await init_db(settings.db_path)
    print(f"[stack] db_path={settings.db_path}")
    print(f"[stack] log_path={settings.log_path}")
    print(f"[stack] product_id={product_id}")
    if args.edge_state_source == "events":
        print(
            "[stack] NOTE: SQLite path is local cache/compatibility; "
            "event-mode state is driven by event bus (and Postgres projections when configured)."
        )
    print(
        "[stack] event_shadow={shadow} event_bus={bus}".format(
            shadow="off" if args.disable_event_shadow else str(events_dir),
            bus=event_bus_url or "off",
        )
    )
    if (
        args.edge_state_source == "events" or args.opportunity_state_source == "events"
    ) and not event_bus_url:
        raise RuntimeError(
            "Event-state components require event bus. "
            "Set --enable-event-bus or provide --event-bus-url."
        )
    if args.enable_execution and not event_bus_url:
        raise RuntimeError(
            "Execution service requires event bus. "
            "Set --enable-event-bus or provide --event-bus-url."
        )
    health_pg_dsn = settings.pg_dsn
    max_persistence_market_lag = (
        args.health_max_persistence_market_lag
        if args.health_max_persistence_market_lag is not None
        else int(settings.bus_consumer_lag_alert_threshold)
    )
    persistence_metrics_log_path = (
        repo_root / args.health_persistence_metrics_log_path
        if args.health_persistence_metrics_log_path
        else None
    )
    if args.edge_state_source == "events" and not args.disable_health:
        if health_pg_dsn:
            print("[stack] health backend=postgres (event_store projections)")
        else:
            print(
                "[stack] NOTE: health loop reads legacy SQLite tables when PG_DSN is unset; "
                "set PG_DSN for event-store health or use --disable-health."
            )
    refresh_markets_initial_delay_seconds = args.refresh_markets_initial_delay_seconds
    refresh_contracts_initial_delay_seconds = (
        args.refresh_contracts_initial_delay_seconds
    )
    if args.edge_state_source == "events":
        refresh_markets_initial_delay_seconds = 0
        refresh_contracts_initial_delay_seconds = 0
        print(
            "[stack] NOTE: edge-state-source=events forcing immediate "
            "refresh_markets/refresh_contracts warmup for state hydration."
        )

    series_args = _series_args(args.series)

    collector_cmd = [
        args.python_bin,
        "-m",
        "kalshi_bot.app.collector",
        "--coinbase",
        "--seconds",
        str(args.coinbase_seconds),
    ]
    if args.component_debug:
        collector_cmd.append("--debug")
    if args.coinbase_publish_only:
        collector_cmd.append("--coinbase-publish-only")
    if not args.disable_event_shadow:
        collector_cmd.extend(
            [
                "--events-jsonl-path",
                str(events_dir / "spot_ticks.jsonl"),
            ]
        )
    if event_bus_url:
        collector_cmd.extend(
            [
                "--events-bus",
                "--events-bus-url",
                event_bus_url,
            ]
        )

    status_value = args.status if args.status is not None else "all"
    status_args = ["--status", status_value]

    if args.quote_source == "ws":
        quotes_cmd = [
            args.python_bin,
            str(repo_root / "scripts" / "stream_kalshi_quotes_ws.py"),
            "--seconds",
            str(args.quote_seconds),
            "--max-horizon-seconds",
            str(args.max_horizon_seconds),
            "--reload-markets-every-seconds",
            str(args.quotes_reload_markets_every_seconds),
            "--reload-markets-align-period-seconds",
            str(args.quotes_reload_markets_align_period_seconds),
            "--reload-markets-align-offset-seconds",
            str(args.quotes_reload_markets_align_offset_seconds),
            *status_args,
            *series_args,
        ]
        if not args.disable_event_shadow:
            quotes_cmd.extend(
                [
                    "--events-jsonl-path",
                    str(events_dir / "quote_updates.jsonl"),
                ]
            )
        if event_bus_url:
            quotes_cmd.extend(
                [
                    "--events-bus",
                    "--events-bus-url",
                    event_bus_url,
                ]
            )
        if args.quotes_publish_only:
            quotes_cmd.append("--publish-only")
    else:
        quotes_cmd = [
            args.python_bin,
            str(repo_root / "scripts" / "poll_kalshi_quotes.py"),
            "--seconds",
            str(args.quote_seconds),
            "--interval",
            str(args.quote_interval),
            "--concurrency",
            str(args.quote_concurrency),
            "--max-horizon-seconds",
            str(args.max_horizon_seconds),
            *status_args,
            *series_args,
        ]

    edges_cmd = [
        args.python_bin,
        str(repo_root / "scripts" / "run_live_edges.py"),
        "--interval-seconds",
        str(args.edge_interval_seconds),
        "--product-id",
        product_id,
        "--max-horizon-seconds",
        str(args.max_horizon_seconds),
        "--state-source",
        args.edge_state_source,
        *status_args,
        *series_args,
    ]
    if args.component_debug:
        edges_cmd.append("--debug")
    if not args.disable_event_shadow:
        edges_cmd.extend(
            [
                "--events-jsonl-path",
                str(events_dir / "edge_snapshots.jsonl"),
            ]
        )
    if event_bus_url:
        edges_cmd.extend(
            [
                "--events-bus",
                "--events-bus-url",
                event_bus_url,
            ]
        )
    if args.edges_events_consumer_durable:
        edges_cmd.extend(
            ["--events-consumer-durable", args.edges_events_consumer_durable]
        )

    opportunities_cmd = [
        args.python_bin,
        str(repo_root / "scripts" / "run_opportunity_loop.py"),
        "--interval-seconds",
        str(args.opportunity_interval_seconds),
        "--max-snapshot-age-seconds",
        str(args.health_max_snapshot_age_seconds),
        "--state-source",
        args.opportunity_state_source,
    ]
    if args.component_debug:
        opportunities_cmd.append("--debug")
    if not args.disable_event_shadow:
        opportunities_cmd.extend(
            [
                "--events-jsonl-path",
                str(events_dir / "opportunity_decisions.jsonl"),
            ]
        )
    if event_bus_url:
        opportunities_cmd.extend(
            [
                "--events-bus",
                "--events-bus-url",
                event_bus_url,
            ]
        )
    execution_cmd = [
        args.python_bin,
        str(repo_root / "scripts" / "run_paper_execution.py"),
        "--interval-seconds",
        str(args.execution_interval_seconds),
        "--log-every-seconds",
        str(args.execution_log_every_seconds),
        "--kill-switch-path",
        str(repo_root / args.execution_kill_switch_path),
    ]
    if args.execution_max_open_positions is not None:
        execution_cmd.extend(
            ["--max-open-positions", str(args.execution_max_open_positions)]
        )
    if args.execution_take_cooldown_seconds is not None:
        execution_cmd.extend(
            ["--take-cooldown-seconds", str(args.execution_take_cooldown_seconds)]
        )
    execution_cmd.extend(
        [
            "--alert-reject-rate-threshold",
            str(args.execution_alert_reject_rate_threshold),
            "--alert-reject-rate-min-orders",
            str(args.execution_alert_reject_rate_min_orders),
            "--alert-cooldown-seconds",
            str(args.execution_alert_cooldown_seconds),
        ]
    )
    if not args.disable_event_shadow:
        execution_cmd.extend(
            [
                "--events-jsonl-path",
                str(events_dir / "execution_events.jsonl"),
            ]
        )
    if event_bus_url:
        execution_cmd.extend(
            [
                "--events-bus-url",
                event_bus_url,
            ]
        )
    if args.component_debug:
        execution_cmd.append("--debug")

    scoring_cmd = [
        args.python_bin,
        str(repo_root / "scripts" / "score_edge_snapshots.py"),
        "--limit",
        str(args.scoring_limit),
    ]
    report_cmd = [
        args.python_bin,
        str(repo_root / "scripts" / "report_model_performance.py"),
        "--since-seconds",
        str(args.report_since_seconds),
    ]
    execution_report_cmd = [
        args.python_bin,
        str(repo_root / "scripts" / "report_execution_daily.py"),
        "--since-seconds",
        str(args.execution_report_since_seconds),
    ]
    refresh_markets_cmd = [
        args.python_bin,
        str(repo_root / "scripts" / "refresh_btc_markets.py"),
        *status_args,
    ]
    refresh_contracts_cmd = [
        args.python_bin,
        str(repo_root / "scripts" / "refresh_kalshi_contracts.py"),
        *status_args,
        *series_args,
        "--refresh-age-seconds",
        str(args.contracts_refresh_age_seconds),
    ]
    refresh_settlements_cmd = [
        args.python_bin,
        str(repo_root / "scripts" / "refresh_kalshi_settlements.py"),
        "--status",
        "resolved",
        "--since-seconds",
        str(args.settlements_since_seconds),
        "--limit",
        str(args.settlements_limit),
        *series_args,
    ]
    if settings.pg_dsn:
        refresh_settlements_cmd.extend(["--pg-dsn", settings.pg_dsn])
    if args.refresh_markets_publish_only:
        refresh_markets_cmd.append("--publish-only")
    if args.refresh_contracts_publish_only:
        refresh_contracts_cmd.append("--publish-only")
    if not args.disable_event_shadow:
        refresh_markets_cmd.extend(
            [
                "--events-jsonl-path",
                str(events_dir / "market_lifecycle.jsonl"),
            ]
        )
        refresh_contracts_cmd.extend(
            [
                "--events-jsonl-path",
                str(events_dir / "contract_updates.jsonl"),
            ]
        )
        refresh_settlements_cmd.extend(
            [
                "--events-jsonl-path",
                str(events_dir / "settlement_updates.jsonl"),
            ]
        )
    if event_bus_url:
        refresh_markets_cmd.extend(
            [
                "--events-bus",
                "--events-bus-url",
                event_bus_url,
            ]
        )
        refresh_settlements_cmd.extend(
            [
                "--events-bus",
                "--events-bus-url",
                event_bus_url,
            ]
        )
        refresh_contracts_cmd.extend(
            [
                "--events-bus",
                "--events-bus-url",
                event_bus_url,
            ]
        )
    long_running: list[Component] = []
    if not args.disable_coinbase:
        long_running.append(Component("coinbase", collector_cmd))
    if not args.disable_quotes:
        long_running.append(Component("quotes", quotes_cmd))
    if not args.disable_edges:
        long_running.append(Component("edges", edges_cmd))
    if not args.disable_opportunities:
        long_running.append(Component("opportunities", opportunities_cmd))
    if args.enable_execution:
        long_running.append(Component("execution", execution_cmd))
    periodic: list[PeriodicJob] = []
    if not args.disable_scoring:
        periodic.append(
            PeriodicJob(
                component=Component("scoring", scoring_cmd),
                interval_seconds=args.scoring_every_minutes * 60,
                initial_delay_seconds=0,
            )
        )
    if not args.disable_report:
        periodic.append(
            PeriodicJob(
                component=Component("report", report_cmd),
                interval_seconds=args.report_every_minutes * 60,
                initial_delay_seconds=0,
            )
        )
    if not args.disable_execution_report:
        if settings.pg_dsn:
            periodic.append(
                PeriodicJob(
                    component=Component("execution_report", execution_report_cmd),
                    interval_seconds=args.execution_report_every_minutes * 60,
                    initial_delay_seconds=0,
                )
            )
        else:
            print(
                "[stack] NOTE: execution report disabled because PG_DSN is not set."
            )
    if not args.disable_refresh_jobs:
        refresh_markets_align_period_seconds = (
            args.refresh_markets_align_period_seconds
            if args.refresh_markets_align_period_seconds > 0
            else None
        )
        refresh_contracts_align_period_seconds = (
            args.refresh_contracts_align_period_seconds
            if args.refresh_contracts_align_period_seconds > 0
            else None
        )
        periodic.extend(
            [
                PeriodicJob(
                    component=Component("refresh_markets", refresh_markets_cmd),
                    interval_seconds=args.refresh_markets_every_minutes * 60,
                    initial_delay_seconds=refresh_markets_initial_delay_seconds,
                    align_period_seconds=refresh_markets_align_period_seconds,
                    align_offset_seconds=args.refresh_markets_align_offset_seconds,
                ),
                PeriodicJob(
                    component=Component("refresh_contracts", refresh_contracts_cmd),
                    interval_seconds=args.refresh_contracts_every_minutes * 60,
                    initial_delay_seconds=refresh_contracts_initial_delay_seconds,
                    align_period_seconds=refresh_contracts_align_period_seconds,
                    align_offset_seconds=args.refresh_contracts_align_offset_seconds,
                ),
            ]
        )
        if not args.disable_refresh_settlements:
            periodic.append(
                PeriodicJob(
                    component=Component("refresh_settlements", refresh_settlements_cmd),
                    interval_seconds=args.refresh_settlements_every_minutes * 60,
                    initial_delay_seconds=args.refresh_settlements_initial_delay_seconds,
                )
            )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    registry = ProcessRegistry()
    tasks: list[asyncio.Task[None]] = []
    for component in long_running:
        tasks.append(
            asyncio.create_task(
                _run_component_loop(
                    component,
                    cwd=repo_root,
                    env=base_env,
                    registry=registry,
                    stop_event=stop_event,
                )
            )
        )
    for job in periodic:
        tasks.append(
            asyncio.create_task(
                _run_periodic_job(
                    job.component,
                    interval_seconds=job.interval_seconds,
                    initial_delay_seconds=job.initial_delay_seconds,
                    align_period_seconds=job.align_period_seconds,
                    align_offset_seconds=job.align_offset_seconds,
                    cwd=repo_root,
                    env=base_env,
                    stop_event=stop_event,
                )
            )
        )
    if not args.disable_health:
        tasks.append(
            asyncio.create_task(
                _run_health_loop(
                        db_path=settings.db_path,
                        pg_dsn=health_pg_dsn,
                        product_id=product_id,
                        max_horizon_seconds=args.max_horizon_seconds,
                        every_seconds=args.health_every_seconds,
                        window_minutes=args.health_window_minutes,
                        max_spot_age_seconds=args.health_max_spot_age_seconds,
                        max_quote_age_seconds=args.health_max_quote_age_seconds,
                        max_snapshot_age_seconds=args.health_max_snapshot_age_seconds,
                        max_persistence_market_lag=max_persistence_market_lag,
                        persistence_metrics_log_path=persistence_metrics_log_path,
                        max_execution_reject_rate=args.health_max_execution_reject_rate,
                        reject_rate_min_orders=args.health_reject_rate_min_orders,
                        alert_cooldown_seconds=args.health_alert_cooldown_seconds,
                        stop_event=stop_event,
                )
            )
        )

    await stop_event.wait()
    print("[stack] shutdown requested")
    await registry.terminate_all()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    print("[stack] shutdown complete")
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
