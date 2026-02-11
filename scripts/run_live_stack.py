from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import aiosqlite

from kalshi_bot.app.live_stack_health import collect_live_health, format_live_health
from kalshi_bot.config import load_settings
from kalshi_bot.data import init_db
from kalshi_bot.kalshi.btc_markets import BTC_SERIES_TICKERS
from kalshi_bot.kalshi.market_filters import normalize_series


@dataclass(frozen=True)
class Component:
    name: str
    cmd: list[str]


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
    parser.add_argument("--component-debug", action="store_true")

    parser.add_argument("--coinbase-seconds", type=int, default=86400)
    parser.add_argument("--quote-seconds", type=int, default=900)
    parser.add_argument("--quote-interval", type=float, default=5.0)
    parser.add_argument("--quote-concurrency", type=int, default=5)
    parser.add_argument("--edge-interval-seconds", type=int, default=10)
    parser.add_argument("--max-horizon-seconds", type=int, default=6 * 3600)
    parser.add_argument("--opportunity-interval-seconds", type=int, default=10)

    parser.add_argument("--settlements-every-minutes", type=int, default=15)
    parser.add_argument("--settlements-since-seconds", type=int, default=2 * 24 * 3600)
    parser.add_argument("--settlements-limit", type=int, default=600)
    parser.add_argument("--scoring-every-minutes", type=int, default=10)
    parser.add_argument("--scoring-limit", type=int, default=2000)
    parser.add_argument("--report-every-minutes", type=int, default=60)
    parser.add_argument("--report-since-seconds", type=int, default=7 * 24 * 3600)
    parser.add_argument("--disable-report", action="store_true")

    parser.add_argument("--health-every-seconds", type=int, default=300)
    parser.add_argument("--health-window-minutes", type=int, default=10)
    return parser.parse_args()


def _series_args(series: list[str]) -> list[str]:
    args: list[str] = []
    for value in series:
        args.extend(["--series", value])
    return args


def _with_py_path(env: dict[str, str], src_path: Path) -> dict[str, str]:
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
    cwd: Path,
    env: dict[str, str],
    stop_event: asyncio.Event,
) -> None:
    interval = max(interval_seconds, 1)
    while not stop_event.is_set():
        start = time.monotonic()
        try:
            await _run_once(component, cwd=cwd, env=env, stop_event=stop_event)
        except Exception as exc:
            print(f"[stack] {component.name} raised: {exc}")
        elapsed = time.monotonic() - start
        await _sleep_until_stop(stop_event, max(0.0, interval - elapsed))


async def _run_health_loop(
    *,
    db_path: Path,
    product_id: str,
    max_horizon_seconds: int,
    every_seconds: int,
    window_minutes: int,
    stop_event: asyncio.Event,
) -> None:
    interval = max(every_seconds, 1)
    while not stop_event.is_set():
        now_ts = int(time.time())
        try:
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
        except Exception as exc:
            print(f"[health] query_failed={exc}")
        await _sleep_until_stop(stop_event, interval)


async def _run() -> int:
    args = _parse_args()
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

    await init_db(settings.db_path)
    print(f"[stack] db_path={settings.db_path}")
    print(f"[stack] log_path={settings.log_path}")
    print(f"[stack] product_id={product_id}")

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

    quotes_cmd = [
        args.python_bin,
        str(repo_root / "scripts" / "poll_kalshi_quotes.py"),
        "--seconds",
        str(args.quote_seconds),
        "--interval",
        str(args.quote_interval),
        "--concurrency",
        str(args.quote_concurrency),
        "--status",
        args.status,
        *series_args,
    ]

    edges_cmd = [
        args.python_bin,
        str(repo_root / "scripts" / "run_live_edges.py"),
        "--interval-seconds",
        str(args.edge_interval_seconds),
        "--product-id",
        product_id,
        "--status",
        args.status,
        "--max-horizon-seconds",
        str(args.max_horizon_seconds),
        *series_args,
    ]
    if args.component_debug:
        edges_cmd.append("--debug")

    opportunities_cmd = [
        args.python_bin,
        str(repo_root / "scripts" / "run_opportunity_loop.py"),
        "--interval-seconds",
        str(args.opportunity_interval_seconds),
    ]
    if args.component_debug:
        opportunities_cmd.append("--debug")

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

    long_running = [
        Component("coinbase", collector_cmd),
        Component("quotes", quotes_cmd),
        Component("edges", edges_cmd),
        Component("opportunities", opportunities_cmd),
    ]
    periodic = [
        (
            Component("scoring", scoring_cmd),
            args.scoring_every_minutes * 60,
        ),
    ]
    if not args.disable_report:
        periodic.append((Component("report", report_cmd), args.report_every_minutes * 60))

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
    for component, interval_seconds in periodic:
        tasks.append(
            asyncio.create_task(
                _run_periodic_job(
                    component,
                    interval_seconds=interval_seconds,
                    cwd=repo_root,
                    env=base_env,
                    stop_event=stop_event,
                )
            )
        )
    tasks.append(
        asyncio.create_task(
            _run_health_loop(
                    db_path=settings.db_path,
                    product_id=product_id,
                    max_horizon_seconds=args.max_horizon_seconds,
                    every_seconds=args.health_every_seconds,
                    window_minutes=args.health_window_minutes,
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
