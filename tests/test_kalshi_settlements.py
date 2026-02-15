import asyncio
import time
import importlib
from datetime import datetime, timezone
from typing import Any

import aiosqlite

from kalshi_bot.data import init_db
from kalshi_bot.data.dao import Dao


def _load_settlements_module() -> object:
    return importlib.import_module(
        "kalshi_bot.app.refresh_kalshi_settlements"
    )


def _iso_ts(ts: int) -> str:
    return (
        datetime.fromtimestamp(ts, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def test_update_contract_outcome_writes_fields(tmp_path):
    db_path = tmp_path / "settlements.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("KXBTC-SETTLE", 30000.0, None, "greater", now, now),
            )
            await conn.commit()

            dao = Dao(conn)
            rowcount = await dao.update_contract_outcome(
                ticker="KXBTC-SETTLE",
                outcome=1,
                settled_ts=now + 10,
                updated_ts=now + 20,
                raw_json="{}",
            )
            assert rowcount == 1
            await conn.commit()

            cursor = await conn.execute(
                "SELECT outcome, settled_ts, raw_json FROM kalshi_contracts WHERE ticker = ?",
                ("KXBTC-SETTLE",),
            )
            row = await cursor.fetchone()
            assert row == (1, now + 10, "{}")

    asyncio.run(_run())


def test_refresh_settlements_dry_run_and_idempotent(tmp_path):
    db_path = tmp_path / "settlements_dry.sqlite"
    module = _load_settlements_module()

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("KXBTC-SETTLED", 30000.0, None, "greater", now, now),
            )
            await conn.commit()

            market = {
                "ticker": "KXBTC-SETTLED",
                "result": "yes",
                "settlement_ts": _iso_ts(now),
            }
            existing = {"KXBTC-SETTLED": {"outcome": None, "settled_ts": None}}
            updates, create_rows, counters = module.build_settlement_updates(
                [market],
                existing,
                now_ts=now,
                since_ts=now - 60,
                logger=None,
                force=False,
            )
            assert counters["updated_outcomes_count"] == 1
            assert len(create_rows) == 0

            applied = await module._apply_updates(
                conn, updates, now_ts=now, dry_run=True, force=False
            )
            assert applied == 0
            cursor = await conn.execute(
                "SELECT outcome FROM kalshi_contracts WHERE ticker = ?",
                ("KXBTC-SETTLED",),
            )
            row = await cursor.fetchone()
            assert row == (None,)

            applied = await module._apply_updates(
                conn, updates, now_ts=now, dry_run=False, force=False
            )
            assert applied == 1
            await conn.commit()

            cursor = await conn.execute(
                "SELECT outcome FROM kalshi_contracts WHERE ticker = ?",
                ("KXBTC-SETTLED",),
            )
            row = await cursor.fetchone()
            assert row == (1,)

            existing = {"KXBTC-SETTLED": {"outcome": 1, "settled_ts": now}}
            updates, create_rows, counters = module.build_settlement_updates(
                [market],
                existing,
                now_ts=now,
                since_ts=now - 60,
                logger=None,
                force=False,
            )
            assert counters["already_had_outcome_count"] == 1
            assert len(create_rows) == 0

    asyncio.run(_run())


def test_refresh_settlements_dry_run_no_changes(tmp_path):
    db_path = tmp_path / "settlements_dry_no_change.sqlite"
    module = _load_settlements_module()

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("KXBTC-DRY", 30000.0, None, "greater", now, now),
            )
            await conn.commit()

            market = {
                "ticker": "KXBTC-DRY",
                "result": "yes",
                "settlement_ts": _iso_ts(now),
            }
            existing = {"KXBTC-DRY": {"outcome": None, "settled_ts": None}}
            updates, _, _ = module.build_settlement_updates(
                [market],
                existing,
                now_ts=now,
                since_ts=now - 60,
                logger=None,
                force=False,
            )
            applied = await module._apply_updates(
                conn, updates, now_ts=now, dry_run=True, force=False
            )
            assert applied == 0

            cursor = await conn.execute(
                "SELECT outcome, settled_ts FROM kalshi_contracts WHERE ticker = ?",
                ("KXBTC-DRY",),
            )
            row = await cursor.fetchone()
            assert row == (None, None)

    asyncio.run(_run())


def test_refresh_settlements_creates_missing_contract_and_conflict(tmp_path):
    db_path = tmp_path / "settlements_conflict.sqlite"
    module = _load_settlements_module()

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            market = {
                "ticker": "KXBTC-MISSING",
                "result": "yes",
                "settlement_ts": _iso_ts(now),
                "close_time": _iso_ts(now - 60),
            }
            existing: dict[str, dict[str, Any]] = {}
            updates, create_rows, counters = module.build_settlement_updates(
                [market],
                existing,
                now_ts=now,
                since_ts=now - 60,
                logger=None,
                force=False,
            )
            assert counters["created_contracts"] == 1
            assert len(create_rows) == 1

            dao = Dao(conn)
            await dao.upsert_kalshi_contract(create_rows[0])
            await conn.commit()
            applied = await module._apply_updates(
                conn, updates, now_ts=now, dry_run=False, force=False
            )
            assert applied == 1
            await conn.commit()

            cursor = await conn.execute(
                "SELECT outcome, settled_ts FROM kalshi_contracts WHERE ticker = ?",
                ("KXBTC-MISSING",),
            )
            row = await cursor.fetchone()
            assert row == (1, now)

            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, outcome, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("KXBTC-CONFLICT", 30000.0, None, "greater", now, 0, now),
            )
            await conn.commit()

            conflict_market = {
                "ticker": "KXBTC-CONFLICT",
                "result": "yes",
                "settlement_ts": _iso_ts(now),
            }
            existing = {"KXBTC-CONFLICT": {"outcome": 0, "settled_ts": None}}
            updates, _, counters = module.build_settlement_updates(
                [conflict_market],
                existing,
                now_ts=now,
                since_ts=now - 60,
                logger=None,
                force=False,
            )
            assert counters["conflict_outcome_count"] == 1
            applied = await module._apply_updates(
                conn, updates, now_ts=now, dry_run=False, force=False
            )
            assert applied == 1
            await conn.commit()

            cursor = await conn.execute(
                "SELECT outcome FROM kalshi_contracts WHERE ticker = ?",
                ("KXBTC-CONFLICT",),
            )
            row = await cursor.fetchone()
            assert row == (0,)

            updates, _, counters = module.build_settlement_updates(
                [conflict_market],
                existing,
                now_ts=now,
                since_ts=now - 60,
                logger=None,
                force=True,
            )
            applied = await module._apply_updates(
                conn, updates, now_ts=now, dry_run=False, force=True
            )
            assert applied == 1
            await conn.commit()

            cursor = await conn.execute(
                "SELECT outcome FROM kalshi_contracts WHERE ticker = ?",
                ("KXBTC-CONFLICT",),
            )
            row = await cursor.fetchone()
            assert row == (1,)

    asyncio.run(_run())
