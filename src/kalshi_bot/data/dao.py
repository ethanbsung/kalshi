from __future__ import annotations

import asyncio
import sqlite3
from typing import Any, Mapping

import aiosqlite


class Dao:
    SPOT_TICK_COLUMNS = (
        "ts",
        "product_id",
        "price",
        "best_bid",
        "best_ask",
        "bid_qty",
        "ask_qty",
        "sequence_num",
        "raw_json",
    )
    KALSHI_MARKET_COLUMNS = (
        "market_id",
        "ts_loaded",
        "title",
        "strike",
        "settlement_ts",
        "close_ts",
        "expected_expiration_ts",
        "expiration_ts",
        "status",
        "raw_json",
    )
    KALSHI_TICKER_COLUMNS = (
        "ts",
        "market_id",
        "price",
        "best_yes_bid",
        "best_yes_ask",
        "best_no_bid",
        "best_no_ask",
        "volume",
        "open_interest",
        "dollar_volume",
        "dollar_open_interest",
        "raw_json",
    )
    KALSHI_QUOTE_COLUMNS = (
        "ts",
        "market_id",
        "yes_bid",
        "yes_ask",
        "no_bid",
        "no_ask",
        "yes_mid",
        "no_mid",
        "p_mid",
        "volume",
        "volume_24h",
        "open_interest",
        "raw_json",
    )
    KALSHI_CONTRACT_COLUMNS = (
        "ticker",
        "lower",
        "upper",
        "strike_type",
        "settlement_ts",
        "close_ts",
        "expected_expiration_ts",
        "expiration_ts",
        "settled_ts",
        "outcome",
        "raw_json",
        "updated_ts",
    )
    KALSHI_EDGE_COLUMNS = (
        "ts",
        "market_id",
        "settlement_ts",
        "horizon_seconds",
        "spot_price",
        "sigma_annualized",
        "prob_yes",
        "yes_bid",
        "yes_ask",
        "no_bid",
        "no_ask",
        "ev_take_yes",
        "ev_take_no",
        "raw_json",
    )
    KALSHI_EDGE_SNAPSHOT_COLUMNS = (
        "asof_ts",
        "market_id",
        "settlement_ts",
        "spot_ts",
        "spot_price",
        "sigma_annualized",
        "prob_yes",
        "prob_yes_raw",
        "horizon_seconds",
        "quote_ts",
        "yes_bid",
        "yes_ask",
        "no_bid",
        "no_ask",
        "yes_mid",
        "no_mid",
        "ev_take_yes",
        "ev_take_no",
        "spot_age_seconds",
        "quote_age_seconds",
        "skip_reason",
        "raw_json",
    )
    KALSHI_EDGE_SNAPSHOT_SCORE_COLUMNS = (
        "asof_ts",
        "market_id",
        "settled_ts",
        "outcome",
        "pnl_take_yes",
        "pnl_take_no",
        "brier",
        "logloss",
        "error",
        "created_ts",
    )
    OPPORTUNITY_COLUMNS = (
        "ts_eval",
        "market_id",
        "settlement_ts",
        "strike",
        "spot_price",
        "sigma",
        "tau",
        "p_model",
        "p_market",
        "best_yes_bid",
        "best_yes_ask",
        "best_no_bid",
        "best_no_ask",
        "spread",
        "eligible",
        "reason_not_eligible",
        "would_trade",
        "side",
        "ev_raw",
        "ev_net",
        "cost_buffer",
        "raw_json",
    )
    SPOT_SIGMA_HISTORY_COLUMNS = (
        "ts",
        "product_id",
        "sigma",
        "source",
        "reason",
        "method",
        "lookback_seconds",
        "points",
    )

    def __init__(self, conn: aiosqlite.Connection) -> None:
        self._conn = conn

    def _validate_columns(self, table: str, expected: tuple[str, ...], row: Mapping[str, Any]) -> None:
        missing = set(expected) - set(row.keys())
        extra = set(row.keys()) - set(expected)
        if missing or extra:
            raise ValueError(f"{table} columns mismatch missing={missing} extra={extra}")

    async def _insert_row(self, table: str, row: Mapping[str, Any]) -> None:
        if not row:
            raise ValueError("row cannot be empty")
        columns = ", ".join(row.keys())
        placeholders = ", ".join("?" for _ in row)
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        params = tuple(row.values())
        for attempt in range(4):
            try:
                await self._conn.execute(sql, params)
                return
            except sqlite3.OperationalError as exc:
                if "database is locked" not in str(exc).lower() or attempt == 3:
                    raise
                await asyncio.sleep(0.05 * (attempt + 1))

    async def insert_spot_tick(self, row: Mapping[str, Any]) -> None:
        self._validate_columns("spot_ticks", self.SPOT_TICK_COLUMNS, row)
        await self._insert_row("spot_ticks", row)

    async def upsert_kalshi_market(self, row: Mapping[str, Any]) -> None:
        self._validate_columns("kalshi_markets", self.KALSHI_MARKET_COLUMNS, row)
        columns = ", ".join(row.keys())
        placeholders = ", ".join("?" for _ in row)
        updates = ", ".join(
            f"{col}=excluded.{col}"
            for col in row.keys()
            if col != "market_id"
        )
        sql = (
            f"INSERT INTO kalshi_markets ({columns}) VALUES ({placeholders}) "
            f"ON CONFLICT(market_id) DO UPDATE SET {updates}"
        )
        await self._conn.execute(sql, tuple(row.values()))

    async def insert_kalshi_orderbook_snapshot(self, row: Mapping[str, Any]) -> None:
        await self._insert_row("kalshi_orderbook_snapshots", row)

    async def insert_kalshi_orderbook_delta(self, row: Mapping[str, Any]) -> None:
        await self._insert_row("kalshi_orderbook_deltas", row)

    async def insert_kalshi_ticker(self, row: Mapping[str, Any]) -> None:
        self._validate_columns("kalshi_tickers", self.KALSHI_TICKER_COLUMNS, row)
        await self._insert_row("kalshi_tickers", row)

    async def insert_kalshi_quote(self, row: Mapping[str, Any]) -> None:
        self._validate_columns("kalshi_quotes", self.KALSHI_QUOTE_COLUMNS, row)
        await self._insert_row("kalshi_quotes", row)

    async def upsert_kalshi_contract(self, row: Mapping[str, Any]) -> None:
        self._validate_columns("kalshi_contracts", self.KALSHI_CONTRACT_COLUMNS, row)
        columns = ", ".join(row.keys())
        placeholders = ", ".join("?" for _ in row)
        updates = ", ".join(
            f"{col}=excluded.{col}"
            for col in row.keys()
            if col != "ticker"
        )
        sql = (
            f"INSERT INTO kalshi_contracts ({columns}) VALUES ({placeholders}) "
            f"ON CONFLICT(ticker) DO UPDATE SET {updates}"
        )
        await self._conn.execute(sql, tuple(row.values()))

    async def update_contract_outcome(
        self,
        *,
        ticker: str,
        outcome: int | None,
        settled_ts: int | None,
        updated_ts: int,
        raw_json: str | None,
        force: bool = False,
    ) -> int:
        cursor = await self._conn.execute(
            "UPDATE kalshi_contracts "
            "SET outcome = CASE "
            "WHEN ? IS NULL THEN outcome "
            "WHEN outcome IS NULL THEN ? "
            "WHEN ? = 1 THEN ? "
            "ELSE outcome "
            "END, "
            "settled_ts = COALESCE(?, settled_ts), "
            "updated_ts = ?, "
            "raw_json = COALESCE(?, raw_json) "
            "WHERE ticker = ?",
            (
                outcome,
                outcome,
                1 if force else 0,
                outcome,
                settled_ts,
                updated_ts,
                raw_json,
                ticker,
            ),
        )
        return cursor.rowcount

    async def insert_kalshi_edge(self, row: Mapping[str, Any]) -> None:
        self._validate_columns("kalshi_edges", self.KALSHI_EDGE_COLUMNS, row)
        await self._insert_row("kalshi_edges", row)

    async def insert_kalshi_edge_snapshot(self, row: Mapping[str, Any]) -> None:
        self._validate_columns(
            "kalshi_edge_snapshots", self.KALSHI_EDGE_SNAPSHOT_COLUMNS, row
        )
        await self._insert_row("kalshi_edge_snapshots", row)

    async def insert_kalshi_edge_snapshots(
        self, rows: list[Mapping[str, Any]]
    ) -> None:
        if not rows:
            return
        for row in rows:
            await self.insert_kalshi_edge_snapshot(row)

    async def insert_kalshi_edge_snapshot_score(
        self, row: Mapping[str, Any]
    ) -> None:
        self._validate_columns(
            "kalshi_edge_snapshot_scores",
            self.KALSHI_EDGE_SNAPSHOT_SCORE_COLUMNS,
            row,
        )
        columns = ", ".join(row.keys())
        placeholders = ", ".join("?" for _ in row)
        sql = (
            "INSERT OR IGNORE INTO kalshi_edge_snapshot_scores "
            f"({columns}) VALUES ({placeholders})"
        )
        await self._conn.execute(sql, tuple(row.values()))

    async def insert_kalshi_edge_snapshot_scores(
        self, rows: list[Mapping[str, Any]]
    ) -> None:
        if not rows:
            return
        for row in rows:
            await self.insert_kalshi_edge_snapshot_score(row)

    async def get_unscored_edge_snapshots(
        self, limit: int, now_ts: int
    ) -> list[dict[str, Any]]:
        cursor = await self._conn.execute(
            """
            SELECT s.asof_ts, s.market_id, s.settlement_ts, s.spot_ts,
                   s.spot_price, s.sigma_annualized, s.prob_yes,
                   s.prob_yes_raw, s.horizon_seconds, s.quote_ts,
                   s.yes_bid, s.yes_ask, s.no_bid, s.no_ask,
                   s.yes_mid, s.no_mid, s.ev_take_yes, s.ev_take_no,
                   s.spot_age_seconds, s.quote_age_seconds,
                   s.skip_reason, s.raw_json, c.outcome, c.settled_ts
            FROM kalshi_edge_snapshots s
            JOIN kalshi_contracts c ON c.ticker = s.market_id
            LEFT JOIN kalshi_edge_snapshot_scores sc
                ON sc.market_id = s.market_id AND sc.asof_ts = s.asof_ts
            WHERE sc.market_id IS NULL
              AND s.settlement_ts IS NOT NULL
              AND s.settlement_ts <= ?
              AND c.outcome IS NOT NULL
            ORDER BY s.settlement_ts ASC, s.asof_ts ASC
            LIMIT ?
            """,
            (now_ts, limit),
        )
        rows = await cursor.fetchall()
        snapshots: list[dict[str, Any]] = []
        for row in rows:
            (
                asof_ts,
                market_id,
                settlement_ts,
                spot_ts,
                spot_price,
                sigma_annualized,
                prob_yes,
                prob_yes_raw,
                horizon_seconds,
                quote_ts,
                yes_bid,
                yes_ask,
                no_bid,
                no_ask,
                yes_mid,
                no_mid,
                ev_take_yes,
                ev_take_no,
                spot_age_seconds,
                quote_age_seconds,
                skip_reason,
                raw_json,
                outcome,
                settled_ts,
            ) = row
            outcome_val = int(outcome) if outcome is not None else None
            settled_ts_val = int(settled_ts) if settled_ts is not None else None
            snapshots.append(
                {
                    "asof_ts": asof_ts,
                    "market_id": market_id,
                    "settlement_ts": settlement_ts,
                    "spot_ts": spot_ts,
                    "spot_price": spot_price,
                    "sigma_annualized": sigma_annualized,
                    "prob_yes": prob_yes,
                    "prob_yes_raw": prob_yes_raw,
                    "horizon_seconds": horizon_seconds,
                    "quote_ts": quote_ts,
                    "yes_bid": yes_bid,
                    "yes_ask": yes_ask,
                    "no_bid": no_bid,
                    "no_ask": no_ask,
                    "yes_mid": yes_mid,
                    "no_mid": no_mid,
                    "ev_take_yes": ev_take_yes,
                    "ev_take_no": ev_take_no,
                    "spot_age_seconds": spot_age_seconds,
                    "quote_age_seconds": quote_age_seconds,
                    "skip_reason": skip_reason,
                    "raw_json": raw_json,
                    "outcome": outcome_val,
                    "settled_ts": settled_ts_val,
                }
            )
        return snapshots

    async def insert_sigma_history(self, row: Mapping[str, Any]) -> None:
        self._validate_columns(
            "spot_sigma_history", self.SPOT_SIGMA_HISTORY_COLUMNS, row
        )
        await self._insert_row("spot_sigma_history", row)

    async def get_latest_sigma(self, product_id: str) -> float | None:
        cursor = await self._conn.execute(
            "SELECT sigma FROM spot_sigma_history "
            "WHERE product_id = ? ORDER BY ts DESC LIMIT 1",
            (product_id,),
        )
        row = await cursor.fetchone()
        if not row or row[0] is None:
            return None
        try:
            return float(row[0])
        except (TypeError, ValueError):
            return None

    async def insert_opportunity(self, row: Mapping[str, Any]) -> None:
        self._validate_columns("opportunities", self.OPPORTUNITY_COLUMNS, row)
        await self._insert_row("opportunities", row)

    async def insert_opportunities(
        self, rows: list[Mapping[str, Any]]
    ) -> None:
        if not rows:
            return
        columns = self.OPPORTUNITY_COLUMNS
        sql = (
            "INSERT OR IGNORE INTO opportunities ("
            + ", ".join(columns)
            + ") VALUES ("
            + ", ".join("?" for _ in columns)
            + ")"
        )
        values = []
        for row in rows:
            self._validate_columns("opportunities", columns, row)
            values.append(tuple(row[col] for col in columns))
        await self._conn.executemany(sql, values)

    async def insert_order(self, row: Mapping[str, Any]) -> None:
        await self._insert_row("orders", row)

    async def insert_fill(self, row: Mapping[str, Any]) -> None:
        await self._insert_row("fills", row)

    async def insert_positions_snapshot(self, row: Mapping[str, Any]) -> None:
        await self._insert_row("positions_snapshots", row)

    async def insert_settlement(self, row: Mapping[str, Any]) -> None:
        await self._insert_row("settlements", row)

    async def insert_feature(self, row: Mapping[str, Any]) -> None:
        await self._insert_row("features", row)
