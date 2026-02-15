from __future__ import annotations

import json
from typing import Any

from kalshi_bot.events.models import (
    ContractUpdateEvent,
    EdgeSnapshotEvent,
    ExecutionFillEvent,
    ExecutionOrderEvent,
    EventBase,
    MarketLifecycleEvent,
    OpportunityDecisionEvent,
    QuoteUpdateEvent,
    SpotTickEvent,
)
from kalshi_bot.persistence.repository import PersistResult


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS event_store;

CREATE TABLE IF NOT EXISTS event_store.events_raw (
    event_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    idempotency_key TEXT NOT NULL,
    ts_event BIGINT NOT NULL,
    source TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    event_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_type, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_events_raw_ts_event
ON event_store.events_raw (ts_event DESC);

CREATE TABLE IF NOT EXISTS event_store.state_spot_latest (
    product_id TEXT PRIMARY KEY,
    ts BIGINT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    best_bid DOUBLE PRECISION,
    best_ask DOUBLE PRECISION,
    bid_qty DOUBLE PRECISION,
    ask_qty DOUBLE PRECISION,
    sequence_num BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS event_store.state_quote_latest (
    market_id TEXT PRIMARY KEY,
    ts BIGINT NOT NULL,
    yes_bid DOUBLE PRECISION,
    yes_ask DOUBLE PRECISION,
    no_bid DOUBLE PRECISION,
    no_ask DOUBLE PRECISION,
    yes_mid DOUBLE PRECISION,
    no_mid DOUBLE PRECISION,
    p_mid DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS event_store.state_market_latest (
    market_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    close_ts BIGINT,
    expected_expiration_ts BIGINT,
    expiration_ts BIGINT,
    settlement_ts BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS event_store.state_contract_latest (
    ticker TEXT PRIMARY KEY,
    lower DOUBLE PRECISION,
    upper DOUBLE PRECISION,
    strike_type TEXT,
    close_ts BIGINT,
    expected_expiration_ts BIGINT,
    expiration_ts BIGINT,
    settled_ts BIGINT,
    outcome INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS event_store.strategy_edge_latest (
    market_id TEXT PRIMARY KEY,
    asof_ts BIGINT NOT NULL,
    prob_yes DOUBLE PRECISION NOT NULL,
    ev_take_yes DOUBLE PRECISION NOT NULL,
    ev_take_no DOUBLE PRECISION NOT NULL,
    sigma_annualized DOUBLE PRECISION NOT NULL,
    spot_price DOUBLE PRECISION NOT NULL,
    quote_ts BIGINT,
    spot_ts BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS event_store.strategy_opportunity_latest (
    market_id TEXT PRIMARY KEY,
    ts_eval BIGINT NOT NULL,
    eligible BOOLEAN NOT NULL,
    would_trade BOOLEAN NOT NULL,
    side TEXT,
    reason_not_eligible TEXT,
    ev_raw DOUBLE PRECISION,
    ev_net DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS event_store.execution_order_latest (
    order_id TEXT PRIMARY KEY,
    ts_order BIGINT NOT NULL,
    market_id TEXT NOT NULL,
    side TEXT NOT NULL,
    action TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price_cents DOUBLE PRECISION,
    status TEXT NOT NULL,
    reason TEXT,
    opportunity_idempotency_key TEXT,
    paper BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS event_store.execution_fill_latest (
    fill_id TEXT PRIMARY KEY,
    ts_fill BIGINT NOT NULL,
    order_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    side TEXT NOT NULL,
    action TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price_cents DOUBLE PRECISION,
    outcome INTEGER,
    reason TEXT,
    paper BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS event_store.fact_edge_snapshot_scores (
    asof_ts BIGINT NOT NULL,
    market_id TEXT NOT NULL,
    settled_ts BIGINT,
    outcome INTEGER NOT NULL,
    pnl_take_yes DOUBLE PRECISION,
    pnl_take_no DOUBLE PRECISION,
    brier DOUBLE PRECISION,
    logloss DOUBLE PRECISION,
    error TEXT,
    created_ts BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (asof_ts, market_id)
);
"""


class PostgresEventRepository:
    def __init__(self, dsn: str) -> None:
        if not dsn:
            raise ValueError("Postgres DSN is required")
        self._dsn = dsn
        self._conn: Any | None = None
        self._psycopg: Any | None = None

    def _connect(self) -> Any:
        if self._conn is not None:
            return self._conn
        try:
            import psycopg
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "psycopg is required for Postgres persistence. "
                "Install with: pip install psycopg[binary]"
            ) from exc
        self._psycopg = psycopg
        self._conn = psycopg.connect(self._dsn)
        return self._conn

    def ensure_schema(self) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()

    def upsert_event(self, event: EventBase) -> PersistResult:
        conn = self._connect()
        payload_json = json.dumps(event.payload.model_dump(mode="json"))
        event_json = event.model_dump_json()
        inserted = False
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO event_store.events_raw (
                        event_type,
                        schema_version,
                        idempotency_key,
                        ts_event,
                        source,
                        payload_json,
                        event_json
                    ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                    ON CONFLICT (event_type, idempotency_key) DO NOTHING
                    RETURNING 1
                    """,
                    (
                        event.event_type,
                        int(event.schema_version),
                        str(event.idempotency_key),
                        int(event.ts_event),
                        event.source,
                        payload_json,
                        event_json,
                    ),
                )
                inserted = cur.fetchone() is not None
                if inserted:
                    self._upsert_projection(cur, event)
            conn.commit()
            return PersistResult(inserted=inserted)
        except Exception:
            conn.rollback()
            raise

    def _upsert_projection(self, cur: Any, event: EventBase) -> None:
        payload = event.payload.model_dump(mode="python")
        if isinstance(event, SpotTickEvent):
            cur.execute(
                """
                INSERT INTO event_store.state_spot_latest (
                    product_id, ts, price, best_bid, best_ask, bid_qty, ask_qty, sequence_num
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    ts = EXCLUDED.ts,
                    price = EXCLUDED.price,
                    best_bid = EXCLUDED.best_bid,
                    best_ask = EXCLUDED.best_ask,
                    bid_qty = EXCLUDED.bid_qty,
                    ask_qty = EXCLUDED.ask_qty,
                    sequence_num = EXCLUDED.sequence_num,
                    updated_at = NOW()
                """,
                (
                    payload["product_id"],
                    payload["ts"],
                    payload["price"],
                    payload.get("best_bid"),
                    payload.get("best_ask"),
                    payload.get("bid_qty"),
                    payload.get("ask_qty"),
                    payload.get("sequence_num"),
                ),
            )
            return

        if isinstance(event, QuoteUpdateEvent):
            cur.execute(
                """
                INSERT INTO event_store.state_quote_latest (
                    market_id, ts, yes_bid, yes_ask, no_bid, no_ask, yes_mid, no_mid, p_mid
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (market_id) DO UPDATE SET
                    ts = EXCLUDED.ts,
                    yes_bid = EXCLUDED.yes_bid,
                    yes_ask = EXCLUDED.yes_ask,
                    no_bid = EXCLUDED.no_bid,
                    no_ask = EXCLUDED.no_ask,
                    yes_mid = EXCLUDED.yes_mid,
                    no_mid = EXCLUDED.no_mid,
                    p_mid = EXCLUDED.p_mid,
                    updated_at = NOW()
                """,
                (
                    payload["market_id"],
                    payload["ts"],
                    payload.get("yes_bid"),
                    payload.get("yes_ask"),
                    payload.get("no_bid"),
                    payload.get("no_ask"),
                    payload.get("yes_mid"),
                    payload.get("no_mid"),
                    payload.get("p_mid"),
                ),
            )
            return

        if isinstance(event, MarketLifecycleEvent):
            cur.execute(
                """
                INSERT INTO event_store.state_market_latest (
                    market_id, status, close_ts, expected_expiration_ts, expiration_ts, settlement_ts
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (market_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    close_ts = EXCLUDED.close_ts,
                    expected_expiration_ts = EXCLUDED.expected_expiration_ts,
                    expiration_ts = EXCLUDED.expiration_ts,
                    settlement_ts = EXCLUDED.settlement_ts,
                    updated_at = NOW()
                """,
                (
                    payload["market_id"],
                    payload["status"],
                    payload.get("close_ts"),
                    payload.get("expected_expiration_ts"),
                    payload.get("expiration_ts"),
                    payload.get("settlement_ts"),
                ),
            )
            return

        if isinstance(event, ContractUpdateEvent):
            cur.execute(
                """
                INSERT INTO event_store.state_contract_latest (
                    ticker, lower, upper, strike_type, close_ts,
                    expected_expiration_ts, expiration_ts, settled_ts, outcome
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker) DO UPDATE SET
                    lower = COALESCE(EXCLUDED.lower, event_store.state_contract_latest.lower),
                    upper = COALESCE(EXCLUDED.upper, event_store.state_contract_latest.upper),
                    strike_type = COALESCE(EXCLUDED.strike_type, event_store.state_contract_latest.strike_type),
                    close_ts = COALESCE(EXCLUDED.close_ts, event_store.state_contract_latest.close_ts),
                    expected_expiration_ts = COALESCE(
                        EXCLUDED.expected_expiration_ts,
                        event_store.state_contract_latest.expected_expiration_ts
                    ),
                    expiration_ts = COALESCE(EXCLUDED.expiration_ts, event_store.state_contract_latest.expiration_ts),
                    settled_ts = COALESCE(EXCLUDED.settled_ts, event_store.state_contract_latest.settled_ts),
                    outcome = COALESCE(EXCLUDED.outcome, event_store.state_contract_latest.outcome),
                    updated_at = NOW()
                """,
                (
                    payload["ticker"],
                    payload.get("lower"),
                    payload.get("upper"),
                    payload.get("strike_type"),
                    payload.get("close_ts"),
                    payload.get("expected_expiration_ts"),
                    payload.get("expiration_ts"),
                    payload.get("settled_ts"),
                    payload.get("outcome"),
                ),
            )
            return

        if isinstance(event, EdgeSnapshotEvent):
            cur.execute(
                """
                INSERT INTO event_store.strategy_edge_latest (
                    market_id, asof_ts, prob_yes, ev_take_yes, ev_take_no,
                    sigma_annualized, spot_price, quote_ts, spot_ts
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (market_id) DO UPDATE SET
                    asof_ts = EXCLUDED.asof_ts,
                    prob_yes = EXCLUDED.prob_yes,
                    ev_take_yes = EXCLUDED.ev_take_yes,
                    ev_take_no = EXCLUDED.ev_take_no,
                    sigma_annualized = EXCLUDED.sigma_annualized,
                    spot_price = EXCLUDED.spot_price,
                    quote_ts = EXCLUDED.quote_ts,
                    spot_ts = EXCLUDED.spot_ts,
                    updated_at = NOW()
                """,
                (
                    payload["market_id"],
                    payload["asof_ts"],
                    payload["prob_yes"],
                    payload["ev_take_yes"],
                    payload["ev_take_no"],
                    payload["sigma_annualized"],
                    payload["spot_price"],
                    payload.get("quote_ts"),
                    payload.get("spot_ts"),
                ),
            )
            return

        if isinstance(event, OpportunityDecisionEvent):
            cur.execute(
                """
                INSERT INTO event_store.strategy_opportunity_latest (
                    market_id, ts_eval, eligible, would_trade, side,
                    reason_not_eligible, ev_raw, ev_net
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (market_id) DO UPDATE SET
                    ts_eval = EXCLUDED.ts_eval,
                    eligible = EXCLUDED.eligible,
                    would_trade = EXCLUDED.would_trade,
                    side = EXCLUDED.side,
                    reason_not_eligible = EXCLUDED.reason_not_eligible,
                    ev_raw = EXCLUDED.ev_raw,
                    ev_net = EXCLUDED.ev_net,
                    updated_at = NOW()
                """,
                (
                    payload["market_id"],
                    payload["ts_eval"],
                    payload["eligible"],
                    payload["would_trade"],
                    payload.get("side"),
                    payload.get("reason_not_eligible"),
                    payload.get("ev_raw"),
                    payload.get("ev_net"),
                ),
            )
            return

        if isinstance(event, ExecutionOrderEvent):
            cur.execute(
                """
                INSERT INTO event_store.execution_order_latest (
                    order_id, ts_order, market_id, side, action, quantity,
                    price_cents, status, reason, opportunity_idempotency_key, paper
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE SET
                    ts_order = EXCLUDED.ts_order,
                    market_id = EXCLUDED.market_id,
                    side = EXCLUDED.side,
                    action = EXCLUDED.action,
                    quantity = EXCLUDED.quantity,
                    price_cents = EXCLUDED.price_cents,
                    status = EXCLUDED.status,
                    reason = EXCLUDED.reason,
                    opportunity_idempotency_key = EXCLUDED.opportunity_idempotency_key,
                    paper = EXCLUDED.paper,
                    updated_at = NOW()
                """,
                (
                    payload["order_id"],
                    payload["ts_order"],
                    payload["market_id"],
                    payload["side"],
                    payload.get("action", "open"),
                    payload.get("quantity", 1),
                    payload.get("price_cents"),
                    payload["status"],
                    payload.get("reason"),
                    payload.get("opportunity_idempotency_key"),
                    payload.get("paper", True),
                ),
            )
            return

        if isinstance(event, ExecutionFillEvent):
            cur.execute(
                """
                INSERT INTO event_store.execution_fill_latest (
                    fill_id, ts_fill, order_id, market_id, side, action, quantity,
                    price_cents, outcome, reason, paper
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fill_id) DO UPDATE SET
                    ts_fill = EXCLUDED.ts_fill,
                    order_id = EXCLUDED.order_id,
                    market_id = EXCLUDED.market_id,
                    side = EXCLUDED.side,
                    action = EXCLUDED.action,
                    quantity = EXCLUDED.quantity,
                    price_cents = EXCLUDED.price_cents,
                    outcome = EXCLUDED.outcome,
                    reason = EXCLUDED.reason,
                    paper = EXCLUDED.paper,
                    updated_at = NOW()
                """,
                (
                    payload["fill_id"],
                    payload["ts_fill"],
                    payload["order_id"],
                    payload["market_id"],
                    payload["side"],
                    payload.get("action", "open"),
                    payload.get("quantity", 1),
                    payload.get("price_cents"),
                    payload.get("outcome"),
                    payload.get("reason"),
                    payload.get("paper", True),
                ),
            )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
