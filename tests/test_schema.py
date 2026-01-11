import asyncio
import sqlite3

from kalshi_bot.data import init_db


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    # PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
    return {r[1] for r in rows}


def test_schema_tables_exist(tmp_path):
    db_path = tmp_path / "test.sqlite"
    asyncio.run(init_db(db_path))

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        tables = {row[0] for row in rows}

        required_tables = {
            "spot_ticks",
            "kalshi_markets",
            "kalshi_orderbook_snapshots",
            "kalshi_orderbook_deltas",
            "opportunities",
            "orders",
            "fills",
            "positions_snapshots",
            "settlements",
            "features",
            "schema_version",
        }
        missing = required_tables - tables
        assert not missing, f"Missing tables: {missing}"

        spot_cols = _table_columns(conn, "spot_ticks")
        required_spot_cols = {
            "ts",
            "product_id",
            "price",
            "best_bid",
            "best_ask",
            "bid_qty",
            "ask_qty",
            "sequence_num",
            "raw_json",
        }
        missing_spot = required_spot_cols - spot_cols
        assert not missing_spot, f"spot_ticks missing cols: {missing_spot}"

        opp_cols = _table_columns(conn, "opportunities")
        required_opp_cols = {
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
        }
        missing_opp = required_opp_cols - opp_cols
        assert not missing_opp, f"opportunities missing cols: {missing_opp}"

    finally:
        conn.close()