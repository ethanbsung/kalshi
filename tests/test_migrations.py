import asyncio
import sqlite3

from kalshi_bot.data import init_db


def test_migration_symbol_to_product_id_preserves_data(tmp_path):
    db_path = tmp_path / "old.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE spot_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                price REAL,
                best_bid REAL,
                best_ask REAL,
                bid_qty REAL,
                ask_qty REAL,
                sequence_num INTEGER,
                raw_json TEXT
            );
            """
        )
        conn.execute(
            """
            INSERT INTO spot_ticks (
                ts, symbol, price, best_bid, best_ask, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (1700000000, "BTC-USD", 42000.0, 41999.0, 42001.0, "{}"),
        )
        conn.commit()
    finally:
        conn.close()

    asyncio.run(init_db(db_path))

    conn = sqlite3.connect(db_path)
    try:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(spot_ticks)").fetchall()
        }
        row = conn.execute(
            "SELECT product_id, price FROM spot_ticks ORDER BY id LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    assert "product_id" in columns
    assert "symbol" not in columns
    assert row == ("BTC-USD", 42000.0)


def test_migration_017_dedupes_edge_snapshots_before_unique_index(tmp_path):
    db_path = tmp_path / "pre17.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE schema_version (
                version INTEGER PRIMARY KEY,
                applied_ts INTEGER NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO schema_version (version, applied_ts) VALUES (16, strftime('%s','now'))"
        )
        conn.execute(
            """
            CREATE TABLE kalshi_edge_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asof_ts INTEGER NOT NULL,
                market_id TEXT NOT NULL,
                raw_json TEXT NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO kalshi_edge_snapshots (asof_ts, market_id, raw_json) VALUES (?, ?, ?)",
            (100, "KXBTC-DUP", '{"n":1}'),
        )
        conn.execute(
            "INSERT INTO kalshi_edge_snapshots (asof_ts, market_id, raw_json) VALUES (?, ?, ?)",
            (100, "KXBTC-DUP", '{"n":2}'),
        )
        conn.execute(
            "INSERT INTO kalshi_edge_snapshots (asof_ts, market_id, raw_json) VALUES (?, ?, ?)",
            (101, "KXBTC-KEEP", '{"n":3}'),
        )
        conn.commit()
    finally:
        conn.close()

    asyncio.run(init_db(db_path))

    conn = sqlite3.connect(db_path)
    try:
        version = conn.execute(
            "SELECT MAX(version) FROM schema_version"
        ).fetchone()[0]
        assert version == 17

        rows = conn.execute(
            "SELECT id, asof_ts, market_id FROM kalshi_edge_snapshots ORDER BY id"
        ).fetchall()
        assert rows == [
            (1, 100, "KXBTC-DUP"),
            (3, 101, "KXBTC-KEEP"),
        ]

        index_rows = conn.execute(
            "PRAGMA index_list(kalshi_edge_snapshots)"
        ).fetchall()
        index_map = {row[1]: row[2] for row in index_rows}
        assert (
            index_map.get("idx_kalshi_edge_snapshots_unique_market_asof") == 1
        )
    finally:
        conn.close()


def test_upgrade_from_v13_handles_edge_snapshot_fk_prereq(tmp_path):
    db_path = tmp_path / "v13.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE schema_version (
                version INTEGER PRIMARY KEY,
                applied_ts INTEGER NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO schema_version (version, applied_ts) VALUES (13, strftime('%s','now'))"
        )
        conn.execute(
            """
            CREATE TABLE kalshi_contracts (
                ticker TEXT PRIMARY KEY,
                lower REAL,
                upper REAL,
                strike_type TEXT,
                settlement_ts INTEGER,
                updated_ts INTEGER NOT NULL,
                expiration_ts INTEGER,
                close_ts INTEGER,
                expected_expiration_ts INTEGER
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE kalshi_edge_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asof_ts INTEGER NOT NULL,
                market_id TEXT NOT NULL,
                raw_json TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_eval INTEGER NOT NULL,
                market_id TEXT NOT NULL,
                side TEXT
            );
            """
        )
        conn.execute(
            "INSERT INTO kalshi_edge_snapshots (asof_ts, market_id, raw_json) VALUES (?, ?, ?)",
            (100, "KXBTC-DUP", '{"n":1}'),
        )
        conn.execute(
            "INSERT INTO kalshi_edge_snapshots (asof_ts, market_id, raw_json) VALUES (?, ?, ?)",
            (100, "KXBTC-DUP", '{"n":2}'),
        )
        conn.commit()
    finally:
        conn.close()

    asyncio.run(init_db(db_path))

    conn = sqlite3.connect(db_path)
    try:
        version = conn.execute(
            "SELECT MAX(version) FROM schema_version"
        ).fetchone()[0]
        assert version == 17

        rows = conn.execute(
            "SELECT id, asof_ts, market_id FROM kalshi_edge_snapshots ORDER BY id"
        ).fetchall()
        assert rows == [(1, 100, "KXBTC-DUP")]

        indexes = conn.execute(
            "PRAGMA index_list(kalshi_edge_snapshots)"
        ).fetchall()
        index_map = {row[1]: row[2] for row in indexes}
        assert (
            index_map.get("idx_kalshi_edge_snapshots_unique_market_asof") == 1
        )

        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "kalshi_edge_snapshot_scores" in tables
    finally:
        conn.close()
