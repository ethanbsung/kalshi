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
