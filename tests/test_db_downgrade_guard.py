import asyncio
import sqlite3
from kalshi_bot.data import init_db

def test_init_db_fails_if_db_newer_than_code(tmp_path):
    db_path = tmp_path / "newer.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY, applied_ts INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_version (version, applied_ts) VALUES (999, strftime('%s','now'))")
        conn.commit()
    finally:
        conn.close()

    try:
        asyncio.run(init_db(db_path))
        assert False, "expected init_db to raise"
    except RuntimeError as e:
        assert "newer than code supports" in str(e)