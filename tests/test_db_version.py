import asyncio
import sqlite3

from kalshi_bot.data import init_db


def test_init_db_sets_latest_schema_version(tmp_path):
    db_path = tmp_path / "new.sqlite"
    asyncio.run(init_db(db_path))

    conn = sqlite3.connect(db_path)
    try:
        version = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
        assert version == 16
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "spot_ticks" in tables
    finally:
        conn.close()

def test_init_db_is_idempotent(tmp_path):
    db_path = tmp_path / "idempotent.sqlite"
    asyncio.run(init_db(db_path))
    asyncio.run(init_db(db_path))  # should not raise

    conn = sqlite3.connect(db_path)
    try:
        versions = conn.execute("SELECT version FROM schema_version ORDER BY version").fetchall()
        assert [v[0] for v in versions] == [
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
        ]
    finally:
        conn.close()
