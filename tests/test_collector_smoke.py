import json
import os
import sqlite3
import subprocess
import sys


def test_collector_creates_db_and_writes_log(tmp_path):
    db_path = tmp_path / "kalshi.sqlite"
    log_path = tmp_path / "app.jsonl"

    env = os.environ.copy()
    env["DB_PATH"] = str(db_path)
    env["LOG_PATH"] = str(log_path)
    env["TRADING_ENABLED"] = "0"
    env["KALSHI_ENV"] = "demo"

    # Run collector as a module in a subprocess so we test the real entrypoint.
    proc = subprocess.run(
        [sys.executable, "-m", "kalshi_bot.app.collector"],
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, f"collector failed:\nstdout={proc.stdout}\nstderr={proc.stderr}"

    assert db_path.exists(), "DB file was not created"
    assert log_path.exists(), "Log file was not created"

    # Verify required tables exist
    conn = sqlite3.connect(db_path)
    try:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
    finally:
        conn.close()

    assert "schema_version" in tables
    assert "spot_ticks" in tables
    assert "opportunities" in tables

    # Verify a startup log line exists and is valid JSON
    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) >= 1, "No log lines written"
    obj = json.loads(lines[-1])
    assert obj.get("msg") == "startup"
    assert "ts" in obj