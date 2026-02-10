from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Iterable

import aiosqlite
from importlib import resources

MIGRATIONS_PACKAGE = "kalshi_bot.data.migrations"


def _iter_migration_files() -> Iterable[tuple[int, str]]:
    files = resources.files(MIGRATIONS_PACKAGE).iterdir()
    migrations = []
    for path in files:
        if path.suffix != ".sql":
            continue
        name = path.name
        version_str = name.split("_", 1)[0]
        try:
            version = int(version_str)
        except ValueError:
            continue
        migrations.append((version, name))
    return sorted(migrations, key=lambda item: item[0])


def _read_migration_sql(filename: str) -> str:
    return resources.files(MIGRATIONS_PACKAGE).joinpath(filename).read_text(
        encoding="utf-8"
    )


def _split_migration_sections(sql: str) -> dict[str, str]:
    sections: dict[str, list[str]] = {}
    current = None
    for line in sql.splitlines():
        if line.startswith("-- ") and line[3:].strip():
            current = line[3:].strip()
            sections.setdefault(current, [])
            continue
        if current is not None:
            sections[current].append(line)
    return {key: "\n".join(lines).strip() for key, lines in sections.items()}


async def _get_table_columns(conn: aiosqlite.Connection, table: str) -> set[str]:
    cursor = await conn.execute(f"PRAGMA table_info({table})")
    rows = await cursor.fetchall()
    return {row[1] for row in rows}


async def _has_quotes_fk(conn: aiosqlite.Connection) -> bool:
    try:
        cursor = await conn.execute("PRAGMA foreign_key_list(kalshi_quotes)")
    except sqlite3.OperationalError:
        return False
    rows = await cursor.fetchall()
    for row in rows:
        table = row[2]
        from_col = row[3]
        to_col = row[4]
        if table == "kalshi_markets" and from_col == "market_id" and to_col == "market_id":
            return True
    return False


async def _has_snapshot_unique_market_asof_index(
    conn: aiosqlite.Connection,
) -> bool:
    try:
        cursor = await conn.execute("PRAGMA index_list(kalshi_edge_snapshots)")
    except sqlite3.OperationalError:
        return False
    rows = await cursor.fetchall()
    for row in rows:
        name = row[1]
        unique = row[2]
        if name == "idx_kalshi_edge_snapshots_unique_market_asof" and int(unique) == 1:
            return True
    return False


async def _get_current_version(conn: aiosqlite.Connection) -> int:
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_version ("
        "version INTEGER PRIMARY KEY, applied_ts INTEGER NOT NULL)"
    )
    cursor = await conn.execute("SELECT MAX(version) FROM schema_version")
    row = await cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _supports_rename_column() -> bool:
    return sqlite3.sqlite_version_info >= (3, 25, 0)


async def _migration_sql_for_version(
    conn: aiosqlite.Connection, version: int, filename: str
) -> str | None:
    sql = _read_migration_sql(filename)
    if version == 2:
        columns = await _get_table_columns(conn, "spot_ticks")
        if "symbol" not in columns or "product_id" in columns:
            return "BEGIN; COMMIT;"

        sections = _split_migration_sections(sql)
        if _supports_rename_column():
            return sections.get("rename_column")
        return sections.get("recreate_table")
    if version == 6:
        columns = await _get_table_columns(conn, "kalshi_tickers")
        if "dollar_open_interest" in columns:
            return "BEGIN; COMMIT;"
        return sql
    if version == 7:
        if await _has_quotes_fk(conn):
            return "BEGIN; COMMIT;"
        return sql
    if version == 9:
        market_cols = await _get_table_columns(conn, "kalshi_markets")
        contract_cols = await _get_table_columns(conn, "kalshi_contracts")
        if "expiration_ts" in market_cols and "expiration_ts" in contract_cols:
            return "BEGIN; COMMIT;"
        return sql
    if version == 10:
        market_cols = await _get_table_columns(conn, "kalshi_markets")
        contract_cols = await _get_table_columns(conn, "kalshi_contracts")
        if (
            "close_ts" in market_cols
            and "expected_expiration_ts" in market_cols
            and "close_ts" in contract_cols
            and "expected_expiration_ts" in contract_cols
        ):
            return "BEGIN; COMMIT;"
        return sql
    if version == 11:
        sigma_cols = await _get_table_columns(conn, "spot_sigma_history")
        if sigma_cols:
            return "BEGIN; COMMIT;"
        return sql
    if version == 12:
        sigma_cols = await _get_table_columns(conn, "spot_sigma_history")
        required = {"method", "lookback_seconds", "points"}
        if required.issubset(sigma_cols):
            return "BEGIN; COMMIT;"
        return sql
    if version == 13:
        tables = await _get_table_columns(conn, "kalshi_edge_snapshots")
        if tables:
            return "BEGIN; COMMIT;"
        return sql
    if version == 14:
        score_cols = await _get_table_columns(
            conn, "kalshi_edge_snapshot_scores"
        )
        contract_cols = await _get_table_columns(conn, "kalshi_contracts")
        has_settled_ts = "settled_ts" in contract_cols
        has_outcome = "outcome" in contract_cols
        has_score_table = bool(score_cols)
        has_snapshot_unique = await _has_snapshot_unique_market_asof_index(conn)
        if (
            has_score_table
            and has_settled_ts
            and has_outcome
            and has_snapshot_unique
        ):
            return "BEGIN; COMMIT;"
        statements: list[str] = ["BEGIN;"]
        if not has_settled_ts:
            statements.append(
                "ALTER TABLE kalshi_contracts ADD COLUMN settled_ts INTEGER;"
            )
        if not has_outcome:
            statements.append("ALTER TABLE kalshi_contracts ADD COLUMN outcome INTEGER;")
        if not has_snapshot_unique:
            statements.append(
                "DELETE FROM kalshi_edge_snapshots "
                "WHERE id IN ("
                "    SELECT s.id "
                "    FROM kalshi_edge_snapshots s "
                "    JOIN ("
                "        SELECT market_id, asof_ts, MIN(id) AS keep_id "
                "        FROM kalshi_edge_snapshots "
                "        GROUP BY market_id, asof_ts "
                "        HAVING COUNT(*) > 1"
                "    ) d "
                "      ON s.market_id = d.market_id "
                "     AND s.asof_ts = d.asof_ts "
                "    WHERE s.id <> d.keep_id"
                ");"
            )
            statements.append(
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                "idx_kalshi_edge_snapshots_unique_market_asof "
                "ON kalshi_edge_snapshots(market_id, asof_ts);"
            )
        if not has_score_table:
            statements.append(
                "CREATE TABLE IF NOT EXISTS kalshi_edge_snapshot_scores ("
                "    asof_ts INTEGER NOT NULL,"
                "    market_id TEXT NOT NULL,"
                "    settled_ts INTEGER,"
                "    outcome INTEGER,"
                "    pnl_take_yes REAL,"
                "    pnl_take_no REAL,"
                "    brier REAL,"
                "    logloss REAL,"
                "    error TEXT,"
                "    created_ts INTEGER NOT NULL,"
                "    PRIMARY KEY (market_id, asof_ts),"
                "    FOREIGN KEY (market_id, asof_ts)"
                "        REFERENCES kalshi_edge_snapshots(market_id, asof_ts)"
                ");"
            )
        statements.append(
            "CREATE INDEX IF NOT EXISTS idx_kalshi_edge_snapshot_scores_settled_ts "
            "ON kalshi_edge_snapshot_scores(settled_ts);"
        )
        statements.append(
            "CREATE INDEX IF NOT EXISTS idx_kalshi_edge_snapshot_scores_created_ts "
            "ON kalshi_edge_snapshot_scores(created_ts);"
        )
        statements.append("COMMIT;")
        return "\n".join(statements)
    if version == 15:
        contract_cols = await _get_table_columns(conn, "kalshi_contracts")
        if "raw_json" in contract_cols:
            return "BEGIN; COMMIT;"
        return sql
    if version == 16:
        try:
            cursor = await conn.execute("PRAGMA index_list(opportunities)")
        except sqlite3.OperationalError:
            return sql
        rows = await cursor.fetchall()
        index_names = {row[1] for row in rows}
        if "idx_opportunities_unique" in index_names:
            return "BEGIN; COMMIT;"
        return sql
    if version == 17:
        if await _has_snapshot_unique_market_asof_index(conn):
            return "BEGIN; COMMIT;"
        return sql
    return sql


async def _apply_migration(
    conn: aiosqlite.Connection, version: int, filename: str
) -> None:
    sql = await _migration_sql_for_version(conn, version, filename)
    if not sql:
        return
    await conn.executescript(sql)
    await conn.execute(
        "INSERT INTO schema_version (version, applied_ts) "
        "VALUES (?, strftime('%s','now'))",
        (version,),
    )
    await conn.commit()


async def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    async with aiosqlite.connect(db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 5000;")

        current_version = await _get_current_version(conn)
        known_latest = max((v for v, _ in _iter_migration_files()), default=0)
        if current_version > known_latest:
            raise RuntimeError(
                f"DB schema_version={current_version} is newer than code supports (latest={known_latest})"
            )
        for version, filename in _iter_migration_files():
            if version <= current_version:
                continue
            await _apply_migration(conn, version, filename)
