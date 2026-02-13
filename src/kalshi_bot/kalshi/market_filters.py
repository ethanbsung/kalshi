from __future__ import annotations

import json
from typing import Any

import aiosqlite


def normalize_db_status(status: str | None) -> str | None:
    if status is None:
        return None
    normalized = status.strip().lower()
    if not normalized or normalized == "all":
        return None
    return normalized


def normalize_series(series: list[str] | None) -> list[str]:
    if not series:
        return []
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in series:
        if not item:
            continue
        for part in item.split(","):
            part = part.strip()
            if part and part not in seen:
                seen.add(part)
                cleaned.append(part)
    return cleaned


def build_series_clause(
    series: list[str] | None, column: str = "market_id"
) -> tuple[str, list[Any]]:
    cleaned = normalize_series(series)
    if not cleaned:
        return "", []
    clauses: list[str] = []
    params: list[Any] = []
    for value in cleaned:
        clauses.append(f"({column} = ? OR {column} LIKE ?)")
        params.extend([value, f"{value}-%"])
    return "(" + " OR ".join(clauses) + ")", params


async def fetch_status_counts(conn: aiosqlite.Connection) -> dict[str, int]:
    cursor = await conn.execute(
        "SELECT status, COUNT(*) FROM kalshi_markets GROUP BY status ORDER BY status"
    )
    rows = await cursor.fetchall()
    counts: dict[str, int] = {}
    for status, count in rows:
        key = "NULL" if status is None else str(status)
        counts[key] = int(count)
    return counts


def format_status_counts(counts: dict[str, int]) -> str:
    return json.dumps(counts, sort_keys=True)


def no_markets_message(status: str | None, counts: dict[str, int]) -> str:
    return (
        "No markets matched filter status="
        f"{status}. kalshi_markets status values present: {format_status_counts(counts)}"
    )
