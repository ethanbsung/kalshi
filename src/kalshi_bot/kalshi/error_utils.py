from __future__ import annotations

import asyncio
from typing import Any

from kalshi_bot.kalshi.rest_client import KalshiRestError

ERROR_BUCKETS = (
    "timeout",
    "rate_limited",
    "http_error",
    "auth_error",
    "invalid_payload",
    "unknown",
)


def init_error_counts() -> dict[str, int]:
    return {bucket: 0 for bucket in ERROR_BUCKETS}


def classify_exception(exc: BaseException) -> str:
    if isinstance(exc, KalshiRestError):
        if exc.error_type == "timeout":
            return "timeout"
        if exc.status == 429:
            return "rate_limited"
        if exc.status in {401, 403}:
            return "auth_error"
        if exc.status is not None:
            return "http_error"
        return "unknown"
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "timeout"
    return "unknown"


def add_failed_sample(
    samples: list[str], sample_set: set[str], ticker: Any, limit: int = 10
) -> None:
    if not isinstance(ticker, str) or not ticker:
        return
    if ticker in sample_set:
        return
    if len(samples) >= limit:
        return
    sample_set.add(ticker)
    samples.append(ticker)
