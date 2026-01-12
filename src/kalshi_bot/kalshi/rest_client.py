from __future__ import annotations

import asyncio
import json
import logging
import random
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import error, parse, request

from kalshi_bot.kalshi.auth import KalshiSigner


@dataclass
class KalshiRestError(RuntimeError):
    status: int | None
    body: str
    transient: bool
    retry_after: float | None = None
    error_type: str | None = None


class KalshiRestClient:
    def __init__(
        self,
        base_url: str,
        api_key_id: str | None,
        private_key_path: Path | None,
        logger: logging.Logger,
        timeout: float = 10.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key_id = api_key_id
        self._private_key_path = private_key_path
        self._logger = logger
        self._timeout = timeout
        self._signer: KalshiSigner | None = None

    def _ensure_signer(self) -> KalshiSigner:
        if not self._api_key_id or not self._private_key_path:
            raise RuntimeError("Kalshi API credentials are required for REST calls")
        if self._signer is None:
            self._signer = KalshiSigner(self._api_key_id, self._private_key_path)
        return self._signer

    def _request_sync(
        self, method: str, path: str, params: dict[str, Any] | None
    ) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        if params:
            query = parse.urlencode({k: v for k, v in params.items() if v is not None})
            url = f"{url}?{query}"
        headers, _, _ = self._ensure_signer().build_headers(method, path)
        req = request.Request(url, headers=headers, method=method)
        try:
            with request.urlopen(req, timeout=self._timeout) as resp:
                payload = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            transient = exc.code in {429, 500, 502, 503, 504}
            retry_after = None
            if exc.code == 429:
                header = exc.headers.get("Retry-After")
                if header is not None:
                    try:
                        retry_after = float(header)
                    except ValueError:
                        retry_after = None
            raise KalshiRestError(exc.code, body, transient, retry_after) from exc
        except error.URLError as exc:
            if isinstance(exc.reason, socket.timeout):
                raise KalshiRestError(
                    None, str(exc), True, error_type="timeout"
                ) from exc
            raise KalshiRestError(None, str(exc), True) from exc
        except TimeoutError as exc:
            raise KalshiRestError(
                None, str(exc), True, error_type="timeout"
            ) from exc

        try:
            return json.loads(payload)
        except json.JSONDecodeError as exc:
            raise KalshiRestError(None, payload, False) from exc

    async def _request_json(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        end_time: float | None = None,
    ) -> dict[str, Any]:
        attempt = 0
        backoff = 1.0
        while True:
            if end_time is not None and time.monotonic() >= end_time:
                raise RuntimeError("kalshi_rest_deadline_exceeded")
            try:
                return await asyncio.to_thread(
                    self._request_sync, method, path, params
                )
            except KalshiRestError as exc:
                if not exc.transient or attempt >= 4:
                    self._logger.error(
                        "kalshi_rest_error",
                        extra={"status": exc.status, "body": exc.body},
                    )
                    raise
                attempt += 1
                if exc.status == 429 and exc.retry_after is not None:
                    delay = max(exc.retry_after, 0.0)
                    self._logger.warning(
                        "kalshi_rest_rate_limited",
                        extra={
                            "attempt": attempt,
                            "retry_after_seconds": delay,
                        },
                    )
                else:
                    jitter = random.uniform(0.0, backoff * 0.25)
                    delay = backoff + jitter
                    self._logger.warning(
                        "kalshi_rest_retry",
                        extra={
                            "status": exc.status,
                            "attempt": attempt,
                            "backoff_seconds": delay,
                        },
                    )
                if end_time is not None and time.monotonic() + delay >= end_time:
                    raise RuntimeError("kalshi_rest_deadline_exceeded") from exc
                await asyncio.sleep(delay)
                backoff = min(backoff * 2.0, 30.0)

    async def list_markets(
        self,
        limit: int | None = None,
        max_pages: int | None = None,
        status: str | None = None,
        event_ticker: str | None = None,
        series_ticker: str | None = None,
        tickers: list[str] | None = None,
        end_time: float | None = None,
    ) -> list[dict[str, Any]]:
        markets: list[dict[str, Any]] = []
        cursor: str | None = None
        pages = 0

        while True:
            params: dict[str, Any] = {
                "limit": limit,
                "cursor": cursor,
                "event_ticker": event_ticker,
                "series_ticker": series_ticker,
                "status": status,
            }
            if tickers:
                params["tickers"] = ",".join(tickers)

            payload = await self._request_json(
                "GET", "/markets", params, end_time=end_time
            )
            batch = payload.get("markets", [])
            if isinstance(batch, list):
                markets.extend(batch)
            cursor = payload.get("cursor")
            pages += 1
            self._logger.info(
                "kalshi_rest_call",
                extra={
                    "method": "GET",
                    "path": "/markets",
                    "limit": limit,
                    "status": status,
                    "cursor": params.get("cursor"),
                    "markets_returned": len(batch) if isinstance(batch, list) else 0,
                },
            )
            if max_pages is not None and pages >= max_pages:
                if cursor:
                    self._logger.info(
                        "kalshi_rest_pagination_stop",
                        extra={"pages": pages, "cursor": cursor},
                    )
                break
            if not cursor:
                break
            await asyncio.sleep(0)
        return markets

    async def list_markets_by_series(
        self,
        series_ticker: str,
        status: str | None = None,
        limit: int | None = None,
        max_pages: int | None = None,
        end_time: float | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        markets: list[dict[str, Any]] = []
        payloads: list[dict[str, Any]] = []
        cursor: str | None = None
        pages = 0

        while True:
            params: dict[str, Any] = {
                "limit": limit,
                "cursor": cursor,
                "status": status,
                "series_ticker": series_ticker,
            }
            payload = await self._request_json(
                "GET", "/markets", params, end_time=end_time
            )
            payloads.append(payload)
            batch = payload.get("markets", [])
            if isinstance(batch, list):
                markets.extend(batch)
            cursor = payload.get("cursor")
            pages += 1
            self._logger.info(
                "kalshi_rest_call",
                extra={
                    "method": "GET",
                    "path": "/markets",
                    "limit": limit,
                    "status": status,
                    "cursor": params.get("cursor"),
                    "series_ticker": series_ticker,
                    "markets_returned": len(batch) if isinstance(batch, list) else 0,
                },
            )
            if max_pages is not None and pages >= max_pages:
                if cursor:
                    self._logger.info(
                        "kalshi_rest_pagination_stop",
                        extra={"pages": pages, "cursor": cursor},
                    )
                break
            if not cursor:
                break
            await asyncio.sleep(0)
        return markets, payloads

    async def get_market(
        self, ticker: str, end_time: float | None = None
    ) -> dict[str, Any]:
        payload = await self._request_json(
            "GET", f"/markets/{ticker}", end_time=end_time
        )
        market = payload.get("market")
        if not isinstance(market, dict):
            raise KalshiRestError(None, "Missing market payload", False)
        return market
