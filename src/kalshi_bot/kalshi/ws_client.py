from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable
from urllib import parse

import websockets
from websockets.exceptions import ConnectionClosed

from kalshi_bot.kalshi.auth import KalshiSigner

TickerHandler = Callable[[dict[str, Any]], Awaitable[None]]
SnapshotHandler = Callable[[dict[str, Any]], Awaitable[None]]
DeltaHandler = Callable[[dict[str, Any]], Awaitable[None]]

RAW_MESSAGE_LIMIT = 2000


def _parse_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _implied_no_bid(yes_ask: float | None) -> float | None:
    if yes_ask is None:
        return None
    return 100.0 - yes_ask


def _implied_no_ask(yes_bid: float | None) -> float | None:
    if yes_bid is None:
        return None
    return 100.0 - yes_bid


def parse_ticker_message(
    message: dict[str, Any], received_ts: float
) -> dict[str, Any] | None:
    if message.get("type") != "ticker":
        return None
    msg = message.get("msg")
    if not isinstance(msg, dict):
        return None
    market_ticker = msg.get("market_ticker")
    if not market_ticker:
        return None

    ts = _parse_int(msg.get("ts")) or int(received_ts)
    yes_bid = _parse_float(msg.get("yes_bid"))
    yes_ask = _parse_float(msg.get("yes_ask"))

    return {
        "ts": ts,
        "market_id": market_ticker,
        "price": _parse_float(msg.get("price")),
        "best_yes_bid": yes_bid,
        "best_yes_ask": yes_ask,
        "best_no_bid": _implied_no_bid(yes_ask),
        "best_no_ask": _implied_no_ask(yes_bid),
        "volume": _parse_int(msg.get("volume")),
        "open_interest": _parse_int(msg.get("open_interest")),
        "dollar_volume": _parse_int(msg.get("dollar_volume")),
        "dollar_open_interest": _parse_int(msg.get("dollar_open_interest")),
        "raw_json": json.dumps(message),
    }


def parse_orderbook_snapshot(
    message: dict[str, Any], received_ts: float
) -> dict[str, Any] | None:
    if message.get("type") != "orderbook_snapshot":
        return None
    msg = message.get("msg")
    if not isinstance(msg, dict):
        return None
    market_ticker = msg.get("market_ticker")
    if not market_ticker:
        return None
    yes_levels = msg.get("yes")
    no_levels = msg.get("no")

    return {
        "ts": int(received_ts),
        "market_id": market_ticker,
        "seq": _parse_int(message.get("seq")),
        "yes_bids_json": json.dumps(yes_levels) if yes_levels is not None else None,
        "no_bids_json": json.dumps(no_levels) if no_levels is not None else None,
        "raw_json": json.dumps(message),
    }


def parse_orderbook_delta(
    message: dict[str, Any], received_ts: float
) -> dict[str, Any] | None:
    if message.get("type") != "orderbook_delta":
        return None
    msg = message.get("msg")
    if not isinstance(msg, dict):
        return None
    market_ticker = msg.get("market_ticker")
    if not market_ticker:
        return None

    return {
        "ts": int(received_ts),
        "market_id": market_ticker,
        "seq": _parse_int(message.get("seq")),
        "side": msg.get("side"),
        "price": _parse_float(msg.get("price")),
        "size": _parse_float(msg.get("delta")),
        "raw_json": json.dumps(message),
    }


class KalshiWsClient:
    def __init__(
        self,
        ws_url: str,
        api_key_id: str | None,
        private_key_path: str | None,
        auth_mode: str,
        auth_query_key: str,
        auth_query_signature: str,
        auth_query_timestamp: str,
        market_tickers: list[str] | None,
        logger: logging.Logger,
        message_source: AsyncIterator[dict[str, Any]] | None = None,
    ) -> None:
        self._ws_url = ws_url
        self._api_key_id = api_key_id
        self._private_key_path = (
            Path(private_key_path) if private_key_path is not None else None
        )
        self._auth_mode = auth_mode
        self._auth_query_key = auth_query_key
        self._auth_query_signature = auth_query_signature
        self._auth_query_timestamp = auth_query_timestamp
        self._market_tickers = market_tickers or []
        self._logger = logger
        self._message_source = message_source
        self._signer: KalshiSigner | None = None

        self.connect_attempts = 0
        self.connect_successes = 0
        self.recv_count = 0
        self.parsed_ticker_count = 0
        self.parsed_snapshot_count = 0
        self.parsed_delta_count = 0
        self.error_message_count = 0
        self.close_count = 0
        self._message_count = 0
        self._raw_message_samples: list[str] = []

    def _ensure_signer(self) -> KalshiSigner:
        if not self._api_key_id or not self._private_key_path:
            raise RuntimeError("Kalshi API credentials are required for WS calls")
        if self._signer is None:
            self._signer = KalshiSigner(self._api_key_id, self._private_key_path)
        return self._signer

    def _auth_ws_target(self) -> tuple[str, dict[str, str]]:
        parsed = parse.urlparse(self._ws_url)
        path = parsed.path or "/"
        headers, timestamp, signature = self._ensure_signer().build_headers(
            "GET", path
        )
        if self._auth_mode == "query":
            query = parse.parse_qs(parsed.query)
            query[self._auth_query_key] = [self._api_key_id or ""]
            query[self._auth_query_signature] = [signature]
            query[self._auth_query_timestamp] = [timestamp]
            new_query = parse.urlencode(query, doseq=True)
            ws_url = parse.urlunparse(parsed._replace(query=new_query))
            return ws_url, {}
        return self._ws_url, headers

    def _subscribe_message(self) -> dict[str, Any]:
        params: dict[str, Any] = {"channels": ["orderbook_delta", "ticker"]}
        if self._market_tickers:
            if len(self._market_tickers) == 1:
                params["market_ticker"] = self._market_tickers[0]
            else:
                params["market_tickers"] = self._market_tickers
        return {"id": 1, "cmd": "subscribe", "params": params}

    async def run(
        self,
        on_ticker: TickerHandler,
        on_snapshot: SnapshotHandler,
        on_delta: DeltaHandler,
        run_seconds: int | None = None,
    ) -> None:
        end_time = None
        if run_seconds is not None:
            end_time = time.monotonic() + run_seconds

        backoff = 1.0
        while end_time is None or time.monotonic() < end_time:
            try:
                if self._message_source is not None:
                    await self._consume_source(on_ticker, on_snapshot, on_delta, end_time)
                    break
                await self._consume_websocket(on_ticker, on_snapshot, on_delta, end_time)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._logger.error(
                    "kalshi_ws_exception",
                    extra={
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                        "backoff_seconds": backoff,
                    },
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    async def _consume_source(
        self,
        on_ticker: TickerHandler,
        on_snapshot: SnapshotHandler,
        on_delta: DeltaHandler,
        end_time: float | None,
    ) -> None:
        async for message in self._message_source:
            if end_time is not None and time.monotonic() >= end_time:
                return
            if not isinstance(message, dict):
                self._logger.warning("kalshi_ws_unexpected_message")
                continue
            raw = json.dumps(message, default=str)
            self._note_received(raw)
            self._log_message_summary(message)
            if self._log_error_message(message):
                continue
            await self._handle_message(message, on_ticker, on_snapshot, on_delta)

    async def _consume_websocket(
        self,
        on_ticker: TickerHandler,
        on_snapshot: SnapshotHandler,
        on_delta: DeltaHandler,
        end_time: float | None,
    ) -> None:
        self.connect_attempts += 1
        self._logger.info(
            "kalshi_ws_connecting",
            extra={"ws_url": self._ws_url},
        )
        ws_url, headers = self._auth_ws_target()
        connect_kwargs: dict[str, Any] = {}
        params = inspect.signature(websockets.connect).parameters
        if headers:
            if "additional_headers" in params:
                connect_kwargs["additional_headers"] = headers
            else:
                connect_kwargs["extra_headers"] = headers

        async with websockets.connect(ws_url, **connect_kwargs) as ws:
            self.connect_successes += 1
            self._logger.info(
                "kalshi_ws_connected",
                extra={"ws_url": ws_url},
            )
            payload = self._subscribe_message()
            await ws.send(json.dumps(payload))
            self._logger.info(
                "kalshi_ws_subscribe",
                extra={
                    "channels": payload.get("params", {}).get("channels"),
                    "market_ticker": payload.get("params", {}).get("market_ticker"),
                    "market_tickers": payload.get("params", {}).get("market_tickers"),
                },
            )

            while end_time is None or time.monotonic() < end_time:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                except ConnectionClosed as exc:
                    self.close_count += 1
                    self._logger.warning(
                        "kalshi_ws_closed",
                        extra={"code": exc.code, "reason": exc.reason},
                    )
                    return

                if raw is None:
                    return
                raw_text = (
                    raw.decode("utf-8", errors="replace")
                    if isinstance(raw, (bytes, bytearray))
                    else str(raw)
                )
                self._note_received(raw_text)
                try:
                    message = json.loads(raw_text)
                except json.JSONDecodeError:
                    self._logger.warning("kalshi_ws_bad_json")
                    continue
                if not isinstance(message, dict):
                    self._logger.warning("kalshi_ws_unexpected_message")
                    continue

                self._log_message_summary(message)
                if self._log_error_message(message):
                    continue

                await self._handle_message(message, on_ticker, on_snapshot, on_delta)

    async def _handle_message(
        self,
        message: dict[str, Any],
        on_ticker: TickerHandler,
        on_snapshot: SnapshotHandler,
        on_delta: DeltaHandler,
    ) -> None:
        received_ts = time.time()
        row = parse_ticker_message(message, received_ts)
        if row is not None:
            self.parsed_ticker_count += 1
            await on_ticker(row)
            return

        snapshot = parse_orderbook_snapshot(message, received_ts)
        if snapshot is not None:
            self.parsed_snapshot_count += 1
            await on_snapshot(snapshot)
            return

        delta = parse_orderbook_delta(message, received_ts)
        if delta is not None:
            self.parsed_delta_count += 1
            await on_delta(delta)

    def _log_message_summary(self, message: dict[str, Any]) -> None:
        if self._message_count >= 3:
            return
        self._message_count += 1
        self._logger.info(
            "kalshi_ws_message",
            extra={
                "keys": sorted(message.keys()),
                "type": message.get("type"),
            },
        )

    def _log_error_message(self, message: dict[str, Any]) -> bool:
        if message.get("type") == "error":
            self.error_message_count += 1
            self._logger.error(
                "kalshi_ws_error_message",
                extra={"payload": message},
            )
            return True
        return False

    def _note_received(self, raw_message: str) -> None:
        self.recv_count += 1
        if len(self._raw_message_samples) < 3:
            self._raw_message_samples.append(raw_message[:RAW_MESSAGE_LIMIT])
