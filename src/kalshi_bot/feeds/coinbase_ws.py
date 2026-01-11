from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Any, AsyncIterator, Awaitable, Callable

import websockets
from websockets.exceptions import ConnectionClosed

RowHandler = Callable[[dict[str, Any]], Awaitable[None]]

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


def _parse_timestamp(value: Any, fallback_ts: float) -> int:
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except ValueError:
            return int(fallback_ts)
    return int(fallback_ts)


def parse_ticker_message(
    message: dict[str, Any], received_ts: float, product_id: str
) -> dict[str, Any] | None:
    if message.get("channel") != "ticker":
        return None

    events = message.get("events")
    if not isinstance(events, list):
        return None

    for event in events:
        if not isinstance(event, dict):
            continue
        if event.get("type") not in {"snapshot", "update"}:
            continue
        tickers = event.get("tickers")
        if not isinstance(tickers, list):
            continue

        for ticker in tickers:
            if not isinstance(ticker, dict):
                continue
            if ticker.get("product_id") != product_id:
                continue

            ts = _parse_timestamp(
                message.get("timestamp")
                or event.get("time")
                or ticker.get("time"),
                received_ts,
            )
            row = {
                "ts": ts,
                "product_id": product_id,
                "price": _parse_float(ticker.get("price")),
                "best_bid": _parse_float(ticker.get("best_bid")),
                "best_ask": _parse_float(ticker.get("best_ask")),
                "bid_qty": _parse_float(ticker.get("best_bid_quantity")),
                "ask_qty": _parse_float(ticker.get("best_ask_quantity")),
                "sequence_num": _parse_int(
                    ticker.get("sequence_num")
                    or event.get("sequence_num")
                    or message.get("sequence_num")
                ),
                "raw_json": json.dumps(message),
            }
            return row

    return None


class CoinbaseWsClient:
    def __init__(
        self,
        ws_url: str,
        product_id: str,
        stale_seconds: int,
        logger: logging.Logger,
        message_source: AsyncIterator[dict[str, Any]] | None = None,
    ) -> None:
        self._ws_url = ws_url
        self._product_id = product_id
        self._stale_seconds = stale_seconds
        self._logger = logger
        self._message_source = message_source
        self.last_ticker_ts: float | None = None
        self.is_stale = False
        self._stale_logged = False
        self._message_count = 0
        self.connect_attempts = 0
        self.connect_successes = 0
        self.recv_count = 0
        self.parsed_row_count = 0
        self.error_message_count = 0
        self.close_count = 0
        self._raw_message_samples: list[str] = []

    async def run(self, on_row: RowHandler, run_seconds: int | None = None) -> None:
        end_time = None
        if run_seconds is not None:
            end_time = time.monotonic() + run_seconds
        start_time = time.monotonic()

        backoff = 1.0
        while end_time is None or time.monotonic() < end_time:
            self._check_no_messages_guard(start_time)
            try:
                if self._message_source is not None:
                    await self._consume_source(on_row, end_time, start_time)
                    break
                await self._consume_websocket(on_row, end_time, start_time)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except RuntimeError:
                raise
            except Exception as exc:
                self._logger.error(
                    "coinbase_ws_exception",
                    extra={
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                        "backoff_seconds": backoff,
                    },
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

        if self.recv_count > 0 and self.parsed_row_count == 0:
            self._logger.error(
                "coinbase_no_parsed_rows",
                extra={"raw_messages": self._raw_message_samples},
            )

    async def _consume_source(
        self, on_row: RowHandler, end_time: float | None, start_time: float
    ) -> None:
        async for message in self._message_source:
            if end_time is not None and time.monotonic() >= end_time:
                return
            self._check_no_messages_guard(start_time)
            if not isinstance(message, dict):
                self._logger.warning("coinbase_ws_unexpected_message")
                continue
            self._note_received(json.dumps(message, default=str))
            self._log_message_summary(message)
            if self._log_error_message(message):
                continue
            await self._handle_message(message, on_row)
        self._check_stale()

    async def _consume_websocket(
        self, on_row: RowHandler, end_time: float | None, start_time: float
    ) -> None:
        self.connect_attempts += 1
        self._logger.info(
            "coinbase_ws_connecting",
            extra={"ws_url": self._ws_url},
        )
        async with websockets.connect(self._ws_url) as ws:
            self.connect_successes += 1
            self._logger.info(
                "coinbase_ws_connected",
                extra={
                    "ws_url": self._ws_url,
                    "product_id": self._product_id,
                },
            )
            for payload in self._subscribe_messages():
                await ws.send(json.dumps(payload))
                self._logger.info(
                    "coinbase_ws_subscribe",
                    extra={
                        "keys": sorted(payload.keys()),
                        "channel": payload.get("channel"),
                        "product_ids": payload.get("product_ids"),
                    },
                )

            while end_time is None or time.monotonic() < end_time:
                self._check_no_messages_guard(start_time)
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    self._check_stale()
                    continue
                except ConnectionClosed as exc:
                    self.close_count += 1
                    self._logger.warning(
                        "coinbase_ws_closed",
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
                    self._logger.warning("coinbase_ws_bad_json")
                    continue
                if not isinstance(message, dict):
                    self._logger.warning("coinbase_ws_unexpected_message")
                    continue
                self._log_message_summary(message)
                if self._log_error_message(message):
                    continue

                await self._handle_message(message, on_row)

    async def _handle_message(
        self, message: dict[str, Any], on_row: RowHandler
    ) -> None:
        received_ts = time.time()
        row = parse_ticker_message(message, received_ts, self._product_id)
        if row is None:
            self._check_stale()
            return

        best_bid = row["best_bid"]
        best_ask = row["best_ask"]
        if best_bid is not None and best_ask is not None and best_bid > best_ask:
            self._logger.error(
                "coinbase_bid_ask_inversion",
                extra={
                    "product_id": self._product_id,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                },
            )

        self.last_ticker_ts = time.monotonic()
        self.is_stale = False
        self._stale_logged = False
        self.parsed_row_count += 1

        await on_row(row)

    def _subscribe_messages(self) -> list[dict[str, Any]]:
        return [
            {
                "type": "subscribe",
                "channel": "ticker",
                "product_ids": [self._product_id],
            },
            {
                "type": "subscribe",
                "channel": "heartbeats",
            },
        ]

    def _check_stale(self) -> None:
        if self.last_ticker_ts is None:
            return
        elapsed = time.monotonic() - self.last_ticker_ts
        if elapsed > self._stale_seconds:
            self.is_stale = True
            if not self._stale_logged:
                self._logger.warning(
                    "coinbase_ticker_stale",
                    extra={
                        "product_id": self._product_id,
                        "stale_seconds": self._stale_seconds,
                    },
                )
                self._stale_logged = True
        else:
            self.is_stale = False

    def _log_message_summary(self, message: dict[str, Any]) -> None:
        if self._message_count >= 3:
            return
        self._message_count += 1
        self._logger.info(
            "coinbase_ws_message",
            extra={
                "keys": sorted(message.keys()),
                "channel": message.get("channel"),
                "type": message.get("type"),
            },
        )

    def _log_error_message(self, message: dict[str, Any]) -> bool:
        if message.get("type") == "error":
            self.error_message_count += 1
            self._logger.error(
                "coinbase_ws_error_message",
                extra={"payload": message},
            )
            return True
        return False

    def _note_received(self, raw_message: str) -> None:
        self.recv_count += 1
        if len(self._raw_message_samples) < 3:
            self._raw_message_samples.append(raw_message[:RAW_MESSAGE_LIMIT])

    def _check_no_messages_guard(self, start_time: float) -> None:
        if time.monotonic() - start_time >= 5.0 and self.recv_count == 0:
            self._logger.error(
                "coinbase_no_messages",
                extra={"ws_url": self._ws_url},
            )
            raise RuntimeError("coinbase_no_messages")
