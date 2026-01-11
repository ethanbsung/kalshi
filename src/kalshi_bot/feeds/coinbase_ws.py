from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Any, AsyncIterator, Awaitable, Callable

import websockets

RowHandler = Callable[[dict[str, Any]], Awaitable[None]]


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
        if event.get("type") != "ticker":
            continue
        if event.get("product_id") != product_id:
            continue

        ts = _parse_timestamp(
            message.get("timestamp") or event.get("time"), received_ts
        )
        row = {
            "ts": ts,
            "product_id": product_id,
            "price": _parse_float(event.get("price")),
            "best_bid": _parse_float(event.get("best_bid")),
            "best_ask": _parse_float(event.get("best_ask")),
            "bid_qty": _parse_float(event.get("bid_qty")),
            "ask_qty": _parse_float(event.get("ask_qty")),
            "sequence_num": _parse_int(
                event.get("sequence_num") or message.get("sequence_num")
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

    async def run(self, on_row: RowHandler, run_seconds: int | None = None) -> None:
        end_time = None
        if run_seconds is not None:
            end_time = time.monotonic() + run_seconds

        backoff = 1.0
        while end_time is None or time.monotonic() < end_time:
            try:
                if self._message_source is not None:
                    await self._consume_source(on_row, end_time)
                    return
                await self._consume_websocket(on_row, end_time)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._logger.warning(
                    "coinbase_ws_error",
                    extra={"error": str(exc), "backoff_seconds": backoff},
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    async def _consume_source(
        self, on_row: RowHandler, end_time: float | None
    ) -> None:
        async for message in self._message_source:
            if end_time is not None and time.monotonic() >= end_time:
                return
            if not isinstance(message, dict):
                self._logger.warning("coinbase_ws_unexpected_message")
                continue
            await self._handle_message(message, on_row)
        self._check_stale()

    async def _consume_websocket(
        self, on_row: RowHandler, end_time: float | None
    ) -> None:
        async with websockets.connect(self._ws_url) as ws:
            for payload in self._subscribe_messages():
                await ws.send(json.dumps(payload))

            while end_time is None or time.monotonic() < end_time:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    self._check_stale()
                    continue

                if raw is None:
                    return
                try:
                    message = json.loads(raw)
                except json.JSONDecodeError:
                    self._logger.warning("coinbase_ws_bad_json")
                    continue
                if not isinstance(message, dict):
                    self._logger.warning("coinbase_ws_unexpected_message")
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
                "product_ids": [self._product_id],
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
