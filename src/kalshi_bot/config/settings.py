from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field


class Settings(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    db_path: Path = Field(default=Path("./data/kalshi.sqlite"), alias="DB_PATH")
    log_path: Path = Field(default=Path("./logs/app.jsonl"), alias="LOG_PATH")
    trading_enabled: bool = Field(default=False, alias="TRADING_ENABLED")
    kalshi_env: Literal["demo", "prod"] = Field(default="demo", alias="KALSHI_ENV")

    ev_min: float = Field(default=0.03, alias="EV_MIN")
    tau_max_minutes: int = Field(default=60, alias="TAU_MAX_MINUTES")
    spread_max_ticks: int = Field(default=6, alias="SPREAD_MAX_TICKS")
    no_new_entries_last_seconds: int = Field(
        default=120, alias="NO_NEW_ENTRIES_LAST_SECONDS"
    )
    max_open_positions: int = Field(default=1, alias="MAX_OPEN_POSITIONS")
    max_daily_loss_pct: float = Field(default=0.03, alias="MAX_DAILY_LOSS_PCT")
    max_position_pct: float = Field(default=0.02, alias="MAX_POSITION_PCT")

    coinbase_ws_url: str = Field(
        default="wss://advanced-trade-ws.coinbase.com", alias="COINBASE_WS_URL"
    )
    coinbase_product_id: str = Field(default="BTC-USD", alias="COINBASE_PRODUCT_ID")
    coinbase_stale_seconds: int = Field(default=10, alias="COINBASE_STALE_SECONDS")
    collector_seconds: int = Field(default=60, alias="COLLECTOR_SECONDS")

    kalshi_rest_url: str = Field(
        default="https://api.elections.kalshi.com/trade-api/v2",
        alias="KALSHI_REST_URL",
    )
    kalshi_ws_url: str = Field(
        default="wss://api.elections.kalshi.com/trade-api/ws/v2",
        alias="KALSHI_WS_URL",
    )
    kalshi_api_key_id: str | None = Field(default=None, alias="KALSHI_API_KEY_ID")
    kalshi_private_key_path: Path | None = Field(
        default=None, alias="KALSHI_PRIVATE_KEY_PATH"
    )
    kalshi_ws_auth_mode: Literal["header", "query"] = Field(
        default="header", alias="KALSHI_WS_AUTH_MODE"
    )
    kalshi_ws_auth_query_key: str = Field(
        default="apiKey", alias="KALSHI_WS_AUTH_QUERY_KEY"
    )
    kalshi_ws_auth_query_signature: str = Field(
        default="signature", alias="KALSHI_WS_AUTH_QUERY_SIGNATURE"
    )
    kalshi_ws_auth_query_timestamp: str = Field(
        default="timestamp", alias="KALSHI_WS_AUTH_QUERY_TIMESTAMP"
    )
    kalshi_market_status: str | None = Field(
        default="open", alias="KALSHI_MARKET_STATUS"
    )
    kalshi_market_limit: int = Field(default=100, alias="KALSHI_MARKET_LIMIT")
    kalshi_market_max_pages: int = Field(
        default=1, alias="KALSHI_MARKET_MAX_PAGES"
    )
    kalshi_market_tickers: str | None = Field(
        default=None, alias="KALSHI_MARKET_TICKERS"
    )
    kalshi_event_ticker: str | None = Field(
        default=None, alias="KALSHI_EVENT_TICKER"
    )
    kalshi_series_ticker: str | None = Field(
        default=None, alias="KALSHI_SERIES_TICKER"
    )

    bus_url: str = Field(default="nats://127.0.0.1:4222", alias="BUS_URL")
    bus_stream_retention_hours: int = Field(
        default=168, alias="BUS_STREAM_RETENTION_HOURS"
    )
    bus_consumer_lag_alert_threshold: int = Field(
        default=1000, alias="BUS_CONSUMER_LAG_ALERT_THRESHOLD"
    )

    pg_dsn: str | None = Field(
        default="postgresql://kalshi:kalshi@127.0.0.1:5432/kalshi", alias="PG_DSN"
    )
    pg_pool_min: int = Field(default=2, alias="PG_POOL_MIN")
    pg_pool_max: int = Field(default=10, alias="PG_POOL_MAX")
    pg_statement_timeout_ms: int = Field(
        default=5000, alias="PG_STATEMENT_TIMEOUT_MS"
    )


def load_settings(env_file: str | None = None) -> Settings:
    load_dotenv(env_file)
    raw: dict[str, str] = {}
    for field in Settings.model_fields.values():
        if field.alias is None:
            continue
        value = os.getenv(field.alias)
        if value is not None:
            raw[field.alias] = value
    return Settings.model_validate(raw)
