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


def load_settings(env_file: str | None = None) -> Settings:
    load_dotenv(env_file)
    raw = {}
    for field in Settings.model_fields.values():
        if field.alias is None:
            continue
        value = os.getenv(field.alias)
        if value is not None:
            raw[field.alias] = value
    return Settings(**raw)
