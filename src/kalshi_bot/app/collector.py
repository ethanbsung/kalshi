from __future__ import annotations

import asyncio

from kalshi_bot.config import load_settings
from kalshi_bot.data import init_db
from kalshi_bot.infra import setup_logger


def main() -> int:
    settings = load_settings()
    logger = setup_logger(settings.log_path)

    asyncio.run(init_db(settings.db_path))

    logger.info(
        "startup",
        extra={
            "db_path": str(settings.db_path),
            "log_path": str(settings.log_path),
            "trading_enabled": settings.trading_enabled,
            "kalshi_env": settings.kalshi_env,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
