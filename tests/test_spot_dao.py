import asyncio
import time

import aiosqlite

from kalshi_bot.data import init_db
from kalshi_bot.data.spot_dao import get_latest_spot, get_spot_history


def test_spot_queries(tmp_path):
    db_path = tmp_path / "spots.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        now = int(time.time())
        async with aiosqlite.connect(db_path) as conn:
            await conn.executemany(
                "INSERT INTO spot_ticks (ts, product_id, price, raw_json) VALUES (?, ?, ?, ?)",
                [
                    (now - 200, "BTC-USD", 30000.0, "{}"),
                    (now - 100, "BTC-USD", 31000.0, "{}"),
                    (now - 150, "ETH-USD", 2000.0, "{}"),
                ],
            )
            await conn.commit()

            latest = await get_latest_spot(conn, "BTC-USD")
            assert latest == (now - 100, 31000.0)

            history = await get_spot_history(
                conn, "BTC-USD", lookback_seconds=500, max_points=10
            )
            assert history == [(now - 200, 30000.0), (now - 100, 31000.0)]

    asyncio.run(_run())
