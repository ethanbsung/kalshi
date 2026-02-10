from kalshi_bot.kalshi.btc_markets import (
    BTC_SERIES_TICKERS,
    backfill_market_times,
    empty_series_tickers,
    extract_close_ts,
    extract_expected_expiration_ts,
    extract_expiration_ts,
    extract_strike_basic,
)
from kalshi_bot.data import init_db
import asyncio
import aiosqlite


def test_extract_strike_numeric():
    market = {
        "custom_strike": 44500,
        "price_ranges": [{"start": 0, "end": 100, "step": 1}],
    }
    assert extract_strike_basic(market) == 44500.0


def test_extract_strike_numeric_string():
    market = {"custom_strike": "44,500"}
    assert extract_strike_basic(market) == 44500.0


def test_extract_strike_dict_none():
    market = {
        "custom_strike": {"Associated Markets": ["A", "B"]},
        "price_ranges": [{"start": 0, "end": 100, "step": 1}],
    }
    assert extract_strike_basic(market) is None


def test_extract_strike_missing_none():
    market = {"custom_strike": None}
    assert extract_strike_basic(market) is None


def test_extract_close_and_expiration_ts():
    market = {
        "close_time": "2024-12-31T23:59:00Z",
        "expected_expiration_time": "2025-01-01T00:04:00Z",
        "expiration_time": "2025-01-07T00:00:00Z",
    }
    assert extract_close_ts(market) == 1735689540
    assert extract_expected_expiration_ts(market) == 1735689840
    assert extract_expiration_ts(market) == 1736208000


def test_empty_series_tickers():
    per_series = {
        "KXBTC": {"markets": []},
        "KXBTC15M": {"markets": [{"ticker": "A"}]},
    }
    assert empty_series_tickers(per_series) == ["KXBTC"]


def test_btc_series_tickers_includes_daily_series():
    assert "KXBTCD" in BTC_SERIES_TICKERS


def test_backfill_market_times_updates_close_ts(tmp_path):
    db_path = tmp_path / "backfill.sqlite"

    async def _run() -> None:
        await init_db(db_path)
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(
                "INSERT INTO kalshi_markets ("
                "market_id, ts_loaded, settlement_ts, status, raw_json, expiration_ts"
                ") VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "KXBTC-26JAN1222-B91125",
                    1700000000,
                    1768878000,
                    "active",
                    '{"close_time":"2026-01-13T03:00:00Z","expected_expiration_time":"2026-01-13T03:05:00Z","expiration_time":"2026-01-20T03:00:00Z"}',
                    None,
                ),
            )
            await conn.execute(
                "INSERT INTO kalshi_contracts (ticker, lower, upper, strike_type, settlement_ts, expiration_ts, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("KXBTC-26JAN1222-B91125", None, 91125.0, "less", 1768878000, None, 1700000000),
            )
            await conn.commit()

            updated = await backfill_market_times(conn, logger=None)
            assert updated == 1
            await conn.commit()

            row = await (
                await conn.execute(
                    "SELECT settlement_ts, close_ts, expected_expiration_ts, expiration_ts "
                    "FROM kalshi_markets WHERE market_id = ?",
                    ("KXBTC-26JAN1222-B91125",),
                )
            ).fetchone()
            assert row[0] == 1768273200
            assert row[1] == 1768273200
            assert row[2] == 1768273500
            assert row[3] == 1768878000
            assert row[3] - row[0] >= 6 * 24 * 3600

            row = await (await conn.execute(
                "SELECT settlement_ts, close_ts, expected_expiration_ts, expiration_ts "
                "FROM kalshi_contracts WHERE ticker = ?",
                ("KXBTC-26JAN1222-B91125",),
            )).fetchone()
            assert row[0] == 1768273200
            assert row[1] == 1768273200
            assert row[2] == 1768273500
            assert row[3] == 1768878000

    asyncio.run(_run())
