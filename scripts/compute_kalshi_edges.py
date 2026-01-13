from __future__ import annotations

import argparse
import asyncio

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.data import init_db
from kalshi_bot.infra.logging import setup_logger
from kalshi_bot.strategy.edge_engine import compute_edges


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute Kalshi edges from DB")
    parser.add_argument("--product-id", type=str, default="BTC-USD")
    parser.add_argument("--lookback-seconds", type=int, default=3600)
    parser.add_argument("--max-spot-points", type=int, default=500)
    parser.add_argument("--ewma-lambda", type=float, default=0.94)
    parser.add_argument("--min-points", type=int, default=10)
    parser.add_argument("--sigma-floor", type=float, default=0.2)
    parser.add_argument("--sigma-cap", type=float, default=2.0)
    parser.add_argument("--status", type=str, default="active")
    parser.add_argument("--series", action="append", default=["KXBTC", "KXBTC15M"])
    parser.add_argument("--pct-band", type=float, default=3.0)
    parser.add_argument("--top-n", type=int, default=40)
    parser.add_argument("--freshness-seconds", type=int, default=300)
    parser.add_argument("--max-horizon-seconds", type=int, default=10 * 24 * 3600)
    parser.add_argument("--contracts", type=int, default=1)
    parser.add_argument("--debug-market", action="append", default=[])
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()
    logger = setup_logger(settings.log_path)

    print(f"DB path: {settings.db_path}")

    await init_db(settings.db_path)

    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 5000;")
        await conn.commit()

        summary = await compute_edges(
            conn,
            product_id=args.product_id,
            lookback_seconds=args.lookback_seconds,
            max_spot_points=args.max_spot_points,
            ewma_lambda=args.ewma_lambda,
            min_points=args.min_points,
            sigma_floor=args.sigma_floor,
            sigma_cap=args.sigma_cap,
            status=args.status,
            series=args.series,
            pct_band=args.pct_band,
            top_n=args.top_n,
            freshness_seconds=args.freshness_seconds,
            max_horizon_seconds=args.max_horizon_seconds,
            contracts=args.contracts,
            debug_market_ids=args.debug_market or None,
        )

        if "error" in summary:
            print(f"ERROR: {summary['error']}")
            return 1

        print(
            "latest_quote_ts={latest_quote_ts} quote_age_seconds={quote_age_seconds} "
            "quotes_distinct_markets_recent={quotes_distinct_markets_recent} "
            "relevant_with_recent_quotes={relevant_with_recent_quotes}".format(
                **summary
            )
        )

        if args.debug:
            print(f"sample_relevant_ids={summary.get('relevant_ids_sample')}")
            print(
                f"sample_recent_quote_market_ids={summary.get('recent_quote_market_ids_sample')}"
            )
            print(
                f"sample_missing_quote_market_ids={summary.get('missing_quote_sample')}"
            )

        cursor = await conn.execute(
            "SELECT e.market_id, e.prob_yes, e.yes_ask, e.ev_take_yes, "
            "e.settlement_ts, e.horizon_seconds, c.strike_type, c.lower, c.upper "
            "FROM kalshi_edges e "
            "LEFT JOIN kalshi_contracts c ON e.market_id = c.ticker "
            "WHERE e.ts = ? "
            "ORDER BY e.ev_take_yes DESC LIMIT 5",
            (summary["now_ts"],),
        )
        top_yes = await cursor.fetchall()

        cursor = await conn.execute(
            "SELECT e.market_id, e.prob_yes, e.no_ask, e.ev_take_no, "
            "e.settlement_ts, e.horizon_seconds, c.strike_type, c.lower, c.upper "
            "FROM kalshi_edges e "
            "LEFT JOIN kalshi_contracts c ON e.market_id = c.ticker "
            "WHERE e.ts = ? "
            "ORDER BY ev_take_no DESC LIMIT 5",
            (summary["now_ts"],),
        )
        top_no = await cursor.fetchall()

    print(
        "spot_price={spot_price} spot_ts={spot_ts} sigma_annualized={sigma_annualized:.4f}".format(
            **summary
        )
    )
    print(
        "relevant_total={relevant_total} edges_inserted={edges_inserted} skipped={skipped}".format(
            **summary
        )
    )
    if summary.get("skip_reasons"):
        print(f"skip_reasons={summary['skip_reasons']}")

    if top_yes:
        print("top_ev_yes:")
        for (
            market_id,
            prob_yes,
            yes_ask,
            ev,
            settlement_ts,
            horizon_seconds,
            strike_type,
            lower,
            upper,
        ) in top_yes:
            line = (
                f"  {market_id} prob={prob_yes} yes_ask={yes_ask} ev={ev}"
            )
            if args.debug:
                line += (
                    f" settlement_ts={settlement_ts} spot_ts={summary.get('spot_ts')}"
                    f" horizon_seconds={horizon_seconds} strike_type={strike_type}"
                    f" lower={lower} upper={upper}"
                )
            print(line)

    if top_no:
        print("top_ev_no:")
        for (
            market_id,
            prob_yes,
            no_ask,
            ev,
            settlement_ts,
            horizon_seconds,
            strike_type,
            lower,
            upper,
        ) in top_no:
            line = (
                f"  {market_id} prob={prob_yes} no_ask={no_ask} ev={ev}"
            )
            if args.debug:
                line += (
                    f" settlement_ts={settlement_ts} spot_ts={summary.get('spot_ts')}"
                    f" horizon_seconds={horizon_seconds} strike_type={strike_type}"
                    f" lower={lower} upper={upper}"
                )
            print(line)

    if summary["relevant_total"] == 0:
        print("ERROR: no relevant markets")
        return 1

    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
