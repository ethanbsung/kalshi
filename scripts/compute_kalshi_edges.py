from __future__ import annotations

import argparse
import asyncio

import aiosqlite

from kalshi_bot.config import load_settings
from kalshi_bot.data import init_db
from kalshi_bot.kalshi.btc_markets import BTC_SERIES_TICKERS
from kalshi_bot.strategy.edge_engine import compute_edges


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute Kalshi edges from DB")
    parser.add_argument("--product-id", type=str, default="BTC-USD")
    parser.add_argument(
        "--lookback-seconds",
        type=int,
        default=3900,
        help="History window (seconds) to query spot_ticks for sigma.",
    )
    parser.add_argument("--max-spot-points", type=int, default=20000)
    parser.add_argument("--ewma-lambda", type=float, default=0.94)
    parser.add_argument("--min-points", type=int, default=10)
    parser.add_argument(
        "--min-sigma-lookback-seconds",
        type=int,
        default=3600,
        help="Minimum spot history span (seconds) required for valid sigma.",
    )
    parser.add_argument(
        "--sigma-resample-seconds",
        type=int,
        default=5,
        help="Bucket size (seconds) for spot resampling before sigma.",
    )
    parser.add_argument("--sigma-default", type=float, default=0.6)
    parser.add_argument("--sigma-max", type=float, default=5.0)
    parser.add_argument(
        "--sigma-floor",
        type=float,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--sigma-cap",
        type=float,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--status", type=str, default="active")
    parser.add_argument("--series", action="append", default=list(BTC_SERIES_TICKERS))
    parser.add_argument("--pct-band", type=float, default=3.0)
    parser.add_argument("--top-n", type=int, default=40)
    parser.add_argument("--freshness-seconds", type=int, default=300)
    parser.add_argument("--max-horizon-seconds", type=int, default=6 * 3600)
    parser.add_argument("--min-ask-cents", type=float, default=1.0)
    parser.add_argument("--max-ask-cents", type=float, default=99.0)
    parser.add_argument("--contracts", type=int, default=1)
    parser.add_argument("--debug-market", action="append", default=[])
    parser.add_argument("--debug", action="store_true")
    parser.add_argument(
        "--show-titles",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    return parser.parse_args()


async def _run() -> int:
    args = _parse_args()
    settings = load_settings()

    print(f"DB path: {settings.db_path}")

    await init_db(settings.db_path)

    async with aiosqlite.connect(settings.db_path) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.execute("PRAGMA journal_mode = WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute("PRAGMA busy_timeout = 5000;")
        await conn.commit()

        sigma_default = (
            args.sigma_floor
            if args.sigma_floor is not None
            else args.sigma_default
        )
        sigma_max = (
            args.sigma_cap
            if args.sigma_cap is not None
            else args.sigma_max
        )

        summary = await compute_edges(
            conn,
            product_id=args.product_id,
            lookback_seconds=args.lookback_seconds,
            max_spot_points=args.max_spot_points,
            ewma_lambda=args.ewma_lambda,
            min_points=args.min_points,
            min_sigma_lookback_seconds=args.min_sigma_lookback_seconds,
            resample_seconds=args.sigma_resample_seconds,
            sigma_default=sigma_default,
            sigma_max=sigma_max,
            status=args.status,
            series=args.series,
            pct_band=args.pct_band,
            top_n=args.top_n,
            freshness_seconds=args.freshness_seconds,
            max_horizon_seconds=args.max_horizon_seconds,
            contracts=args.contracts,
            min_ask_cents=args.min_ask_cents,
            max_ask_cents=args.max_ask_cents,
            debug_market_ids=args.debug_market or None,
        )

        if "error" in summary:
            print(f"ERROR: {summary['error']}")
            selection = summary.get("selection", {})
            if selection:
                selection_counts = {
                    "now_ts": selection.get("now_ts"),
                    "candidate_count_total": selection.get("candidate_count_total"),
                    "excluded_expired": selection.get("excluded_expired"),
                    "excluded_horizon_out_of_range": selection.get(
                        "excluded_horizon_out_of_range"
                    ),
                    "excluded_missing_bounds": selection.get(
                        "excluded_missing_bounds"
                    ),
                    "excluded_missing_recent_quote": selection.get(
                        "excluded_missing_recent_quote"
                    ),
                    "excluded_untradable": selection.get("excluded_untradable"),
                    "selected_count": selection.get("selected_count"),
                    "method": selection.get("method"),
                    "require_quotes": selection.get("require_quotes"),
                }
                print(f"selection_summary={selection_counts}")
            skip_reasons = summary.get("skip_reasons")
            if skip_reasons:
                print(f"skip_reasons={skip_reasons}")
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
            if args.show_titles:
                print(
                    f"sample_relevant_titles={summary.get('relevant_titles_sample')}"
                )
            print(
                f"sample_recent_quote_market_ids={summary.get('recent_quote_market_ids_sample')}"
            )
            print(
                f"sample_missing_quote_market_ids={summary.get('missing_quote_sample')}"
            )
            print(
                "sigma_source={sigma_source} sigma_ok={sigma_ok} "
                "sigma_quality={sigma_quality} sigma_reason={sigma_reason} "
                "sigma_points_used={sigma_points_used} "
                "sigma_lookback_seconds_used={sigma_lookback_seconds_used} "
                "min_sigma_lookback_seconds={min_sigma_lookback_seconds} "
                "resample_seconds={resample_seconds} step_seconds={step_seconds}".format(
                    **summary
                )
            )
            selection = summary.get("selection", {})
            if selection:
                selection_counts = {
                    "now_ts": selection.get("now_ts"),
                    "candidate_count_total": selection.get("candidate_count_total"),
                    "excluded_expired": selection.get("excluded_expired"),
                    "excluded_horizon_out_of_range": selection.get(
                        "excluded_horizon_out_of_range"
                    ),
                    "excluded_missing_bounds": selection.get(
                        "excluded_missing_bounds"
                    ),
                    "excluded_missing_recent_quote": selection.get(
                        "excluded_missing_recent_quote"
                    ),
                    "excluded_untradable": selection.get("excluded_untradable"),
                    "selected_count": selection.get("selected_count"),
                    "method": selection.get("method"),
                }
                print(f"selection_summary={selection_counts}")
                print(
                    f"selection_samples={selection.get('selection_samples')}"
                )

            cursor = await conn.execute(
                """
                WITH edge_ids AS (
                    SELECT market_id
                    FROM kalshi_edges
                    WHERE ts = ?
                    ORDER BY market_id
                    LIMIT 5
                ),
                latest AS (
                    SELECT q.market_id, MAX(q.ts) AS max_ts
                    FROM kalshi_quotes q
                    JOIN edge_ids e ON q.market_id = e.market_id
                    GROUP BY q.market_id
                )
                SELECT e.market_id, m.title, e.prob_yes, e.yes_ask, e.no_ask,
                       e.ev_take_yes, e.ev_take_no, q.yes_mid, q.no_mid
                FROM kalshi_edges e
                JOIN edge_ids i ON e.market_id = i.market_id
                LEFT JOIN kalshi_markets m ON e.market_id = m.market_id
                LEFT JOIN latest l ON e.market_id = l.market_id
                LEFT JOIN kalshi_quotes q
                    ON q.market_id = l.market_id AND q.ts = l.max_ts
                WHERE e.ts = ?
                ORDER BY e.market_id
                """,
                (summary["now_ts"], summary["now_ts"]),
            )
            sanity_rows = await cursor.fetchall()
            if sanity_rows:
                print("sanity_check_samples:")
                for (
                    market_id,
                    title,
                    prob_yes,
                    yes_ask,
                    no_ask,
                    ev_yes,
                    ev_no,
                    yes_mid,
                    no_mid,
                ) in sanity_rows:
                    implied_yes_mid = (
                        yes_mid / 100.0 if yes_mid is not None else None
                    )
                    implied_yes_from_no_mid = (
                        1.0 - (no_mid / 100.0) if no_mid is not None else None
                    )
                    title_text = f' title="{title}"' if args.show_titles else ""
                    print(
                        "  {market_id}{title_text} prob_yes={prob_yes} "
                        "implied_yes_mid={implied_yes_mid} "
                        "implied_yes_from_no_mid={implied_yes_from_no_mid} "
                        "yes_ask={yes_ask} no_ask={no_ask} "
                        "ev_yes={ev_yes} ev_no={ev_no}".format(
                            market_id=market_id,
                            title_text=title_text,
                            prob_yes=prob_yes,
                            implied_yes_mid=implied_yes_mid,
                            implied_yes_from_no_mid=implied_yes_from_no_mid,
                            yes_ask=yes_ask,
                            no_ask=no_ask,
                            ev_yes=ev_yes,
                            ev_no=ev_no,
                        )
                    )

            samples = selection.get("selection_samples") if selection else None
            if samples:
                sample_ids = [item["ticker"] for item in samples][:5]
                placeholders = ",".join("?" for _ in sample_ids)
                sample_sql = f"""  # nosec B608
                    WITH latest AS (
                        SELECT market_id, MAX(ts) AS max_ts
                        FROM kalshi_quotes
                        WHERE market_id IN ({placeholders})
                        GROUP BY market_id
                    )
                    SELECT c.ticker, m.title, c.lower, c.upper, c.strike_type,
                           COALESCE(c.close_ts, c.expected_expiration_ts, c.settlement_ts) AS close_ts,
                           q.ts, q.yes_bid, q.yes_ask, q.no_bid, q.no_ask
                    FROM kalshi_contracts c
                    LEFT JOIN kalshi_markets m ON c.ticker = m.market_id
                    LEFT JOIN latest l ON c.ticker = l.market_id
                    LEFT JOIN kalshi_quotes q
                        ON q.market_id = l.market_id AND q.ts = l.max_ts
                    WHERE c.ticker IN ({placeholders})
                    ORDER BY c.ticker
                    """
                cursor = await conn.execute(
                    sample_sql,
                    [*sample_ids, *sample_ids],
                )
                rows = await cursor.fetchall()
                if rows:
                    print("selection_sample_details:")
                    for (
                        ticker,
                        title,
                        lower,
                        upper,
                        strike_type,
                        close_ts,
                        quote_ts,
                        yes_bid,
                        yes_ask,
                        no_bid,
                        no_ask,
                    ) in rows:
                        horizon_seconds = (
                            int(close_ts) - summary["now_ts"]
                            if close_ts is not None
                            else None
                        )
                        prob_horizon_seconds = (
                            int(close_ts) - summary["spot_ts"]
                            if close_ts is not None
                            else None
                        )
                        quote_age = (
                            summary["now_ts"] - int(quote_ts)
                            if quote_ts is not None
                            else None
                        )

                        def _tradable(ask: float | None, bid: float | None) -> bool:
                            if ask is None:
                                return False
                            if ask < 1.0 or ask > 99.0:
                                return False
                            if bid is None:
                                return True
                            if ask < bid:
                                return False
                            return True

                        yes_tradable = _tradable(
                            yes_ask, yes_bid
                        )
                        no_tradable = _tradable(no_ask, no_bid)
                        title_text = f' title="{title}"' if args.show_titles else ""
                        print(
                            f"  {ticker}{title_text} strike_type={strike_type} lower={lower} upper={upper} "
                            f"close_ts={close_ts} selection_horizon_seconds={horizon_seconds} "
                            f"prob_horizon_seconds={prob_horizon_seconds} "
                            f"quote_ts={quote_ts} quote_age={quote_age} "
                            f"yes_ask={yes_ask} no_ask={no_ask} "
                            f"yes_tradable={yes_tradable} no_tradable={no_tradable}"
                        )

        cursor = await conn.execute(
            "SELECT e.market_id, m.title, e.prob_yes, e.yes_ask, e.ev_take_yes, "
            "e.settlement_ts, e.horizon_seconds, c.strike_type, c.lower, c.upper "
            "FROM kalshi_edges e "
            "LEFT JOIN kalshi_contracts c ON e.market_id = c.ticker "
            "LEFT JOIN kalshi_markets m ON e.market_id = m.market_id "
            "WHERE e.ts = ? "
            "ORDER BY e.ev_take_yes DESC LIMIT 5",
            (summary["now_ts"],),
        )
        top_yes = await cursor.fetchall()

        cursor = await conn.execute(
            "SELECT e.market_id, m.title, e.prob_yes, e.no_ask, e.ev_take_no, "
            "e.settlement_ts, e.horizon_seconds, c.strike_type, c.lower, c.upper "
            "FROM kalshi_edges e "
            "LEFT JOIN kalshi_contracts c ON e.market_id = c.ticker "
            "LEFT JOIN kalshi_markets m ON e.market_id = m.market_id "
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
            title,
            prob_yes,
            yes_ask,
            ev,
            settlement_ts,
            horizon_seconds,
            strike_type,
            lower,
            upper,
        ) in top_yes:
            title_text = f' title="{title}"' if args.show_titles else ""
            line = (
                f"  {market_id}{title_text} prob={prob_yes} yes_ask={yes_ask} ev={ev}"
            )
            if args.debug:
                selection_horizon = (
                    settlement_ts - summary.get("now_ts", 0)
                    if settlement_ts is not None
                    else None
                )
                line += (
                    f" settlement_ts={settlement_ts} spot_ts={summary.get('spot_ts')}"
                    f" prob_horizon_seconds={horizon_seconds} "
                    f"selection_horizon_seconds={selection_horizon} "
                    f"strike_type={strike_type}"
                    f" lower={lower} upper={upper}"
                )
            print(line)

    if top_no:
        print("top_ev_no:")
        for (
            market_id,
            title,
            prob_yes,
            no_ask,
            ev,
            settlement_ts,
            horizon_seconds,
            strike_type,
            lower,
            upper,
        ) in top_no:
            title_text = f' title="{title}"' if args.show_titles else ""
            line = (
                f"  {market_id}{title_text} prob={prob_yes} no_ask={no_ask} ev={ev}"
            )
            if args.debug:
                selection_horizon = (
                    settlement_ts - summary.get("now_ts", 0)
                    if settlement_ts is not None
                    else None
                )
                line += (
                    f" settlement_ts={settlement_ts} spot_ts={summary.get('spot_ts')}"
                    f" prob_horizon_seconds={horizon_seconds} "
                    f"selection_horizon_seconds={selection_horizon} "
                    f"strike_type={strike_type}"
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
