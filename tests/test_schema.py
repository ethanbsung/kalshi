import asyncio
import sqlite3

from kalshi_bot.data import init_db


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    # PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
    return {r[1] for r in rows}


def test_schema_tables_exist(tmp_path):
    db_path = tmp_path / "test.sqlite"
    asyncio.run(init_db(db_path))

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        tables = {row[0] for row in rows}

        required_tables = {
            "spot_ticks",
            "spot_sigma_history",
            "kalshi_markets",
            "kalshi_tickers",
            "kalshi_quotes",
            "kalshi_contracts",
            "kalshi_edges",
            "kalshi_edge_snapshots",
            "kalshi_edge_snapshot_scores",
            "kalshi_orderbook_snapshots",
            "kalshi_orderbook_deltas",
            "opportunities",
            "orders",
            "fills",
            "positions_snapshots",
            "settlements",
            "features",
            "schema_version",
        }
        missing = required_tables - tables
        assert not missing, f"Missing tables: {missing}"

        spot_cols = _table_columns(conn, "spot_ticks")
        required_spot_cols = {
            "ts",
            "product_id",
            "price",
            "best_bid",
            "best_ask",
            "bid_qty",
            "ask_qty",
            "sequence_num",
            "raw_json",
        }
        missing_spot = required_spot_cols - spot_cols
        assert not missing_spot, f"spot_ticks missing cols: {missing_spot}"

        market_cols = _table_columns(conn, "kalshi_markets")
        required_market_cols = {
            "market_id",
            "ts_loaded",
            "title",
            "strike",
            "settlement_ts",
            "close_ts",
            "expected_expiration_ts",
            "expiration_ts",
            "status",
            "raw_json",
        }
        missing_markets = required_market_cols - market_cols
        assert not missing_markets, f"kalshi_markets missing cols: {missing_markets}"

        opp_cols = _table_columns(conn, "opportunities")
        required_opp_cols = {
            "ts_eval",
            "market_id",
            "settlement_ts",
            "strike",
            "spot_price",
            "sigma",
            "tau",
            "p_model",
            "p_market",
            "best_yes_bid",
            "best_yes_ask",
            "best_no_bid",
            "best_no_ask",
            "spread",
            "eligible",
            "reason_not_eligible",
            "would_trade",
            "side",
            "ev_raw",
            "ev_net",
            "cost_buffer",
            "raw_json",
        }
        missing_opp = required_opp_cols - opp_cols
        assert not missing_opp, f"opportunities missing cols: {missing_opp}"

        kalshi_ticker_cols = _table_columns(conn, "kalshi_tickers")
        required_kalshi_ticker_cols = {
            "ts",
            "market_id",
            "price",
            "best_yes_bid",
            "best_yes_ask",
            "best_no_bid",
            "best_no_ask",
            "volume",
            "open_interest",
            "dollar_volume",
            "dollar_open_interest",
            "raw_json",
        }
        missing_kalshi = required_kalshi_ticker_cols - kalshi_ticker_cols
        assert not missing_kalshi, f"kalshi_tickers missing cols: {missing_kalshi}"

        kalshi_quote_cols = _table_columns(conn, "kalshi_quotes")
        required_kalshi_quote_cols = {
            "ts",
            "market_id",
            "yes_bid",
            "yes_ask",
            "no_bid",
            "no_ask",
            "yes_mid",
            "no_mid",
            "p_mid",
            "volume",
            "volume_24h",
            "open_interest",
            "raw_json",
        }
        missing_quotes = required_kalshi_quote_cols - kalshi_quote_cols
        assert not missing_quotes, f"kalshi_quotes missing cols: {missing_quotes}"

        kalshi_contract_cols = _table_columns(conn, "kalshi_contracts")
        required_kalshi_contract_cols = {
            "ticker",
            "lower",
            "upper",
            "strike_type",
            "settlement_ts",
            "close_ts",
            "expected_expiration_ts",
            "expiration_ts",
            "settled_ts",
            "outcome",
            "raw_json",
            "updated_ts",
        }
        missing_contracts = required_kalshi_contract_cols - kalshi_contract_cols
        assert not missing_contracts, f"kalshi_contracts missing cols: {missing_contracts}"

        kalshi_edge_cols = _table_columns(conn, "kalshi_edges")
        required_kalshi_edge_cols = {
            "ts",
            "market_id",
            "settlement_ts",
            "horizon_seconds",
            "spot_price",
            "sigma_annualized",
            "prob_yes",
            "yes_bid",
            "yes_ask",
            "no_bid",
            "no_ask",
            "ev_take_yes",
            "ev_take_no",
            "raw_json",
        }
        missing_edges = required_kalshi_edge_cols - kalshi_edge_cols
        assert not missing_edges, f"kalshi_edges missing cols: {missing_edges}"

        snapshot_cols = _table_columns(conn, "kalshi_edge_snapshots")
        required_snapshot_cols = {
            "asof_ts",
            "market_id",
            "settlement_ts",
            "spot_ts",
            "spot_price",
            "sigma_annualized",
            "prob_yes",
            "prob_yes_raw",
            "horizon_seconds",
            "quote_ts",
            "yes_bid",
            "yes_ask",
            "no_bid",
            "no_ask",
            "yes_mid",
            "no_mid",
            "ev_take_yes",
            "ev_take_no",
            "spot_age_seconds",
            "quote_age_seconds",
            "skip_reason",
            "raw_json",
        }
        missing_snapshots = required_snapshot_cols - snapshot_cols
        assert (
            not missing_snapshots
        ), f"kalshi_edge_snapshots missing cols: {missing_snapshots}"

        edge_indexes = conn.execute(
            "PRAGMA index_list(kalshi_edges)"
        ).fetchall()
        edge_index_names = {row[1] for row in edge_indexes}
        assert (
            "idx_kalshi_edges_market_ts" in edge_index_names
        ), "kalshi_edges missing market_id,ts index"

        snapshot_indexes = conn.execute(
            "PRAGMA index_list(kalshi_edge_snapshots)"
        ).fetchall()
        snapshot_index_names = {row[1] for row in snapshot_indexes}
        assert (
            "idx_kalshi_edge_snapshots_market_asof" in snapshot_index_names
        ), "kalshi_edge_snapshots missing market_id,asof_ts index"
        assert (
            "idx_kalshi_edge_snapshots_asof" in snapshot_index_names
        ), "kalshi_edge_snapshots missing asof_ts index"
        assert (
            "idx_kalshi_edge_snapshots_settlement_ts" in snapshot_index_names
        ), "kalshi_edge_snapshots missing settlement_ts index"

        score_cols = _table_columns(conn, "kalshi_edge_snapshot_scores")
        required_score_cols = {
            "asof_ts",
            "market_id",
            "settled_ts",
            "outcome",
            "pnl_take_yes",
            "pnl_take_no",
            "brier",
            "logloss",
            "error",
            "created_ts",
        }
        missing_scores = required_score_cols - score_cols
        assert (
            not missing_scores
        ), f"kalshi_edge_snapshot_scores missing cols: {missing_scores}"

        score_indexes = conn.execute(
            "PRAGMA index_list(kalshi_edge_snapshot_scores)"
        ).fetchall()
        score_index_names = {row[1] for row in score_indexes}
        assert (
            "idx_kalshi_edge_snapshot_scores_settled_ts" in score_index_names
        ), "kalshi_edge_snapshot_scores missing settled_ts index"
        assert (
            "idx_kalshi_edge_snapshot_scores_created_ts" in score_index_names
        ), "kalshi_edge_snapshot_scores missing created_ts index"

        quote_fks = conn.execute(
            "PRAGMA foreign_key_list(kalshi_quotes)"
        ).fetchall()
        assert any(
            row[2] == "kalshi_markets" and row[3] == "market_id"
            for row in quote_fks
        ), "kalshi_quotes missing FK to kalshi_markets"

        quote_indexes = conn.execute(
            "PRAGMA index_list(kalshi_quotes)"
        ).fetchall()
        index_names = {row[1] for row in quote_indexes}
        assert (
            "idx_kalshi_quotes_market_ts" in index_names
        ), "kalshi_quotes missing market_id,ts index"

        sigma_indexes = conn.execute(
            "PRAGMA index_list(spot_sigma_history)"
        ).fetchall()
        sigma_index_names = {row[1] for row in sigma_indexes}
        assert (
            "idx_spot_sigma_history_product_ts" in sigma_index_names
        ), "spot_sigma_history missing product_id,ts index"

        sigma_cols = _table_columns(conn, "spot_sigma_history")
        required_sigma_cols = {
            "ts",
            "product_id",
            "sigma",
            "source",
            "reason",
            "method",
            "lookback_seconds",
            "points",
        }
        missing_sigma = required_sigma_cols - sigma_cols
        assert not missing_sigma, f"spot_sigma_history missing cols: {missing_sigma}"

        opp_indexes = conn.execute(
            "PRAGMA index_list(opportunities)"
        ).fetchall()
        opp_index_names = {row[1] for row in opp_indexes}
        assert (
            "idx_opportunities_unique" in opp_index_names
        ), "opportunities missing unique index"

    finally:
        conn.close()
