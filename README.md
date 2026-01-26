# Kalshi BTC Market Data and Edge Analytics

A Python 3.11 system that ingests Coinbase BTC-USD spot data and Kalshi market data, computes model-based probabilities and expected value (EV) for BTC threshold contracts, and stores everything in SQLite for analysis. The execution and trading layer is intentionally not implemented yet.

## Project status

### Implemented
- Async ingestion from Coinbase WebSocket ticker and Kalshi REST/WS market data.
- SQLite schema + migrations with structured JSONL logging.
- Edge engine using GBM (zero drift), EWMA volatility, and fee-aware EV.
- Edge snapshots, opportunity ledger, and scoring (Brier/logloss, realized PnL).
- Offline test suite with mock WebSocket inputs and schema checks.

### Not implemented
- Live or paper order placement (entrypoints are placeholders).
- Portfolio and risk management (placeholders).
- Production deployment or monitoring.

## Highlights
- Clear separation of ingestion, analytics, and execution layers.
- Deterministic, DB-first pipeline with explicit skip reasons and data-quality checks.
- Market selection filters by time horizon, strike bounds, and quote freshness.
- Calibration tooling via snapshot scoring and performance reports.

## Architecture at a glance

Coinbase WS + Kalshi REST/WS
-> ingestion
-> SQLite (spot_ticks, quotes, contracts, edges, snapshots, opportunities)
-> edge engine
-> snapshots and scoring
-> reporting

## Repository layout
- `src/kalshi_bot/app`: collector entrypoints (`collector.py`), live/paper placeholders.
- `src/kalshi_bot/feeds`: Coinbase and Kalshi clients (REST + WS).
- `src/kalshi_bot/strategy`: edge math, snapshots, and opportunity selection.
- `src/kalshi_bot/data`: SQLite schema, migrations, and DAOs.
- `scripts/`: batch jobs and continuous loops.
- `tests/`: offline pytest suite.
- `docs/`: design notes and validation rules.

## Quickstart

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

## Typical pipeline

Ingest spot + market data:
```bash
python -m kalshi_bot.app.collector --coinbase --seconds 30
python -m kalshi_bot.app.collector --kalshi --seconds 60 --debug
```

Refresh markets and contracts, then poll quotes:
```bash
python3 scripts/refresh_btc_markets.py
python3 scripts/refresh_kalshi_contracts.py --status active
python3 scripts/poll_kalshi_quotes.py --seconds 60 --interval 5 --status active
```

Compute edges and snapshots:
```bash
python3 scripts/compute_kalshi_edges.py --debug
python3 scripts/run_live_edges.py --interval-seconds 10
```

Build opportunities and score snapshots:
```bash
python3 scripts/run_opportunity_loop.py --interval-seconds 10
python3 scripts/refresh_kalshi_settlements.py --status resolved
python3 scripts/score_edge_snapshots.py
python3 scripts/report_model_performance.py
```

## Configuration

Core:
- `DB_PATH` (default `./data/kalshi.sqlite`)
- `LOG_PATH` (default `./logs/app.jsonl`)
- `TRADING_ENABLED` (unused; trading not implemented)

Kalshi:
- `KALSHI_ENV` = `demo` or `prod`
- `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH`
- `KALSHI_REST_URL`, `KALSHI_WS_URL`
- `KALSHI_MARKET_TICKERS` (comma-separated override for WS subscriptions)
- `KALSHI_EVENT_TICKER`, `KALSHI_SERIES_TICKER` (REST filters)

Coinbase:
- `COINBASE_WS_URL` (default `wss://advanced-trade-ws.coinbase.com`)
- `COINBASE_PRODUCT_ID` (default `BTC-USD`)
- `COINBASE_STALE_SECONDS`, `COLLECTOR_SECONDS`

Defaults target BTC series markets (`KXBTC`, `KXBTC15M`) but are configurable via script flags and environment variables.

## Data conventions
- Kalshi prices are stored in cents (0-100); EV is stored in dollars.
- Horizon uses `close_ts` and falls back to `expected_expiration_ts` then `settlement_ts` when needed.

## Testing

```bash
pytest
```

## Docs
- `EDGE_VALIDATION.md` describes how edge quality is evaluated.
- `KALSHI_API_CONTRACT.md` documents the API contract and parsing rules.

## License

MIT
