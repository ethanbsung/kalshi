# Kalshi BTC Threshold Trader

A probability-driven trading system for BTC threshold binaries on Kalshi.

Core idea:
- Estimate the true probability of YES (`p_model`) using spot + volatility + time-to-settlement
- Compare to market-implied probability (`p_market`)
- Trade only when `EV = p_model - (price + costs)` clears a strict threshold
- Prove edge via calibration plots and hypothesis tests (Brier score, log-loss) before sizing up

This repo is structured to be:
- Reliable (reconnects, stale-data detection, audit logs)
- Measurable (every opportunity logged, not just trades)
- Cross-platform (develop on macOS, deploy on Linux)

---

## High-level architecture

Data plane:
1) Coinbase Advanced Trade WebSocket market data (`BTC-USD` ticker, optional level2)
2) Kalshi WebSocket v2 market data (`ticker` + `orderbook_snapshot`/`orderbook_delta`)
3) Kalshi WebSocket v2 authenticated streams (`fill`, optional `market_positions`)
4) Kalshi REST discovery + reconciliation (`/markets`, `/portfolio/positions`)

Decision plane:
5) Volatility estimator (rolling realized vol on spot mid)
6) Probability model (short-horizon diffusion approximation + conservative buffers)
7) Cost model (fees + slippage buffer, later replaced with empirical)
8) EV gating + signal generation

Execution plane:
9) Risk manager (exposure caps, daily loss limit, kill switch)
10) Order manager (limit orders, cancel/replace with rate limiting)
11) Portfolio + reconciliation (fills + REST positions)

Audit / research:
12) SQLite event store for ALL opportunities, orders, fills, settlements, and features
13) Analysis scripts for calibration plots, Brier/log-loss tests, EV bin monotonicity

---

## Important market mechanics (must understand)

### Kalshi orderbook representation
Kalshi orderbooks provide:
- YES bids
- NO bids
and do not directly provide asks.

Asks must be derived from the opposite side bids.

Implement this exactly once and reuse everywhere:
- best_yes_bid = max(YES bids)
- best_no_bid  = max(NO bids)
- best_yes_ask = 1.00 - best_no_bid
- best_no_ask  = 1.00 - best_yes_bid

(Use consistent units: prices in [0,1] in your internal code. Convert to cents/ticks only at IO boundaries.)
In the DB, Kalshi prices are stored in cents as provided by the API.

### Settlement rule
Kalshi BTC markets settle off the **simple average of the final 60 seconds** of CF Benchmarks’ BRTI before the settlement time.
This is not necessarily equal to Coinbase last trade.
Therefore:
- use conservative cost buffers
- avoid trading tiny edges
- avoid new entries inside the final ~2 minutes by default

---

## Repo layout

```text
src/
  kalshi_bot/      # all application code
    app/           # entrypoints (collector/paper/live)
    config/        # typed config loading, env parsing
    infra/         # logging, retry, time utils, shutdown, metrics
    data/          # SQLite helpers + migrations
      migrations/  # ordered SQL migrations (001_*.sql, 002_*.sql)
    feeds/         # Coinbase/Kalshi connectors
    kalshi/        # Kalshi REST + WS clients
    models/        # volatility, probability, cost models
    strategy/      # filters, signals, risk logic
    execution/     # order/portfolio management
tests/
scripts/
README.md
pyproject.toml
```

---

## Environments: macOS dev vs Linux prod

This project is designed to run the same on:
- macOS (development)
- Linux (deployment)

Rules to keep it consistent:
- Python version pinned (see below)
- Use `uv` or `pip` with `requirements.lock` / `pyproject` pinned dependencies
- Use only cross-platform libraries (no OS-specific assumptions)
- All timestamps stored and computed in UTC

Recommended deployment approach:
- Run on Linux in a `tmux` session or as a `systemd` service (see below)

---

## Prerequisites

- Python 3.11.x (required)
- macOS: Xcode command line tools (only for building some Python deps if needed)
- Linux: `python3.11`, `python3.11-venv`, `build-essential` (if needed)

---

## Installation

### Option A (recommended): `uv`
1) Install uv
2) Create venv and install deps

uv venv
source .venv/bin/activate
uv pip install -e .

### Option B: pip + venv

python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .

---

## Configuration

All config is loaded from environment variables.

### Required environment variables

Kalshi (market data ingestion):
- `KALSHI_ENV` = `demo` or `prod`
- `KALSHI_API_KEY_ID`
- `KALSHI_PRIVATE_KEY_PATH`
- `KALSHI_REST_URL` (optional override)
- `KALSHI_WS_URL` (optional override)
- `KALSHI_WS_AUTH_MODE` = `header` or `query` (default `header`)
- `KALSHI_WS_AUTH_QUERY_KEY`, `KALSHI_WS_AUTH_QUERY_SIGNATURE`, `KALSHI_WS_AUTH_QUERY_TIMESTAMP` (optional overrides)
- `KALSHI_MARKET_TICKERS` (comma-separated; required if REST creds are not set)
- `KALSHI_MARKET_STATUS` (default `open`)
- `KALSHI_MARKET_LIMIT` (default `100`)
- `KALSHI_MARKET_MAX_PAGES` (default `1`)
- `KALSHI_EVENT_TICKER`, `KALSHI_SERIES_TICKER` (optional filters)

Coinbase:
- `COINBASE_WS_URL` = `wss://advanced-trade-ws.coinbase.com` (default)
- `COINBASE_PRODUCT_ID` = `BTC-USD` (default)
- `COINBASE_STALE_SECONDS` = `10` (default)
- `COLLECTOR_SECONDS` = `60` (default)
- If you use authenticated Coinbase channels later, add those keys (not required for public ticker)

Storage/logging:
- `DB_PATH` = path to sqlite db (e.g. `./data/kalshi.sqlite`)
- `LOG_PATH` = path to json logs (e.g. `./logs/app.jsonl`)

Safety:
- `TRADING_ENABLED` = `0/1` (hard gate)
- `MAX_DAILY_LOSS_PCT` = e.g. `0.03`
- `MAX_POSITION_PCT` = e.g. `0.02`

### Config defaults (recommended)
Start conservative:
- `TAU_MAX_MINUTES` = 60
- `EV_MIN` = 0.03
- `SPREAD_MAX_TICKS` = 6
- `NO_NEW_ENTRIES_LAST_SECONDS` = 120
- `MAX_OPEN_POSITIONS` = 1 (initially)

---

## Run modes

### 1) Collector (data only)
Purpose:
- verify feeds and parsing
- record orderbooks and spot ticks
- no EV logic required

Run:

python -m kalshi_bot.app.collector

Phase 1A (Coinbase ticker only):

python -m kalshi_bot.app.collector --coinbase --seconds 30

Kalshi market data (ticker + orderbook):

python -m kalshi_bot.app.collector --kalshi --seconds 60 --debug

Debug to stdout + file:

python -m kalshi_bot.app.collector --coinbase --seconds 30 --debug

Check rows (example):

sqlite3 ./data/kalshi.sqlite "SELECT COUNT(*) FROM spot_ticks;"
sqlite3 ./data/kalshi.sqlite "SELECT COUNT(*) FROM kalshi_tickers;"

Definition of done:
- runs for the configured seconds without exceptions
- spot ticks are inserted into `spot_ticks` (Coinbase)
- Kalshi tickers/orderbook rows are inserted when `--kalshi` is enabled

---

### 2) Paper (shadow trading)
Purpose:
- compute `p_model`, `p_market`, EV, and “would trade” decisions
- log every opportunity (even no-trade)
- no orders placed

Run:

python -m kalshi_bot.app.paper

Definition of done:
- not implemented yet (placeholder)

---

### 3) Live (demo/prod)
Purpose:
- place real orders (start in demo)
- small sizing and strict risk gating

Run:

python -m kalshi_bot.app.live

Hard safety requirements before enabling:
- `TRADING_ENABLED=1`
- kill switch wired and tested
- REST reconciliation works
- write-rate limiting is enforced
 
Status:
- not implemented yet (placeholder)

---

## Data model (minimum required tables)

This system is built around proving edge, so you must log opportunities even when you do nothing.

Minimum tables:
- `spot_ticks`
- `kalshi_markets`
- `kalshi_tickers`
- `kalshi_orderbook_snapshots`
- `kalshi_orderbook_deltas`
- `opportunities` (one row per evaluation)
- `orders`
- `fills`
- `positions_snapshots`
- `settlements`
- `features` (sigma, tau, etc.)

See `EDGE_VALIDATION.md` for the exact fields required for proof of edge.

---

## Edge validation workflow

### What “edge” means here
You have edge only if:
- your probability estimates are better calibrated than market-implied probabilities
- Brier score and/or log-loss beat the market with statistical significance
- high predicted EV bins outperform low EV bins
- results persist out-of-sample

### How long it typically takes
- <100 settled observations: too noisy
- ~200: enough to kill obvious bad models
- 300–500: meaningful calibration and hypothesis tests
- 400–600+: out-of-sample confirmation

Do not scale size until these tests pass.

---

## Reliability requirements (do not skip)

Must-have before real trading:
- stale-data detection (Coinbase and Kalshi)
- reconnect with exponential backoff
- orderbook consistency checks (no negative size, seq monotonic)
- write-rate limiter for create/cancel/reprice
- “stop trading on inconsistency” policy
- periodic reconciliation against REST positions endpoint
- kill switch that cancels all open orders and stops the loop

If any of these fail, the bot must stop trading immediately.

---

## Development notes

- All timestamps in UTC
- Use structured JSON logging
- Never hardcode API keys
- Prefer small, testable modules
- Add unit tests for:
  - derived ask/bid logic
  - sigma stability
  - probability monotonicity
  - cost model sanity
  - EV computation

---

## Safety disclaimers

This is an experimental trading system.
- Start in demo
- Size tiny in prod
- Prefer validation over profit early
- If metrics degrade, stop and diagnose

---

## Quickstart checklist

1) Install deps and create venv
2) Set env vars (DB/LOG paths + Coinbase WS as needed)
3) Run collector to create SQLite tables (migrations apply automatically)
4) Run Coinbase collector for ~30 seconds and confirm `spot_ticks` rows exist
5) Run Kalshi collector and confirm `kalshi_markets` + `kalshi_tickers` rows exist
6) Paper/live trading steps are placeholders until trading logic lands
