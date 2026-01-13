# Kalshi BTC Market Data + Edge Engine (Current State)

This repo currently implements **data ingestion + edge computation** for BTC Kalshi markets. It does **not** place orders yet. The intent is to provide a reliable, debuggable pipeline from live data to EV calculations stored in SQLite.

---

## What the system does today

**Ingestion + storage**
- **Coinbase**: public WebSocket ticker for `BTC-USD` into `spot_ticks`.
- **Kalshi REST**:
  - Market discovery limited to **BTC series only**: `KXBTC` and `KXBTC15M` via `GET /trade-api/v2/markets?status=open&series_ticker=...` (no global scanning).
  - Quote polling via `GET /trade-api/v2/markets/{ticker}` into `kalshi_quotes`.
  - Contract refresh via `GET /trade-api/v2/markets/{ticker}` into `kalshi_contracts`.
- **Kalshi WS v2**: optional streaming ticker + orderbook deltas/snapshots into `kalshi_tickers`, `kalshi_orderbook_snapshots`, `kalshi_orderbook_deltas`.

**Edge computation (DB-only)**
- Uses **spot ticks from DB**, **contract bounds**, and **latest quotes** to compute:
  - model probability `prob_yes`
  - EV for **buy YES at ask** and **buy NO at ask** (taker)
- Inserts rows into `kalshi_edges` (one row per market per run).

**Reliability & diagnostics**
- Structured JSON logging to `LOG_PATH`.
- Guardrails for stale data, invalid quotes, and ambiguous contracts.
- Debug output for specific market IDs when requested.

---

## What is NOT implemented yet

- Order placement / execution logic (no trading).
- Portfolio/risk management in production.
- Kalshi authenticated trading endpoints.
- Coinbase REST or orderbook ingestion (ticker only).
- Model calibration workflows (Brier/log-loss plots are planned but not implemented).

---

## BTC market universe (important)

This system **only** targets BTC series tickers:
- `KXBTC`
- `KXBTC15M`

Market discovery uses **Kalshi REST**:
```
GET /trade-api/v2/markets?status=open&series_ticker=KXBTC
GET /trade-api/v2/markets?status=open&series_ticker=KXBTC15M
```
If a series returns **zero markets**, refresh scripts treat it as fatal and log the raw payloads.

No keyword scanning or heuristic detection is used.

---

## Time semantics (critical)

**For probability horizons, use the market close time.**

Stored timestamps:
- `kalshi_markets.close_ts`: **close_time** (preferred)
- `kalshi_markets.expected_expiration_ts`: expected_expiration_time (fallback)
- `kalshi_markets.expiration_ts`: expiration_time (not used for horizons)
- `kalshi_markets.settlement_ts`: **legacy field now equal to close_ts**

`kalshi_contracts` mirrors these same fields.

Edge computation uses:
```
close_ts -> expected_expiration_ts -> settlement_ts
```
If no close time is available, the market is skipped (no guessing).

---

## Data model (key tables + important columns)

**Core ingestion tables**
- `spot_ticks`: `ts`, `product_id`, `price`, `best_bid`, `best_ask`, `raw_json`
- `kalshi_markets`: `market_id`, `title`, `strike`, `close_ts`, `expected_expiration_ts`, `expiration_ts`, `status`, `raw_json`
- `kalshi_quotes`: `ts`, `market_id`, `yes_bid`, `yes_ask`, `no_bid`, `no_ask`, `yes_mid`, `no_mid`, `p_mid`, `volume`, `open_interest`, `raw_json`
- `kalshi_contracts`: `ticker`, `lower`, `upper`, `strike_type`, `close_ts`, `expected_expiration_ts`, `expiration_ts`
- `kalshi_tickers`: WS ticker snapshots (`best_yes_bid/ask`, `best_no_bid/ask`, volume, open_interest)
- `kalshi_orderbook_snapshots` / `kalshi_orderbook_deltas`: raw orderbook levels

**Computed tables**
- `kalshi_edges`: `ts`, `market_id`, `prob_yes`, `yes_ask`, `no_ask`, `ev_take_yes`, `ev_take_no`, `horizon_seconds`, `raw_json`
- `spot_sigma_history`: time series of sigma estimates for fallback and diagnostics

All prices from Kalshi are stored in **cents (0–100)**. EV is stored in **dollars**.

---

## Models & math

**Probability model**
- GBM with **mu = 0** drift in price:
  - `ln(S_T / S_0) ~ N(-0.5*sigma^2*t, sigma^2*t)`
- Probability functions:
  - `less`: `P(S_T <= upper)`
  - `greater`: `P(S_T >= lower)`
  - `between`: `P(lower <= S_T < upper)`
- If inputs invalid (spot <= 0, sigma <= 0, horizon <= 0), probability returns **None**.

**Volatility (sigma)**
- EWMA log-returns from spot history, annualized.
- Guardrails:
  - If sigma is non-finite, <= 0, or above `sigma_max`, it is rejected.
  - Fallback to last stored sigma in `spot_sigma_history` if available.
  - Otherwise use a conservative default (`--sigma-default`).
- Summary output includes `sigma_source` and `sigma_reason` for diagnostics.

**Fees**
- Kalshi taker fee formula:
  - `fee = ceil(0.07 * C * P * (1 - P) * 100) / 100`
  - `P` is price in dollars (`price_cents / 100`).

---

## Edge computation flow (DB-only)

1. Load latest spot from `spot_ticks`.
2. Estimate sigma from recent spot history.
3. Select a **relevant universe** near spot using `kalshi_contracts` bounds.
4. Fetch **latest quote per market_id** within freshness window.
5. Compute `prob_yes` and EV for YES/NO **independently**.
6. Insert into `kalshi_edges` even if one side is missing.

Skip reasons are explicit (e.g., `missing_contract`, `missing_settlement_ts`, `expired_contract`, `missing_quote`, `missing_yes_ask`, `missing_no_ask`).

---

## Kalshi contract parsing behavior

- Primary source of bounds: **payload fields** from REST (`floor_strike`, `cap_strike`, `strike_type`).
- For BTC series (`KXBTC`, `KXBTC15M`): **ticker fallback is disabled**.
- If bounds are missing, strike_type is `None`, and the market is skipped.

---

## Entry points & scripts

### Core collectors
- Coinbase ticker:
  ```bash
  python -m kalshi_bot.app.collector --coinbase --seconds 30
  ```
- Kalshi WS (ticker + orderbook):
  ```bash
  python -m kalshi_bot.app.collector --kalshi --seconds 60 --debug
  ```

### Market refresh (REST)
- Fetch BTC series markets:
  ```bash
  python3 scripts/refresh_btc_markets.py
  ```

### Contract refresh (REST)
```bash
python3 scripts/refresh_kalshi_contracts.py --status active
```

### Quote polling (REST)
```bash
python3 scripts/poll_kalshi_quotes.py --seconds 60 --interval 5 --status active
```

### Edge computation (DB-only)
```bash
python3 scripts/compute_kalshi_edges.py --debug
```

### Full pipeline smoke test
```bash
python3 scripts/smoke_kalshi_pipeline.py --seconds 30 --interval 5
```

### Orderbook / WS smoke test
```bash
KALSHI_ENV=prod KALSHI_MARKET_TICKERS="..." python3 scripts/kalshi_smoke_test.py
```

### Convenience runner
```bash
bash scripts/refresh_all.sh
```

---

## Configuration (env vars)

Core:
- `DB_PATH` (default `./data/kalshi.sqlite`)
- `LOG_PATH` (default `./logs/app.jsonl`)
- `TRADING_ENABLED` (currently unused; trading not implemented)

Kalshi REST/WS:
- `KALSHI_ENV` = `demo` or `prod`
- `KALSHI_API_KEY_ID`
- `KALSHI_PRIVATE_KEY_PATH`
- `KALSHI_REST_URL` (default `https://api.elections.kalshi.com/trade-api/v2`)
- `KALSHI_WS_URL` (default `wss://api.elections.kalshi.com/trade-api/ws/v2`)
- `KALSHI_WS_AUTH_MODE` = `header` or `query`
- `KALSHI_WS_AUTH_QUERY_KEY`, `KALSHI_WS_AUTH_QUERY_SIGNATURE`, `KALSHI_WS_AUTH_QUERY_TIMESTAMP`
- `KALSHI_MARKET_STATUS` (REST filter, default `open`)
- `KALSHI_MARKET_LIMIT` (default `100`)
- `KALSHI_MARKET_MAX_PAGES` (default `1`)
- `KALSHI_MARKET_TICKERS` (comma-separated; overrides REST list for WS)

Coinbase:
- `COINBASE_WS_URL` (default `wss://advanced-trade-ws.coinbase.com`)
- `COINBASE_PRODUCT_ID` (default `BTC-USD`)
- `COINBASE_STALE_SECONDS` (default `10`)
- `COLLECTOR_SECONDS` (default `60`)

Note: Kalshi DB statuses often appear as `active`, while REST listing uses `open`. Scripts that read from DB default to **no status filter**; pass `--status active` explicitly when needed.

---

## Logging

Logs are JSONL written to `LOG_PATH` with fields like:
- timestamp (UTC ISO8601)
- level
- msg
- module
- extra fields (e.g., counters, errors)

Collector `--debug` also logs to stdout.

---

## Testing

Tests are offline and do not require network:
```bash
python -m pytest -q
```

Key test coverage:
- probability model correctness and monotonicity
- fee rounding and EV math
- quote validation and parsing
- market/contract parsing
- DB migrations and schema integrity

---

## Quick sanity checks

Check spot ticks:
```bash
sqlite3 ./data/kalshi.sqlite "SELECT COUNT(*) FROM spot_ticks;"
```

Check Kalshi markets:
```bash
sqlite3 ./data/kalshi.sqlite "SELECT COUNT(*) FROM kalshi_markets;"
```

Check edges:
```bash
sqlite3 ./data/kalshi.sqlite "SELECT COUNT(*) FROM kalshi_edges;"
```

---

## Summary

This repo is currently a **data + analytics pipeline** for BTC Kalshi markets:
- Live data ingestion (Coinbase + Kalshi)
- Deterministic, DB-only edge computation
- Strong diagnostics and schema-backed evidence

It is **not** a trading bot yet. The focus is correctness, reproducibility, and measurable edge before execution logic is added.
