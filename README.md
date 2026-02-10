# Kalshi BTC Shadow Trading System

Python 3.11 event-driven system for ingesting Coinbase + Kalshi data, computing model probabilities and fee-aware expected value, and tracking shadow positions end-to-end in SQLite.

This repo is focused on **research + shadow execution**, not live order placement.

## What This Project Does

- Streams Coinbase BTC spot data and polls Kalshi quotes.
- Refreshes Kalshi BTC market/contract metadata across series (`KXBTC`, `KXBTC15M`, `KXBTCD`).
- Computes probabilities via GBM-style pricing with EWMA volatility.
- Produces fee-aware TAKE/PASS decisions with explicit gating and reasons.
- Tracks hypothetical positions, open/unrealized PnL, settled/realized PnL.
- Scores model quality vs outcomes and compares to market-implied baseline.

## Employer-Relevant Engineering Highlights

- Async pipeline with clear stage boundaries (`ingestion -> edges -> opportunities -> settlements -> scoring -> reporting`).
- Deterministic DB-first architecture (SQLite + migrations + idempotent upserts).
- Operational hardening for long-running loops:
  - WAL mode / busy timeouts
  - retry for transient SQLite lock contention
  - component restarts in live stack runner
- Observability:
  - structured JSON app logs
  - trader-style decision tape (`TAKE` and abnormal `PASS` reasons)
  - health snapshots and performance reports
- Evaluation rigor:
  - fee-aware decision EV
  - settled-outcome scoring
  - model-vs-market Brier/logloss deltas
  - shadow ledger for realistic paper-trading feedback loops

## Current Scope

Implemented:
- Data ingestion and market metadata refresh.
- Edge/snapshot/opportunity generation.
- Shadow position tracking and reporting.
- Settlements integration and scored outcomes.
- Offline test suite and migration checks.

Not implemented:
- Live order routing / broker execution.
- Full portfolio optimizer and production orchestration stack.

## Architecture

Coinbase WS + Kalshi REST
-> `spot_ticks` + `kalshi_quotes` + contracts/markets
-> edge engine (`kalshi_edges`, `kalshi_edge_snapshots`)
-> opportunity engine (`opportunities`)
-> settlements + scoring (`kalshi_contracts`, `kalshi_edge_snapshot_scores`)
-> trader/performance reports

## Repo Layout

- `src/kalshi_bot/strategy`: edge math, volatility, opportunity logic, scoring.
- `src/kalshi_bot/kalshi`: Kalshi REST/WS clients + parsing/health utilities.
- `src/kalshi_bot/data`: schema, migrations, DAOs.
- `src/kalshi_bot/app`: orchestrators and health reporting.
- `scripts/`: operational entrypoints (live stack, refresh, scoring, reports).
- `tests/`: offline pytest coverage.

## Quickstart

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

## Run The Stack

Recommended single-command runner:

```bash
./scripts/live_stack.sh
```

This starts:
- Coinbase collector
- Kalshi quote poller
- edge loop
- opportunity loop
- periodic settlements/scoring/performance reporting

## Core Reports

Open + settled shadow positions:

```bash
PYTHONPATH=src .venv/bin/python scripts/report_open_shadow_positions.py
```

Model performance (including model-vs-market baseline):

```bash
PYTHONPATH=src .venv/bin/python scripts/report_model_performance.py --since-seconds 86400
```

## Configuration Notes

Key env vars:
- `DB_PATH`, `LOG_PATH`
- `KALSHI_ENV`, `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH`
- `COINBASE_PRODUCT_ID`, `COINBASE_STALE_SECONDS`

Important runtime defaults (current):
- `run_live_stack.py --quote-seconds 900` (reloads quote universe every 15m)
- `run_live_stack.py --settlements-every-minutes 5`
- `run_live_edges.py --lookback-seconds 3900`
- `run_live_edges.py --max-horizon-seconds 21600` (6h)
- `run_opportunity_loop.py --min-ev 0.03`

## Data Conventions

- Contract prices are cents (`0..100`).
- Probabilities are `0..1`.
- EV and PnL are dollars per contract (fee-aware on entry for shadow realized).
- Horizons are based on `close_ts` (fallbacks: `expected_expiration_ts`, `settlement_ts`).

## Docs

- `EDGE_VALIDATION.md`
- `KALSHI_API_CONTRACT.md`

## License

MIT
