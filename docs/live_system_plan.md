# Live System Plan (Kalshi BTC)

## Goal
Run the bot 24/7 in **shadow mode** (no order submission), compute edges continuously, log where trades *would* be taken, and track model performance vs. settled outcomes.

## Current State (Done)
- Coinbase spot ingestion to `spot_ticks` via WS collector.
- Kalshi market ingestion for BTC series (`KXBTC`, `KXBTC15M`) into `kalshi_markets`.
- Contract bounds refresh into `kalshi_contracts` (close/expiration timestamps stored).
- Quote polling into `kalshi_quotes`.
- Edge computation into `kalshi_edges` and append-only `kalshi_edge_snapshots`.
- Settlement ingestion script into `kalshi_contracts.outcome/settled_ts`.
- Snapshot scoring into `kalshi_edge_snapshot_scores` (brier/logloss/pnl).
- Health/selection filters for relevance, staleness, and tradability.

## Gaps Before Shadow Trading Is Complete
- “Would-trade” logging with clear entry rules and sizing (recorded to DB).
- Monitoring/alerting (process liveness, data gaps, error rates).
- Daily/weekly evaluation reports (edge quality, calibration, realized pnl).

## Implementation Plan
### Phase 1: Always-On Data Plane (now)
- Run refresh + poll + edge loop 24/7.
- Regular settlement ingestion + scoring backfills.
- Build dashboards/metrics (counts, coverage, staleness, error buckets).

### Phase 2: Model Performance Tracking (now)
- Nightly summaries from `kalshi_edge_snapshot_scores`:
  - Brier/logloss per horizon bucket and series.
  - Realized pnl for “take YES/NO at ask” vs. predicted EV.
- Monitor drift: compare implied mid vs. model prob.

### Phase 3: Shadow Trading Decisions (next)
- Define “would-trade” rules (EV thresholds, spread limits, time-to-close).
- Log each candidate decision with inputs + rationale (no order placement).
- Measure hit-rate vs. market settlement outcomes over time.

## Live Run Checklist (Shadow Mode)
- Set env vars: `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH`, `DB_PATH`, `LOG_PATH`.
- Start pipeline:
  1) `python3 scripts/refresh_btc_markets.py`
  2) `python3 scripts/refresh_kalshi_contracts.py --status active`
  3) `python3 scripts/poll_kalshi_quotes.py --seconds 0 --interval 5 --status active`
  4) `python3 scripts/run_live_edges.py --interval-seconds 10`
  5) `python3 scripts/refresh_kalshi_settlements.py --since-seconds 86400`
  6) `python3 scripts/score_edge_snapshots.py`
- Confirm:
  - `kalshi_quotes` is updating every few seconds.
  - `kalshi_edge_snapshots` rows are being appended.
  - `kalshi_edge_snapshot_scores` grows as outcomes settle.

## Immediate Next Tasks
- Add a supervisor setup (systemd or tmux) with automatic restarts.
- Implement “would-trade” logging and a daily summary script.
- Add alert hooks for data outages and failure spikes.
