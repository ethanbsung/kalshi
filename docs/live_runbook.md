# Live Stack Runbook

## Goal
Run the full shadow-mode stack with one command:

- Coinbase spot ingestion
- Kalshi quote polling (REST, used by edge engine)
- Live edge + snapshot loop
- Opportunity loop
- Periodic settlements refresh + snapshot scoring (+ optional report)

No order execution is performed.

## Prerequisites

1. Create and activate the Python environment.
2. Install dependencies.
3. Set environment variables in `.env` (or shell):
   - `DB_PATH`
   - `LOG_PATH`
   - `COINBASE_PRODUCT_ID` (optional, defaults to `BTC-USD`)
   - `KALSHI_API_KEY_ID`
   - `KALSHI_PRIVATE_KEY_PATH`
   - `KALSHI_REST_URL` (optional default exists)
   - `KALSHI_WS_URL` (optional default exists)
4. Ensure migrations can run.

## Start Command

```bash
./scripts/live_stack.sh
```

`scripts/live_stack.sh` uses these defaults:
- `--status active`
- `--edge-interval-seconds 10`
- `--opportunity-interval-seconds 10`
- `--settlements-every-minutes 360`
- `--scoring-every-minutes 60`
- `--report-every-minutes 60`

Override defaults by passing extra flags:

```bash
./scripts/live_stack.sh \
  --settlements-every-minutes 10 \
  --scoring-every-minutes 10
```

Stop with `Ctrl+C` (the orchestrator terminates all child processes).

Decision log (trader tape):
- Default path: `logs/trader_tape.log`
- Includes: tick summary, all `TAKE` lines, compact `PASS_SUMMARY`, and a few `PASS_SAMPLE` lines.
- Tune verbosity in `scripts/run_opportunity_loop.py` flags:
  - `--decision-pass-reason-limit`
  - `--decision-pass-sample-limit`
  - `--decision-log-path`
  - `--disable-decision-log`

## Minimum Viable Live Test Checklist

1. Confirm startup logs show components `coinbase`, `quotes`, `edges`, `opportunities`.
2. Confirm periodic jobs run: `settlements`, `scoring` (and `report` if enabled).
3. Confirm health line appears every minute:
   - `spot_age_s`
   - `quote_age_s`
   - `snapshot_age_s`
   - rolling counts for `snapshots`, `opportunities`, `scores`
4. In SQLite, verify growth:
   - `spot_ticks` increases
   - `kalshi_quotes` increases
   - `kalshi_edge_snapshots` increases
   - `opportunities` increases
5. After markets settle, verify:
   - `kalshi_contracts.outcome` and `kalshi_contracts.settled_ts` are populated
   - `kalshi_edge_snapshot_scores` increases

## Notes

- SQLite WAL and `busy_timeout` are enabled in component scripts; brief lock retries are expected under concurrent writes.
- Components are restarted automatically by the orchestrator with exponential backoff if they crash.
- This stack is shadow-mode only; execution modules remain placeholders.
