# Kalshi BTC Live System Production Redesign Plan

Status: Active v2  
Owner: `ethanbsung` + `codex`  
Last updated: 2026-02-13  
Scope: Redesign runtime and data architecture for continuous live operation while preserving core strategy logic.

## 0. Implementation Progress Snapshot

As of 2026-02-13:

- Phase A status: `validated_complete`
- Phase B status: `in_progress`
- Completed in code:
  - Versioned event contracts and idempotency key helpers in `src/kalshi_bot/events/`.
  - JetStream helpers and stream provisioning scripts:
    - `src/kalshi_bot/events/jetstream.py`
    - `src/kalshi_bot/events/jetstream_bus.py`
    - `scripts/setup_jetstream_streams.py`
  - Persistence service skeleton with Postgres raw events + latest-state projections:
    - `src/kalshi_bot/persistence/postgres.py`
    - `src/kalshi_bot/persistence/service.py`
    - `scripts/run_persistence_service.py`
  - Shadow event emission wired for current runtime producers:
    - spot ticks (`collector.py`)
    - quote updates (`stream_kalshi_quotes_ws.py`)
    - edge snapshots (`run_live_edges.py`)
    - opportunity decisions (`run_opportunity_loop.py`)
    - market lifecycle (`refresh_btc_markets.py`)
    - contract updates (`refresh_kalshi_contracts.py`)
    - live stack wiring (`run_live_stack.py`) for event shadow files
    - optional JetStream producer mode across all core emitters
      (`--events-bus`, `--events-bus-url`) and stack-level pass-through
      (`--enable-event-bus`, `--event-bus-url`)
  - DLQ tooling:
    - `scripts/inspect_dlq.py` for inspect + optional replay/ack flows
  - Validation runbook:
    - `docs/PHASE_A_VALIDATION.md`
    - `infra/docker-compose.phase-a.yml`
    - `scripts/phase_a_infra_up.sh`
    - `scripts/phase_a_infra_down.sh`
  - Phase B ingest controls:
    - `collector.py` supports `--coinbase-publish-only`
    - `stream_kalshi_quotes_ws.py` supports `--publish-only`
    - `run_live_stack.py` supports component disable flags for ingest-only soaks
    - `docs/PHASE_B_VALIDATION.md`
- Phase A validation completed:
  - JetStream stream provisioning validated against local NATS (`MARKET_EVENTS`, `STRATEGY_EVENTS`, `EXECUTION_EVENTS`, `DEAD_LETTER`).
  - Persistence consumer validated against local Postgres with no parse/persist errors.
  - End-to-end ingest validation confirmed persisted event types:
    - `spot_tick`
    - `quote_update`
    - `market_lifecycle`
    - `contract_update`

## 1. Goals, Non-Goals, and Invariants

### Goals
- Operate continuously for multi-day periods without ingestion stalls, stale snapshots, or write-contention cascades.
- Preserve current edge math and opportunity decision semantics.
- Reach live-grade reliability with explicit risk controls and deterministic degraded modes.
- Keep complete auditability and replayability for every input, decision, and action.

### Non-goals
- No alpha/model redesign in this project phase.
- No SQLite runtime hardening as a long-term direction.
- No requirement to preserve current SQLite history for production cutover.

### Invariants (must not change without explicit strategy sign-off)
- `compute_edges` math remains functionally equivalent (`src/kalshi_bot/strategy/edge_engine.py`).
- Opportunity semantics remain functionally equivalent (`src/kalshi_bot/strategy/opportunity_engine.py` + `scripts/run_opportunity_loop.py`).
- Fee logic and side-selection behavior remain equivalent.

## 2. Why Current Architecture Is Not Live-Grade

Observed failure pattern from overnight runs:
- Spot, quote, and snapshot freshness all went stale together.
- `stale_edge_snapshots` loops persisted while core processes remained alive.
- `database is locked` retries/skips appeared repeatedly in refresh and ingest paths.
- Snapshot generation flatlined, then opportunities flatlined by dependency.

Conclusion:
- The current single-DB, multi-writer SQLite architecture is structurally unfit for live operation.
- This is an architecture problem, not a strategy-math problem.

## 3. Hard Decisions (Locked)

1. SQLite is removed from the live runtime write path.
2. Live path uses an event bus plus Postgres persistence.
3. Only one service (`svc_persistence`) writes to Postgres.
4. Edge and opportunity services compute from event-driven state, not DB polling loops.
5. Promotion to live requires passing explicit shadow/paper soak gates.

## 4. Target Architecture (Concrete)

## 4.1 Technology Choices
- Message bus: NATS JetStream.
- Primary database: Postgres 16.
- Initial deployment target: systemd-managed services on one host.
- Metrics: Prometheus-compatible endpoint per service.
- Dashboards/alerts: Grafana + alert rules.

Rationale:
- NATS JetStream provides durable streams, consumer offsets, and straightforward ops for this scale.
- Postgres removes SQLite single-writer lock bottlenecks and supports robust idempotent upsert patterns.
- systemd is the fastest reliable control-plane for a single-host production ramp.

## 4.2 Service Topology
- `svc_spot_ingest`: Coinbase WS client; emits normalized `spot_tick` events.
- `svc_quote_ingest`: Kalshi WS client; emits normalized `quote_update` events.
- `svc_market_catalog`: Kalshi REST polling; emits market and contract lifecycle events.
- `svc_state_builder`: consumes events; maintains in-memory tradable state and freshness views.
- `svc_edge_runner`: periodic edge ticks from in-memory state; emits `edge_snapshot` events.
- `svc_opportunity_runner`: consumes edge snapshots; emits `opportunity_decision` events.
- `svc_persistence`: single Postgres writer for all event and projection writes.
- `svc_scoring_reporting`: batch analytics, scoring, and model reports from Postgres.
- `svc_execution` (later phase): shadow/paper/live order routing under risk limits.

## 4.3 Event Streams and Contracts
JetStream streams (canonical):
- `market.spot_ticks`
- `market.quote_updates`
- `market.lifecycle`
- `market.contract_updates`
- `strategy.edge_snapshots`
- `strategy.opportunity_decisions`
- `execution.orders`
- `execution.fills`

Event contract requirements:
- Versioned schema (`schema_version`).
- Producer timestamp + event timestamp.
- Deterministic idempotency key in payload metadata.
- Strict validation on publish and consume.
- Dead-letter stream for malformed events.

### Event Identity and Idempotency Rules
- Every event MUST include: `event_type`, `schema_version`, `ts_event`, `source`, `idempotency_key`, and `payload`.
- Preferred key strategy: use stable upstream identity when present (exchange sequence/message id).
- Fallback strategy when upstream identity is missing: compute `idempotency_key` as a hash of canonical payload plus stable producer metadata.
  - Canonicalization requirements: sorted JSON keys, explicit nulls retained, no whitespace, deterministic float formatting.
  - Include `event_type`, `schema_version`, `source`, and canonical `payload` in the hash source.
  - Do not include local wall-clock ingestion time in fallback key derivation.
- Producer implementation MUST be deterministic across restarts for the same input event.
- If a producer cannot build deterministic identity, event publish must fail closed (send to DLQ) rather than emit unstable keys.

## 4.4 Write/Read Semantics
Write semantics:
- At-least-once delivery from producers.
- Idempotent persistence based on unique keys.
- Batched commit windows in `svc_persistence` (small bounded transactions).

Read semantics:
- Strategy services consume from state builder snapshots and stream events.
- Postgres is audit/projection store, not the hot-path dependency for edge/opportunity loops.

### Ordering and Dedup Semantics
- Ordering guarantee is per-subject/per-producer only; no global ordering guarantee across subjects or producers.
- Consumers MUST treat events as at-least-once and potentially duplicated (especially across reconnect/replay).
- Projection upserts MUST be monotonic by domain timestamp (`ts`, `asof_ts`, `ts_eval`, etc.):
  - Ignore updates older than the current projection timestamp.
  - On equal timestamp ties, use deterministic tie-break (`idempotency_key` lexical order).
- Strategy-critical consumers must be safe under bounded out-of-order delivery and replay.

### Persistence Throughput and Backpressure
- `svc_persistence` is single writer by design; it must use bounded micro-batches.
  - Target defaults: flush every `50-200ms` OR `100-1000` events, whichever comes first.
  - Hard transaction ceiling: `<= 5000` events per commit.
- Backpressure behavior:
  - Producers continue publishing while JetStream accepts writes.
  - If stream publish acks degrade or fail repeatedly, system enters no-trade degraded mode.
  - If persistence lag exceeds trigger thresholds (Section 6.2), no-trade mode is enforced until recovery hysteresis is satisfied.

## 5. Storage Model (Postgres)

## 5.1 Append-Only Event Tables
- `events_spot_ticks`
- `events_quote_updates`
- `events_market_lifecycle`
- `events_contract_updates`
- `events_edge_snapshots`
- `events_opportunity_decisions`
- `events_orders`
- `events_fills`

## 5.2 Projection Tables
- `state_spot_latest`
- `state_quote_latest`
- `state_market_latest`
- `state_contract_latest`
- `state_positions`
- `state_risk_limits`

## 5.3 Analytics Tables
- `fact_edge_snapshot_scores`
- `fact_opportunity_outcomes`
- `fact_daily_performance`

## 5.4 Idempotency Keys (Required)
- Spot: preferred `(product_id, exchange_ts, sequence_num)`; fallback `hash(canonical payload + source)`.
- Quote: preferred `(market_id, quote_ts, source_msg_id|seq)`; fallback `hash(canonical payload + source)`.
- Market lifecycle: `(market_id, status, close_ts, expected_expiration_ts, expiration_ts, settlement_ts)`
- Contract update: `(ticker, close_ts, expected_expiration_ts, expiration_ts, settled_ts, outcome)`
- Edge snapshot: `(asof_ts, market_id, strategy_version)`
- Opportunity decision: `(ts_eval, market_id, side, strategy_version)`

### Required Uniqueness Constraints
- Every append-only event table must enforce idempotency at the storage layer.
  - Option A (preferred): central raw table with `UNIQUE (event_type, idempotency_key)`.
  - Option B (typed tables): each table has `idempotency_key TEXT NOT NULL UNIQUE`.
- All projection upserts must have a single stable primary key (`market_id`, `product_id`, etc.) and monotonic timestamp checks.

## 6. Runtime Reliability Requirements

## 6.1 SLO Targets
- Spot freshness p99 < 3s.
- Quote freshness p99 (tradable universe) < 5s.
- Edge snapshot gap p99 < 15s.
- Hard maximum snapshot gap: 60s.
- Opportunity decision latency p99 < 2s from snapshot availability.
- Event consumer lag p99 < 10s for strategy-critical streams.

## 6.2 Hard Fail-Safe Modes
- If `snapshot_age_s > 60`: disable new entries immediately.
- If quote coverage for tradable universe < 80% for > 60s: no-trade degraded mode.
- If state-builder lag exceeds threshold: no-trade degraded mode.
- If risk service unavailable: no-trade degraded mode.
- Recovery requires sustained healthy window before re-enabling entries.

### Explicit No-Trade Triggers and Recovery Hysteresis
- Enter no-trade immediately when any condition is true:
  - `state_builder_consumer_lag_s > 10s` for `>= 30s`.
  - `persistence_consumer_lag_s > 30s` for `>= 60s`.
  - `spot_age_s > 10s` OR `quote_age_s > 30s` OR `snapshot_age_s > 60s`.
  - Tradable-market quote coverage `< 80%` for `>= 60s`.
- Recovery hysteresis:
  - Require all trigger metrics healthy continuously for `>= 5 minutes` before re-enabling entries.
  - Re-enable in paper/shadow first if run mode supports staged recovery.

## 6.3 Control-Plane Requirements
- Every service exposes liveness and readiness checks.
- systemd restart policy with bounded backoff.
- Alert on restart storms.
- Alert on lag growth, dead-letter growth, and no-snapshot streaks.

## 6.4 REST Rate Limit Policy
- All REST pollers must implement bounded exponential backoff with full jitter on `429/5xx`.
- Respect `Retry-After` header when present; it overrides computed backoff.
- Enforce global per-service request budgets (token bucket) and max in-flight concurrency.
- Stagger periodic jobs (markets/contracts/settlements) with explicit offsets to avoid synchronized API bursts.
- If rate-limit budget is exhausted for strategy-critical refreshes, trigger degraded no-trade mode instead of tight retry loops.

## 7. Configuration Migration (SQLite-Free)

## 7.1 Removed for Live Path
- `DB_PATH` as live write target.
- Timeboxed process knobs (`COLLECTOR_SECONDS`, quote runtime windows).
- Any cron schedule that writes directly to live database tables.

## 7.2 Kept/renamed Core Knobs
- Keep strategy knobs (`EV_MIN`, sigma knobs, horizon/freshness knobs) with equivalent semantics.
- Replace storage/bus knobs with:
  - `BUS_URL`
  - `BUS_STREAM_RETENTION_HOURS`
  - `PG_DSN`
  - `PG_POOL_MIN`
  - `PG_POOL_MAX`
  - `PG_STATEMENT_TIMEOUT_MS`

## 7.3 Stream Capacity and Retention Sizing
- Before live/paper soak, estimate sustained and peak event volume and configure JetStream limits accordingly.
- Retention must be set by stream with explicit `max_age` and `max_bytes`; DLQ retention must be configured separately.
- Disk sizing formula (baseline):  
  `expected_bytes = event_rate_per_s * avg_payload_bytes * retention_seconds * replication_factor * 1.3(headroom)`

Sizing TODO table (must be filled before Phase C):

| Stream/Event Type | Sustained Rate (evt/s) | Peak Rate (evt/s) | Avg Payload (bytes) | Retention (hours) | Expected Disk (GB) |
| --- | ---: | ---: | ---: | ---: | ---: |
| `market.spot_ticks` | TODO | TODO | TODO | TODO | TODO |
| `market.quote_updates` | TODO | TODO | TODO | TODO | TODO |
| `market.lifecycle` + `market.contract_updates` | TODO | TODO | TODO | TODO | TODO |
| `strategy.edge_snapshots` | TODO | TODO | TODO | TODO | TODO |
| `strategy.opportunity_decisions` | TODO | TODO | TODO | TODO | TODO |
| `dlq.>` | TODO | TODO | TODO | TODO | TODO |

## 8. Migration Plan (No More SQLite Stabilization Track)

## Phase A: Bus + Contract Foundation (Week 1)
Scope:
- Finalize event schemas and validation library.
- Stand up JetStream streams and DLQ.
- Build persistence service skeleton with idempotent inserts.

Exit criteria:
- All core event types publish and validate.
- Persistence service can consume and persist without strategy coupling.
- `p99 publish-to-persist latency < 2s` in a 1-hour integration run.
- DLQ rate is explained/triaged and not growing unexpectedly during healthy flow.

## Phase B: Ingest Cutover to Bus (Week 2)
Scope:
- `svc_spot_ingest`, `svc_quote_ingest`, and `svc_market_catalog` publish only to bus.
- Remove direct DB writes from ingest services.

Exit criteria:
- No ingest path writes to SQLite or Postgres directly except through `svc_persistence`.
- Ingest lag and drop metrics within thresholds for a 24h soak.
- `consumer_lag p99 < 10s` for market streams over 24h.
- No restart storm (`>3 restarts/hour` for any ingest component).

## Phase C: State Builder + Edge Runner Cutover (Week 3)
Scope:
- Build `svc_state_builder` from bus streams.
- Adapt edge runner to state snapshots and emit edge events.
- Keep edge math logic unchanged.

Exit criteria:
- Replay parity for edge outputs passes CI thresholds.
- 48h soak with snapshot cadence SLO met.
- No-trade trigger/recovery hysteresis behavior validated in failure injection.

## Phase D: Opportunity Runner Cutover (Week 4)
Scope:
- Opportunity runner consumes edge snapshot stream/state.
- Emits decisions to bus and persistence.

Exit criteria:
- Replay parity for decision outputs passes CI thresholds.
- 48h soak with no unexplained no-trade gaps.
- `p99 edge->opportunity decision latency < 2s` during soak.

## Phase E: Scoring/Reporting + Paper Trading (Week 5)
Scope:
- Move scoring/reporting to Postgres-backed jobs.
- Run full paper mode with execution service in shadow/paper.

Exit criteria:
- 7-day paper run with zero critical outages.
- Risk controls and kill-switch behavior validated.
- No sustained DLQ growth in healthy windows (`< 0.1%` of total events/day).

## Phase F: Live Ramp (Week 6+)
Scope:
- Enable live execution with strict caps.
- Progressive notional and frequency ramp only after stable checkpoints.

Exit criteria:
- 14-day stable operation with no critical reliability incident.
- Formal go/no-go sign-off.
- `p99 publish-to-persist latency < 2s` and `consumer_lag p99 < 10s` maintained through ramp.

## 9. Test and Validation Strategy

## 9.1 Deterministic Replay Parity (Mandatory)
- Replays must compare baseline vs redesigned outputs:
  - `prob_yes`, `ev_take_yes`, `ev_take_no`
  - opportunity eligibility, side, and no-trade reasons
- CI fails on tolerance breach.

## 9.2 Soak and Chaos Validation
- 24h, 72h, and 7-day soaks.
- Failure injection scenarios:
  - spot stream disconnect storm
  - quote stream disconnect storm
  - delayed market lifecycle updates
  - persistence slowdown and backlog surge
  - bus consumer restart storms
- Verify bounded degradation and automatic recovery.

## 10. Live Readiness Gates (Hard)

All gates must pass before real capital:
1. `7 days` shadow with no critical SLO breach.
2. `7 days` paper with no critical SLO breach.
3. Replay parity suite green on latest code.
4. Kill switch and risk cap drills passed.
5. Alerting and runbooks validated by on-call dry run.
6. `p99 publish-to-persist latency < 2s` over final 7-day pre-live window.
7. No restart storms and no unexplained DLQ growth during final 7-day pre-live window.

## 11. Immediate Build Queue (Next 10 Working Days)

1. Lock schema versions and idempotency key spec for all event types.
2. Stand up JetStream with stream retention and DLQ config.
3. Implement `svc_persistence` with batch writes and conflict handling.
4. Migrate spot ingest to publish-only bus mode.
5. Migrate quote ingest to publish-only bus mode.
6. Migrate market catalog to publish-only bus mode.
7. Implement state-builder projection for tradable universe and freshness.
8. Add parity replay harness for edge and opportunity outputs.
9. Wire Prometheus metrics and alert rules for lag/freshness/SLO.
10. Build and test service runbooks for restart, lag, and degraded-mode incidents.

## 12. Operational Notes

- Existing SQLite files are disposable for runtime migration.
- Optional archive is for post-mortem only, not part of live runtime.
- Any future request to reintroduce SQLite in the live path requires explicit sign-off and new architecture review.

## 12.1 Local/Dev and Single-Host Execution Notes
- Local/dev minimum stack: NATS (JetStream enabled) + Postgres 16 via Docker Compose or equivalent.
- systemd ordering for single-host deployment:
  - services should use `After=network-online.target`.
  - strategy/persistence services should `Wants=nats.service postgresql.service`.
  - strategy services should start after persistence/state dependencies are ready.
- Keep explicit startup ordering in runbooks to avoid partial-start stale states.

## 12.2 State Builder Ownership and Restart Behavior
- `svc_state_builder` is the only authoritative hot-state source for strategy services.
- `svc_edge_runner` and `svc_opportunity_runner` must not maintain independent canonical market state.
- On restart, `svc_state_builder` should:
  - restore from latest checkpoint/snapshot (Postgres projection or local snapshot file), then
  - replay stream tail from durable consumer offset.
- Checkpoint cadence should be frequent enough to keep restart replay bounded (target: recover hot state within a few minutes, not full-history replay).

## 13. Summary

- We are not pursuing incremental SQLite runtime stabilization.
- We are executing a full event-driven + Postgres redesign with strict reliability gates.
- Strategy math stays intact; transport, state, and persistence are the redesign surface.
- Priority is live-grade reliability first, then controlled live ramp.
