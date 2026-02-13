# Phase A Validation Guide

Date: 2026-02-13  
Scope: Validate event contracts, JetStream streams, producer bus publishing, and Postgres persistence service.

## 1. Prerequisites

- Python deps:
  - `pip install -e ".[dev,live]"`
- Running services:
  - NATS JetStream (default `nats://127.0.0.1:4222`)
  - Postgres 16 (reachable DSN for `PG_DSN`)

Optional local bring-up (single host):

```bash
bash scripts/phase_a_infra_up.sh
```

## 2. Environment

```bash
export BUS_URL="nats://127.0.0.1:4222"
export PG_DSN="postgresql://kalshi:kalshi@127.0.0.1:5432/kalshi"
```

## 3. Provision Streams

```bash
.venv/bin/python scripts/setup_jetstream_streams.py
```

Expected:
- `MARKET_EVENTS` created/updated
- `STRATEGY_EVENTS` created/updated
- `EXECUTION_EVENTS` created/updated
- `DEAD_LETTER` created/updated

## 4. Start Persistence Service

```bash
.venv/bin/python scripts/run_persistence_service.py
```

Expected:
- steady counters printed every interval
- no parse/persist error growth in healthy flow
- `consumer_lag={...}` values trend down/stay bounded during healthy ingest

## 5. Start Live Stack with Bus Publishing

In a second terminal:

```bash
./scripts/live_stack.sh \
  --enable-event-bus \
  --event-bus-url "$BUS_URL" \
  --health-every-seconds 60
```

Expected:
- component logs continue as normal
- persistence service counters (`processed`, `inserted`) increase continuously
- no sustained DLQ growth under normal operation

## 6. Check Persisted Events in Postgres

```sql
SELECT event_type, COUNT(*)
FROM event_store.events_raw
GROUP BY event_type
ORDER BY event_type;
```

Expected:
- Non-zero rows for active producers:
  - `spot_tick`
  - `quote_update`
  - `edge_snapshot`
  - `opportunity_decision`
  - `market_lifecycle` (after refresh runs)
  - `contract_update` (after refresh runs)

## 7. DLQ Inspection

Inspect only:

```bash
.venv/bin/python scripts/inspect_dlq.py --max-messages 100
```

Ack inspected messages:

```bash
.venv/bin/python scripts/inspect_dlq.py --max-messages 100 --ack
```

Replay parseable DLQ events back to canonical subjects:

```bash
.venv/bin/python scripts/inspect_dlq.py --max-messages 100 --replay --ack
```

## 8. Exit Criteria for Phase A

- Streams provision cleanly.
- Producers publish to bus without runtime crashes.
- Persistence service inserts events idempotently.
- Projection tables update (`state_*` and `strategy_*` latest tables).
- DLQ remains near-zero in healthy conditions, with replay tooling available.

Stop local infra when done:

```bash
bash scripts/phase_a_infra_down.sh
```
