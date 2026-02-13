# Phase B Validation Guide

Date: 2026-02-13  
Scope: Validate ingest publish-only cutover behavior (spot + quotes) with JetStream + persistence.

## 1. Preconditions

- Phase A infrastructure is up (`NATS` + `Postgres`).
- Streams are provisioned:
  - `.venv/bin/python scripts/setup_jetstream_streams.py`
- Persistence service is running:
  - `.venv/bin/python scripts/run_persistence_service.py`

## 2. Run Ingest-Only Stack (Publish-Only)

```bash
./scripts/live_stack.sh \
  --enable-event-bus \
  --coinbase-publish-only \
  --quotes-publish-only \
  --disable-edges \
  --disable-opportunities \
  --disable-scoring \
  --disable-report \
  --disable-event-shadow
```

Expected:
- `coinbase` and `quotes` components stay up and publish events.
- No SQLite quote/spot insert workload from those two ingest components.
- `run_persistence_service.py` shows increasing `processed`/`inserted` and bounded `consumer_lag`.

## 3. Verify Persisted Event Mix

```sql
SELECT event_type, COUNT(*)
FROM event_store.events_raw
GROUP BY event_type
ORDER BY event_type;
```

Expected:
- Non-zero counts for:
  - `spot_tick`
  - `quote_update`

## 4. Exit Criteria for This Cut

- 24h soak with:
  - no restart storm on ingest components (`<= 3 restarts/hour/component`),
  - bounded consumer lag,
  - no unexplained DLQ growth in healthy windows.
