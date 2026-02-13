# Redesign Decisions Log

Status: Active  
Owner: `ethanbsung` + `codex`  
Created: 2026-02-13

## Decision 001 - Existing SQLite Data Is Disposable
- Date: 2026-02-13
- Context: Small historical dataset; migration should not be blocked by legacy retention concerns.
- Decision: Production cutover does not require preserving current SQLite contents.
- Consequence:
  - We can reset runtime data safely during redesign rollout.
  - Optional archive remains available for post-mortems only.

## Decision 002 - Core Strategy Logic Must Remain Equivalent
- Date: 2026-02-13
- Context: Reliability redesign must not silently alter alpha behavior.
- Decision: Preserve edge and opportunity semantics; transport and persistence are the change surface.
- Consequence:
  - Replay parity becomes a hard acceptance gate.
  - Any intentional logic drift requires explicit sign-off.

## Decision 003 - SQLite Is Removed from Live Runtime Path
- Date: 2026-02-13
- Context: Overnight evidence shows persistent lock contention and stale-data cascades under multi-writer load.
- Decision: Do not pursue SQLite runtime stabilization as the long-term path.
- Consequence:
  - Live path architecture excludes SQLite writes.
  - SQLite may be archived, but not used as live persistence.

## Decision 004 - Event Bus + Single Writer Architecture
- Date: 2026-02-13
- Context: Current architecture couples many writers to one storage lock domain.
- Decision: Use durable event streams and route all database writes through one persistence service.
- Consequence:
  - Producers publish events only.
  - `svc_persistence` is the sole Postgres writer.

## Decision 005 - Concrete Production Stack Choices
- Date: 2026-02-13
- Context: Open-ended tooling decisions create delivery ambiguity.
- Decision:
  - Bus: NATS JetStream
  - Database: Postgres 16
  - Supervisor: systemd on single host for initial production rollout
- Consequence:
  - Build and operations work can proceed without architecture uncertainty.
  - Alternate stack choices require explicit change request.

## Decision 006 - Hard Promotion Gates Before Real Capital
- Date: 2026-02-13
- Context: Reliability must be demonstrated before live execution.
- Decision: Require strict shadow and paper soak gates plus risk-control drills before live ramp.
- Consequence:
  - No production trading until gates are met.
  - Go/no-go becomes objective and auditable.
