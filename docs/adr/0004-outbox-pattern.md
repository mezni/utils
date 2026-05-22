# ADR-0004 — Outbox pattern for reliable event publishing

- **Status**: Accepted
- **Date**: 2026-05-22
- **Deciders**: BorneMap core team
- **Tags**: data-integrity, events, rabbitmq

## Context

BorneMap uses RabbitMQ for asynchronous communication, in particular to
move domain events from `core-service` to `analytics-service` (which
writes audit logs and analytics aggregates to MongoDB).

A naive `dbCommit() ; rabbit.publish()` pattern is unsafe:

- If `dbCommit()` succeeds and `rabbit.publish()` fails, downstream
  consumers never see the event → audit gap, analytics drift.
- If `rabbit.publish()` succeeds and `dbCommit()` is then rolled back,
  consumers see a "phantom" event that never happened → corrupt audit.

Constitution Principle III makes this non-negotiable: `core-service` is
the sole event producer, and event integrity MUST be guaranteed.

## Decision

We will implement the **transactional outbox** pattern.

- Every domain event is written to an `outbox` table in PostgreSQL
  **inside the same transaction** as the business mutation that
  produced it.
- A **relay worker**, co-located with `core-service`, polls `outbox`
  rows where `published_at IS NULL`, publishes each to RabbitMQ, then
  sets `published_at`.
- Consumers (analytics-service today, others later) treat delivery as
  **at-least-once** and MUST be **idempotent**, keyed by the outbox
  row's event id.
- No service publishes domain events to RabbitMQ outside this pipeline.
  Direct `channel.publish` calls from business logic are forbidden.

### Outbox table shape

| column        | type            | notes                              |
|---------------|-----------------|------------------------------------|
| id            | UUID PK         | the event id; used as dedupe key   |
| aggregate_type| text            | e.g., `station`                    |
| aggregate_id  | text            | e.g., `STA-...`                    |
| event_type    | text            | e.g., `StationCreated`             |
| payload       | jsonb           | event body                         |
| created_at    | timestamptz     | default `now()`                    |
| published_at  | timestamptz NULL| set by relay after successful publish |

## Alternatives considered

- **CDC (Debezium reading Postgres WAL)** — Rejected for MVP. Heavier
  ops footprint; useful only if we outgrow the relay polling approach.
- **Two-phase commit between Postgres and RabbitMQ** — Rejected.
  Operationally fragile; not supported well in RabbitMQ.
- **At-most-once with retries from the producer** — Rejected. Violates
  Principle III; loses events on any process death between commit and
  publish.

## Consequences

- **Positive**
  - Strong guarantee that what consumers see matches what the database
    committed.
  - Recovery is trivial: a relay restart picks up unpublished rows.
- **Negative**
  - Producer write amplification (one extra row per mutation).
  - Polling latency floor on the relay (mitigated by short poll
    intervals or `LISTEN/NOTIFY`).
  - Consumers MUST be idempotent — discipline that has to hold forever.
- **Follow-ups**
  - Phase 3 creates the `outbox` table.
  - Phase 5a delivers the relay worker and transaction rollback tests
    before any CRUD endpoint can merge.
  - Phase 5.5 delivers an idempotent consumer in `analytics-service`.

## Compliance check

- Schema review: `outbox` table exists with the columns above.
- Static check / lint: grep for `channel.publish` or equivalent direct
  publishes outside the relay worker → fail CI.
- Test gate: transaction rollback tests prove the outbox row vanishes
  with its mutation (Principle VII).
- Test gate: at-least-once + idempotency tests on the consumer side
  (Phase 5.5).
