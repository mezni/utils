# ADR-005: Analytics Append-Only Model

**Status:** Accepted
**Date:** 2026-06-10
**MVP:** MVP-1

---

## Context

BorneMap requires event capture for UX analytics. The data model could support updates/deletes for flexibility, or enforce immutability for audit integrity.

## Decision

**analytics_db is append-only.**

- `raw_events` table: INSERT only, no UPDATE, no DELETE
- Event payload is flexible JSONB
- Event validation happens before insert (in clickstream-service)
- Aggregates are computed separately (event_aggregates, station_analytics)

## Rationale

- Immutable event logs guarantee audit trail integrity
- Prevents accidental or malicious data manipulation
- Simplifies write path (no locking, no conflict resolution)
- Aligns with event sourcing principles for future analytics pipelines
- JSONB payload allows schema evolution without migrations

## Consequences

- Data correction must happen via new events (compensation pattern)
- Storage grows monotonically — partitioning strategy required post MVP-2
- Aggregates are eventually consistent, computed from raw event log
- No joins to platform_db at write time

## Related

- ADR-004: Microservice Boundaries
