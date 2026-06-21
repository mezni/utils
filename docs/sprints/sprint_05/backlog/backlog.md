# Sprint 4 — Telemetry Ingestion Core

**Status**: NOT_STARTED
**Constitution Version**: 1.15.2
**Dependencies**: Sprint 3 (inventory system operational)

---

## Must Have (Exit Criteria)

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S4-001 | Implement `POST /api/v1/telemetry/events` in driver-service | team | NOT_STARTED |
| S4-002 | Create analytics_events table in analytics_db | team | NOT_STARTED |
| S4-003 | Implement event validation layer (schema_version, timestamp, payload, user_id) | team | NOT_STARTED |
| S4-004 | Implement event normalization pipeline (validate → enrich → deduplicate → persist) | team | NOT_STARTED |
| S4-005 | Implement idempotency system (idempotency_key hash index, duplicate rejection) | team | NOT_STARTED |
| S4-006 | Implement event enrichment (geolocation, session metadata, role context) | team | NOT_STARTED |
| S4-007 | Create event schema registry in domain-types (contracts only) | team | NOT_STARTED |
| S4-008 | Implement frontend telemetry SDK in client-core (emitter, retry, batch, idempotency key gen) | team | NOT_STARTED |
| S4-009 | Configure Traefik telemetry routing (/api/v1/telemetry/* → driver-service ONLY) | team | NOT_STARTED |
| S4-010 | Implement dead-letter logging for malformed events | team | NOT_STARTED |

## Should Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S4-011 | Create CI analytics write gate | team | NOT_STARTED |
| S4-012 | Create CI event schema validation gate | team | NOT_STARTED |
| S4-013 | Create CI idempotency gate | team | NOT_STARTED |
| S4-014 | Create CI telemetry routing gate | team | NOT_STARTED |
| S4-015 | Create CI payload structure gate | team | NOT_STARTED |
| S4-016 | Write integration tests for event ingestion pipeline | team | NOT_STARTED |

## Nice to Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S4-017 | Add event replay mechanism for recovery | team | NOT_STARTED |
| S4-018 | Create event throughput benchmark | team | NOT_STARTED |

## CI Additions (Sprint 4)

| ID | Gate | Rule |
|----|------|------|
| CI-4.1 | Analytics Write Gate (CRITICAL) | FAIL if any service other than driver-service writes analytics_db |
| CI-4.2 | Event Schema Validation Gate | FAIL if missing schema_version, invalid UUID in user_id, invalid timestamp |
| CI-4.3 | Idempotency Gate | FAIL if duplicate event ingestion not handled or missing idempotency_key |
| CI-4.4 | Telemetry Routing Gate | FAIL if telemetry endpoint exists outside driver-service |
| CI-4.5 | Payload Structure Gate | FAIL if payload is not JSON object or nested invalid types detected |

## Exit Criteria

Sprint 4 is COMPLETE ONLY IF:
- [ ] All events flow through driver-service ingestion endpoint
- [ ] No external writes to analytics_db
- [ ] Schema validation enforced in CI (invalid events rejected)
- [ ] Idempotency guaranteed (replay-safe storage verified)
- [ ] Analytics write gate passes
- [ ] Telemetry routing enforced
- [ ] Schema validation strict mode active
