# Sprint 7 — System Hardening & Reliability Layer

**Status**: NOT_STARTED
**Constitution Version**: 1.15.2
**Dependencies**: Sprint 6 (UX layer complete)

---

## Must Have (Exit Criteria)

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S7-001 | Implement timeout enforcement on all outbound calls | team | NOT_STARTED |
| S7-002 | Implement bounded retries (max 3 retries, no infinite loops) | team | NOT_STARTED |
| S7-003 | Implement circuit breaker for Redis and PostGIS access | team | NOT_STARTED |
| S7-004 | Implement graceful degradation responses | team | NOT_STARTED |
| S7-005 | Enforce PostGIS query safety (max radius 50km, max results 500, bounded bbox) | team | NOT_STARTED |
| S7-006 | Enforce transaction wrapping for all analytics writes | team | NOT_STARTED |
| S7-007 | Implement atomic writes for user provisioning, station updates, charger assignments | team | NOT_STARTED |
| S7-008 | Formalize cache invalidation contracts with TTL standards | team | NOT_STARTED |
| S7-009 | Implement structured error format across all APIs | team | NOT_STARTED |
| S7-010 | Implement /health, /ready, /live endpoints per service | team | NOT_STARTED |
| S7-011 | Implement readiness vs liveness separation | team | NOT_STARTED |
| S7-012 | Add correlation IDs (trace_id) to all requests | team | NOT_STARTED |

## Should Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S7-013 | Create load stability tests (1k concurrent geo, 10k event bursts) | team | NOT_STARTED |
| S7-014 | Create CI query safety gate | team | NOT_STARTED |
| S7-015 | Create CI transaction safety gate | team | NOT_STARTED |
| S7-016 | Create CI retry safety gate | team | NOT_STARTED |
| S7-017 | Create CI cache invalidation gate | team | NOT_STARTED |
| S7-018 | Create CI error contract gate | team | NOT_STARTED |
| S7-019 | Create CI cross-schema mutation gate (reinforced) | team | NOT_STARTED |

## Nice to Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S7-020 | Add dependency health checks (DB + Redis) | team | NOT_STARTED |
| S7-021 | Add dead-letter queue for malformed events | team | NOT_STARTED |

## CI Additions (Sprint 7)

| ID | Gate | Rule |
|----|------|------|
| CI-7.1 | Query Safety Gate | FAIL if PostGIS query has no LIMIT or unbounded ST_DWithin |
| CI-7.2 | Transaction Safety Gate | FAIL if multi-step DB operation lacks rollback |
| CI-7.3 | Retry Safety Gate | FAIL if infinite retry loops or retry count > 3 |
| CI-7.4 | Cache Invalidation Gate | FAIL if cache mutation without invalidation rule |
| CI-7.5 | Error Contract Gate | FAIL if any endpoint returns unstructured error responses |
| CI-7.6 | Cross-Schema Mutation Gate | FAIL if service writes outside owned schema |

## Exit Criteria

Sprint 7 is COMPLETE ONLY IF:
- [ ] All services handle failure safely (no cascading failures)
- [ ] Transaction rollback verified (no partial writes allowed)
- [ ] Geo queries bounded and stable
- [ ] Retry, transaction, cache, and schema gates pass
