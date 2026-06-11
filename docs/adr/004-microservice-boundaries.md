# ADR-004: Microservice Boundaries

**Status:** Accepted
**Date:** 2026-06-10
**MVP:** MVP-1

---

## Context

BorneMap's backend functionality could be organized as a monolith or split into services. The team needed clear domain boundaries to prevent scope creep and cross-service coupling.

## Decision

**Strict domain-isolated services with no cross-service calls:**

| Service | Domain | Can Access |
|---|---|---|
| driver-service | station discovery (read-only) | platform_db.inventory, platform_db.gis |
| admin-service | station + partner management (CRUD) | platform_db.inventory |
| clickstream-service | event ingestion | analytics_db |
| auth-gateway | identity abstraction | Keycloak |

## Rationale

- Each service owns a single domain — no overlap
- Services communicate via HTTP only, never direct function calls
- No service depends on another service's internal logic
- Database credentials are scoped per-service
- Enables independent deployment and testing

## Consequences

- No cross-service DB access
- No shared business logic between services
- Shared code limited to DTOs, validation helpers, utilities
- Service boundaries are enforceable at the build level
- Future event-driven communication must still respect domain boundaries

## Related

- ADR-001: API Gateway Rejection
