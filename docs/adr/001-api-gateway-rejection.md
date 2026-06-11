# ADR-001: API Gateway Rejection

**Status:** Accepted
**Date:** 2026-06-10
**MVP:** MVP-1

---

## Context

BorneMap has multiple backend services. Common architecture patterns suggest introducing an API gateway as a single entry point for routing, authentication, and rate limiting.

## Decision

**No API gateway will be used.**

Clients communicate with backend services directly:

```
Driver App → Driver Service (8080)
Dashboard  → Admin Service (8081)
All Apps   → Clickstream Service (8082)
```

## Rationale

- MVP scope does not warrant gateway complexity
- Direct access reduces latency and debugging overhead
- Services are few and well-defined
- Traefik handles edge routing in production (MVP-6)
- Premature gateway adds coupling without demonstrated need

## Consequences

- Clients must know each service endpoint
- Rate limiting moves to service layer
- Authentication handled per-service via JWT validation
- Traefik ingress is the only acceptable edge component (MVP-6)

## Related

- ADR-004: Microservice Boundaries
