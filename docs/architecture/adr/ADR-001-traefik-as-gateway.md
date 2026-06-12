# ADR-001: Traefik as API Gateway

**Status:** Accepted  
**Date:** 2026-06-10  
**Authors:** Claude Code, Claude (chat)

---

## Context

BorneMap serves multiple client types (mobile, web, dashboard) to multiple backends (driver-service, admin-service).

In MVP-1, we operate locally via Docker Compose. By MVP-6, we need production-grade API gateway behavior:
- TLS termination
- Intelligent routing (by path, hostname, or URL prefix)
- Auth middleware integration
- Rate limiting (future)

Initially, we considered:
1. **No gateway** — direct service-to-client communication
2. **Custom Node.js BFF** — adds complexity, operational overhead
3. **AWS API Gateway** — cloud-locked, overengineered for MVP
4. **Traefik** — lightweight, Docker-native, integrates with auth

---

## Decision

**Implement Traefik as the API gateway for all client traffic.**

All client requests route through Traefik, which:
- Terminates TLS (production only)
- Routes by URL prefix (`/api/v1/*` → services)
- Validates JWTs (via auth middleware)
- Handles compression, rate limiting

**Local dev:** Traefik runs in Docker Compose, clients connect via `http://localhost:8080`.  
**Production:** Traefik exposed on public IP, TLS enabled.

---

## Rationale

| Option | Pros | Cons |
|--------|------|------|
| No gateway | Simplest code path | Exposes service details, no TLS abstraction, harder to evolve |
| Node.js BFF | Language familiarity | Adds operational load, not Docker-optimized, introduces new service type |
| AWS API Gateway | Managed, scalable | Cloud-locked, overengineered for MVP, costs, cold starts |
| **Traefik** | **Docker-native, lightweight, production-grade, no vendor lock-in** | **Small learning curve** |

Traefik provides:
- **Simplicity:** Single config for routing, TLS, auth
- **Scalability:** Proven in production at scale
- **Docker integration:** Auto-discovery via labels
- **No vendor lock-in:** Works on any infra (Docker, K8s, bare metal)

---

## Consequences

### Positive
- Clients never touch service IPs directly
- Auth validation happens at gateway boundary
- Easy to add rate limiting, CORS headers, compression later
- Clear separation of concerns (gateway ≠ business logic)

### Negative
- Extra hop for every request (negligible latency: <1ms local)
- Traefik config must be maintained (simple, documented)
- New team members must understand gateway behavior

---

## Implementation Notes

1. **Docker Compose:** Include Traefik service with health checks
2. **Routing rules:** Route `/api/v1/*` to backend services by prefix
3. **Auth middleware:** (MVP-3) Validate Keycloak JWTs at gateway
4. **Local dev:** Expose port 8080 for all client traffic
5. **Production:** Enable TLS, configure DNS

---

## Related ADRs

- ADR-002: Rust + Actix services
- ADR-004: Clickstream in admin-service (events routed via gateway)

---

## References

- [Traefik documentation](https://doc.traefik.io)
- [Docker Compose networking](https://docs.docker.com/compose/networking/)
