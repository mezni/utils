# ADR-0001: System Architecture Overview

**Status:** Accepted
**Date:** June 2026
**Deciders:** Project team

---

## Context

BorneMap needs a platform architecture for EV charging station discovery and management in Tunisia. The key requirements are:
- Fast product validation
- Strict architectural constraints
- Clear service boundaries

## Decision

Adopt a three-service microservice architecture with:
- **auth-service (:3000)** — sole Keycloak admin API caller, owns `users` schema
- **driver-service (:3001)** — spatial read API, owns `gis` schema and Redis cache
- **admin-service (:3002)** — partner CRUD, owns `inventory` schema and `analytics_db`

## Consequences

**Positive:**
- Clear service ownership boundaries
- Independent deployability
- Schema-level data isolation

**Negative:**
- Service-to-service calls add latency vs monolith
- Three-service topology frozen — no future expansion

## Compliance

Enforced by `constitution/constitution.md` Section 3 and `tools/ci_guard.sh` Gate 5.
