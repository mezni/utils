# ADR-0002: Service Boundary Rules

**Status:** Accepted
**Date:** June 2026
**Deciders:** Project team

---

## Context

With three services, we need strict rules about what each service can and cannot do to prevent architecture erosion.

## Decision

Adopt the following boundary rules:
1. **auth-service** is the ONLY service that calls Keycloak Admin REST API
2. **driver-service** is the ONLY service that serves geo queries to end users
3. **admin-service** is the ONLY service that writes to `inventory` schema
4. No service may import Rust code from another service's crate
5. No service may write directly to another service's schema
6. driver-service may query materialized views (read-only) via dedicated DB read role
7. Services communicate only via HTTP over internal Docker network

## Consequences

**Positive:**
- Prevents circular dependencies
- Clear debugging boundaries
- Schema isolation enforced at CI level

**Negative:**
- Some operations require chained HTTP calls
- Materialized view approach adds complexity

## Compliance

Enforced by `constitution/guardrails.md` Section 4 and `tools/ci_guard.sh` Gate 3 (schema isolation).
