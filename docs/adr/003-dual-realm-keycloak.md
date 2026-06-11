# ADR-003: Dual-Realm Keycloak Identity Model

**Status:** Accepted
**Date:** 2026-06-10
**MVP:** MVP-3

---

## Context

BorneMap serves two distinct user populations: drivers (public + registered) and operators (partners + admins). These groups have different auth requirements, security postures, and user management flows.

## Decision

**Two Keycloak realms:**

| Realm | Users | Auth Methods |
|---|---|---|
| `bm-drivers` | public_driver, registered_driver | email/password, Google OAuth |
| `bm-control` | partner, admin | admin-created, invitation-based |

## Rationale

- Drivers and operators have fundamentally different security requirements
- Drivers need self-service registration and social login
- Operators require admin-provisioned accounts and invitation flows
- Realm separation prevents cross-contamination of auth policies
- Aligns with zero-trust principle of least privilege

## Consequences

- Auth Gateway must route to correct realm based on client context
- Backend services validate realm in JWT to enforce service-specific rules
- No frontend code accesses Keycloak directly
- Realm management is internal only (no public admin console)
- Social login config is isolated to bm-drivers realm

## Related

- ADR-005: Analytics Append-Only Model
