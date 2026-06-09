# ADR-007: Keycloak for authentication (from MVP-3)

**Status:** Accepted
**Date:** 2026-06-09

## Context

MVP-3 introduces authentication. The platform needs identity federation (Google, Facebook), role-based access control (registered_driver, partner, admin), and JWT issuance. Building custom auth is security-sensitive and time-consuming.

## Decision

Use Keycloak 24 as the identity provider. Keycloak owns all authentication — no service implements its own token issuance. JWTs are validated against Keycloak's JWKS endpoint with caching. Social login (Google, Facebook) configured in the Keycloak realm.

## Consequences

- Proven, battle-tested identity solution
- Social login works out of the box
- JWT includes custom claims (role, partner_id) via mappers
- Additional infrastructure to manage (Keycloak container + its database)
- Realm configuration must be exported and version-controlled
