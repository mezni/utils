# ADR-007: Keycloak for Authentication

**Status**: Accepted
**Date**: 2026-06-07

## Context

The platform supports three user roles (registered_driver, partner, admin) and needs login with email, Google, and Facebook. Options: Auth0, Firebase Auth, Keycloak (self-hosted), or custom auth.

## Decision

Use Keycloak 24 (self-hosted) as the authentication server.

## Rationale

- Industry-standard identity and access management
- Supports social login (Google, Facebook) out of the box
- JWT-based tokens with customizable claims (including partner_id injection)
- Realm export/import for reproducible configuration
- Active open-source project under CNCF stewardship
- Self-hosted — no third-party dependency for authentication
- ADR-006 (bare metal + Docker Compose) makes a self-hosted Keycloak instance simple to run

## Consequences

- Keycloak is a Java application with significant memory overhead
- Realm configuration must be maintained and versioned (infra/keycloak/realm-export.json)
- Keycloak upgrades are breaking and require careful migration
- Token validation requires JWKS endpoint caching in service middleware

## Compliance

- No service implements its own login, token issuance, or session management
- JWT validation uses the ev-auth shared crate (Phase 2)
- Realm configuration is committed to the repository
