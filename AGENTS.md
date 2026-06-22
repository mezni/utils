<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan

**Feature**: 002-identity-security-core
**Plan**: specs/002-identity-security-core/plan.md
**Spec**: specs/002-identity-security-core/spec.md
**Branch**: 002-identity-security-core
**Date**: 2026-06-21

## Quick Links

- [Feature Specification](specs/002-identity-security-core/spec.md)
- [Implementation Plan](specs/002-identity-security-core/plan.md)
- [Research Report](specs/002-identity-security-core/research.md)
- [Data Model](specs/002-identity-security-core/data-model.md)
- [Quickstart Guide](specs/002-identity-security-core/quickstart.md)
- [Contracts](specs/002-identity-security-core/contracts/)
- [Constitution](docs/constitution/constitution.md)
- [Sprint 1 Backlog](docs/sprints/sprint_01/backlog/backlog.md)

## Key Information

**Services**: auth-service (3000), driver-service (3001), admin-service (3002)
**Databases**: platform_db (users, gis, inventory), analytics_db (telemetry, analytics, system), keycloak_db
**CI Pipeline**: 9 stages with hard-stop enforcement + 4 Sprint 1 security gates
**Identity**: UUID for users (Keycloak), nanoid(12) with PREFIX for entities
**Roles**: driver, partner, admin (RBAC enforced on every endpoint)
**Auth**: Keycloak OIDC with JWT validation via JWKS
**Event Bus**: Auth audit events → driver-service POST /api/v1/telemetry/events → analytics_db
**SQLx**: Compile-time verification mandatory
**Contract-First**: DTOs in domain-types, then backend, then frontend
<!-- SPECKIT END -->
