<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan

**Feature**: 005-admin-analytics (Sprint 4)
**Plan**: specs/005-admin-analytics/plan.md
**Spec**: specs/005-admin-analytics/spec.md
**Branch**: 003-gis-engine
**Date**: 2026-06-22

## Quick Links

- [Feature Specification](specs/005-admin-analytics/spec.md)
- [Implementation Plan](specs/005-admin-analytics/plan.md)
- [Research Report](specs/005-admin-analytics/research.md)
- [Data Model](specs/005-admin-analytics/data-model.md)
- [Quickstart Guide](specs/005-admin-analytics/quickstart.md)
- [Contracts](specs/005-admin-analytics/contracts/)
- [Quality Checklist](specs/005-admin-analytics/checklists/requirements.md)
- [Constitution](docs/constitution/constitution.md)
- [Sprint 4 Backlog](docs/sprints/sprint_04/backlog/backlog.md)

## Key Information

**Services**: auth-service (3000), driver-service (3001), admin-service (3002)
**Databases**: platform_db (users, gis, inventory), analytics_db (telemetry, analytics, system), keycloak_db
**CI Pipeline**: 12 stages with hard-stop enforcement + 5 Sprint 4 security gates
**Identity**: UUID for users (Keycloak), nanoid(12) with PREFIX for entities
**Roles**: driver, partner, admin (RBAC enforced on every endpoint)
**Auth**: Keycloak OIDC with JWT validation via JWKS
**Analytics**: Read-only layer with materialized views, caching, and synchronous invalidation
**SQLx**: Compile-time verification mandatory
**Contract-First**: DTOs in domain-types, then backend, then frontend
<!-- SPECKIT END -->
