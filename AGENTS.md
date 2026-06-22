<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan

**Feature**: 003-gis-engine
**Plan**: specs/003-gis-engine/plan.md
**Spec**: specs/003-gis-engine/spec.md
**Branch**: 003-gis-engine
**Date**: 2026-06-22

## Quick Links

- [Feature Specification](specs/003-gis-engine/spec.md)
- [Implementation Plan](specs/003-gis-engine/plan.md)
- [Research Report](specs/003-gis-engine/research.md)
- [Data Model](specs/003-gis-engine/data-model.md)
- [Quickstart Guide](specs/003-gis-engine/quickstart.md)
- [Contracts](specs/003-gis-engine/contracts/)
- [Constitution](docs/constitution/constitution.md)
- [Sprint 2 Backlog](docs/sprints/sprint_02/backlog/backlog.md)

## Key Information

**Services**: auth-service (3000), driver-service (3001), admin-service (3002)
**Databases**: platform_db (users, gis, inventory), analytics_db (telemetry, analytics, system), keycloak_db
**CI Pipeline**: 12 stages with hard-stop enforcement + 5 Sprint 2 security gates
**Identity**: UUID for users (Keycloak), nanoid(12) with PREFIX for entities
**Roles**: driver, partner, admin (RBAC enforced on every endpoint)
**Auth**: Keycloak OIDC with JWT validation via JWKS
**GIS**: PostGIS spatial queries, Redis spatial cache, OSM ingestion pipeline
**Event Bus**: Auth audit events → driver-service POST /api/v1/telemetry/events → analytics_db
**SQLx**: Compile-time verification mandatory
**Contract-First**: DTOs in domain-types, then backend, then frontend
<!-- SPECKIT END -->
