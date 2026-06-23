<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan

**Feature**: 006-driver-experience-layer (Sprint 5)
**Plan**: specs/006-driver-experience-layer/plan.md
**Spec**: specs/006-driver-experience-layer/spec.md
**Branch**: 006-driver-experience-layer
**Date**: 2026-06-22

## Quick Links

- [Feature Specification](specs/006-driver-experience-layer/spec.md)
- [Implementation Plan](specs/006-driver-experience-layer/plan.md)
- [Research Report](specs/006-driver-experience-layer/research.md)
- [Data Model](specs/006-driver-experience-layer/data-model.md)
- [Quickstart Guide](specs/006-driver-experience-layer/quickstart.md)
- [Contracts](specs/006-driver-experience-layer/contracts/)
- [Constitution](docs/constitution/constitution.md)
- [Sprint 5 Backlog](docs/sprints/sprint_05/backlog/backlog.md)

## Key Information

**Services**: auth-service (3000), driver-service (3001), admin-service (3002)
**Databases**: platform_db (users, gis, inventory), analytics_db (telemetry, analytics, system), keycloak_db
**CI Pipeline**: 12 stages with hard-stop enforcement + 5 Sprint 5 security gates
**Identity**: UUID for users (Keycloak), nanoid(12) with PREFIX for entities
**Roles**: driver, partner, admin (RBAC enforced on every endpoint)
**Auth**: Keycloak OIDC with JWT validation via JWKS
**Frontend**: data-consumer-only — no business logic leakage
**Personalization**: users.preferences JSONB (separate favorites + preferences sections)
**Favorites**: driver-service owns favorites API (POST/GET/DELETE /api/v1/driver/favorites)
**Search**: Online via driver-service → Postgres trigram; offline via local cache
**Offline**: AsyncStorage/IndexedDB cache — zero backend dependency
**Telemetry**: Sprint 3 pipeline extended with 6 new event types
**SQLx**: Compile-time verification mandatory
**Contract-First**: DTOs in domain-types, then backend, then frontend
<!-- SPECKIT END -->
