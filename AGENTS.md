<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan

**Feature**: 001-system-bootstrap
**Plan**: specs/001-system-bootstrap/plan.md
**Spec**: specs/001-system-bootstrap/spec.md
**Branch**: 001-system-bootstrap
**Date**: 2026-06-21

## Quick Links

- [Feature Specification](specs/001-system-bootstrap/spec.md)
- [Implementation Plan](specs/001-system-bootstrap/plan.md)
- [Research Report](specs/001-system-bootstrap/research.md)
- [Data Model](specs/001-system-bootstrap/data-model.md)
- [Quickstart Guide](specs/001-system-bootstrap/quickstart.md)
- [Contracts](specs/001-system-bootstrap/contracts/)
- [Constitution](docs/constitution/constitution.md)
- [Sprint 0 Backlog](docs/sprints/sprint_00/backlog/backlog.md)

## Key Information

**Services**: auth-service (3000), driver-service (3001), admin-service (3002)
**Databases**: platform_db (users, gis, inventory), analytics_db (telemetry, analytics, system), keycloak_db
**CI Pipeline**: 9 stages with hard-stop enforcement
**Identity**: UUID for users (Keycloak), nanoid(12) with PREFIX for entities
**SQLx**: Compile-time verification mandatory
**Contract-First**: DTOs in domain-types, then backend, then frontend
<!-- SPECKIT END -->
