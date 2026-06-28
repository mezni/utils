# Sprint 01 — Specification

## Goal
Establish the full project scaffolding: workspace, shared crates, service skeletons, database schema definitions, frontend foundations, development tooling, and CI/CD pipeline.

## Scope
- Rust workspace with 3 services + 5 shared crates
- Clean Architecture enforced per service
- PostgreSQL schema design (users, ev, gis)
- Frontend foundations (admin-dashboard, driver-web)
- Development scripts and Docker Compose
- Documentation structure
- CI/CD workflows

## Services Affected
- auth-service (skeleton)
- admin-service (skeleton)
- driver-service (skeleton)

## Database Changes
- Enable extensions: uuid-ossp, postgis, pgcrypto
- Create schemas: users, ev, gis

## Deliverables
1. Rust workspace with compilable members
2. Clean Architecture directory structure
3. Shared cross-cutting crates
4. Database migration files
5. Frontend Vite + React + Tailwind scaffolds
6. Docker Compose with PostgreSQL + PostGIS
7. Development shell scripts
8. CI/CD GitHub Actions
9. Sprint documentation

## Constraints
- Zero business logic in Sprint 01
- Clean Architecture layers must exist even if empty
- No authentication logic
- No API endpoints beyond health check
