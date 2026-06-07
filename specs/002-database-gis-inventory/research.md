# Research: Database — GIS and Inventory Schemas

## Overview

All technology decisions for this sprint are pre-determined by the existing project architecture (ADR-001 through ADR-015) and constitution. No unresolved technical choices required research.

## Technology Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Database | PostgreSQL 16 + PostGIS 3.4 | Per constitution technology stack and Docker Compose from Sprint 1.1 |
| Migration format | Raw SQL files, numeric prefix | Simplest approach; no migration framework dependency; psql applies them directly |
| Migration runner | Bash script with psql + DATABASE_URL | Single file, no dependencies beyond psql; CI-compatible |
| Seed data format | Raw SQL with INSERT statements | Same as migrations; directly loadable with psql |
| ID format | NanoID (PRT-..., STN-..., CHG-...) | Per ADR-003 and ev-core crate from Sprint 1.1 |
| Connector type values | Type2, Type2Combo, Chademo, CCS, Schuko, Wall | Aligned with ev-core ConnectorType enum (per clarification) |
| Spatial index type | GiST (Generalized Search Tree) | PostgreSQL standard for spatial indexing; required for ST_DWithin performance |

## Alternatives Considered

| Alternative | Rejected Because |
|-------------|-----------------|
| SQLx migration framework | Requires Rust compilation; not suitable for raw DB setup before services run |
| Flyway / Liquibase | External dependencies not justified for 6 migrations; ADR-005 favors simplicity |
| Embedded migration in binary | sqlx::migrate! used in service startup (Sprint 1.3); migrate.sh is for standalone DB setup |
| Docker-entrypoint-initdb.d | Only runs on first container start; cannot re-run migrations on existing DB |
| UUIDs for IDs | Rejected per ADR-003; NanoIDs are human-friendly and URL-safe |
