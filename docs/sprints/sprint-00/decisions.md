# Sprint 00 — Architecture Decision Records

## ADR-001: Use trilo/axum as Rust Web Framework

**Status:** Accepted  
**Context:** Need a production-grade async HTTP framework for microservices.  
**Decision:** Use trilo/axum for all services. It is type-safe, async-native, has excellent ergonomics, and is the most actively maintained Rust web framework.  
**Consequences:** Consistent API patterns across all services; large ecosystem of middleware.

## ADR-002: PostGIS as Single Geospatial Engine

**Status:** Accepted  
**Context:** Services need to query stations by proximity. GIS logic could live in application code or in the database.  
**Decision:** All spatial logic lives in PostgreSQL via PostGIS. Services never compute distances or projections.  
**Consequences:** Zero application-level GIS code; DB triggers maintain consistency; queries are optimized via GiST indexes; services remain thin.

## ADR-003: EV Schema as Single Source of Truth

**Status:** Accepted  
**Context:** Business data (partners, stations, connectors) must have one authoritative location to prevent data drift.  
**Decision:** `ev` schema is the sole source of truth. The `gis` schema is purely derived via triggers. No service writes to `gis`.  
**Consequences:** Strong consistency guarantee; trigger-based synchronization adds a small write latency but eliminates drift.

## ADR-004: Strong CQRS Separation

**Status:** Accepted  
**Context:** Admin Service handles writes, Driver Service handles reads. Mixing concerns leads to complex permission models and unclear ownership.  
**Decision:** Admin Service writes to `ev` only; Driver Service reads via `gis.nearby_stations()` only. No cross-service DB writes.  
**Consequences:** Clear ownership boundaries; simpler testing; easier to optimize read and write paths independently.

## ADR-005: Prefixed Entity IDs

**Status:** Accepted  
**Context:** UUIDs are globally unique but not human-readable. In API responses, knowing the entity type from the ID is useful.  
**Decision:** Use `PRT_`, `STN_`, `CON_` prefixes for partner, station, and connector IDs. The prefix is part of the display representation; the DB stores full UUIDs.  
**Consequences:** Better developer UX; slightly more complex ID generation; no impact on DB schema.

## ADR-006: SQLx with Compile-Time Verification

**Status:** Accepted  
**Context:** Raw SQL strings are error-prone and a security risk.  
**Decision:** Use SQLx with compile-time checked queries against the database. Migrations are SQL-first.  
**Consequences:** SQL errors caught at compile time; requires a running DB during compilation; prevents SQL injection.

## ADR-007: Workspace-Based Monorepo

**Status:** Accepted  
**Context:** Multiple services share domain types, DTOs, and utility code.  
**Decision:** Use Cargo workspace with three crates: `admin-service`, `driver-service`, `auth-service`. Shared code lives in a `lib/` crate.  
**Consequences:** Single `Cargo.lock`; shared types without publishing; unified CI; but larger build scope.
