# Research: Infrastructure & Database Setup

## Decision Log

### Decision 1: Database Topology

- **Decision**: Two separate PostgreSQL 16 containers — platform_db (with PostGIS)
  and analytics_db (plain PostgreSQL)
- **Rationale**: Clear separation of concerns between operational data (inventory
  with spatial queries) and analytics data (append-only event log). Separate
  containers allow independent scaling, backup policies, and failure isolation.
- **Alternatives considered**:
  - Single database instance with two databases — rejected: couples operational
    and analytics concerns, single point of failure for both workloads
  - Single instance, analytics as schema in platform_db — rejected: complicates
    access control and backup policies

### Decision 2: Spatial Index Strategy

- **Decision**: GiST (Generalized Search Tree) spatial index on
  `inventory.station.location` column, partial index excluding soft-deleted rows
- **Rationale**: GiST is the standard choice for geographic range queries
  (ST_DWithin). Partial index (`WHERE deleted_at IS NULL`) reduces index size
  and improves selectivity by excluding soft-deleted stations.
- **Alternatives considered**:
  - BRIN index — space-efficient but slower for range queries
  - SP-GiST index — faster but larger disk footprint
- **Reference**: ADR-005 — PostGIS Spatial Indexes

### Decision 3: Migration Script Design

- **Decision**: Versioned, idempotent SQL migration files in `infra/migrations/`
  with `IF NOT EXISTS` guards
- **Rationale**: Idempotency is required by FR-007. Sequential numbering
  (001-, 002-, etc.) provides clear ordering. SQL-only keeps dependencies
  minimal (no migration framework required for MVP-1).
- **Alternatives considered**:
  - sqlx migrations (Rust-native) — rejected: would couple infra setup to
    backend language choice; SQL files are language-agnostic
  - Dedicated migration tool (flyway, goose) — rejected: overkill for two
    databases with <10 tables; adds tooling dependency

### Decision 4: Seed Data Strategy

- **Decision**: Static SQL INSERT statements in migration 005-seed-data.sql,
  loaded during initial setup
- **Rationale**: Simplest approach for MVP-1. SQL is self-documenting,
  version-controlled, and runs alongside schema migrations. Sufficient for
  initial testing with 2 partners, 3 stations, 5 chargers.
- **Alternatives considered**:
  - TypeScript seed script — rejected: adds Node.js dependency to infra setup
  - API-based seeding — rejected: backend services not yet built in Phase 1

### Decision 5: Startup Orchestration

- **Decision**: Docker Compose with health checks and dependency ordering
- **Rationale**: Single-command startup (FR-001). Health checks ensure services
  report ready only after database is accepting connections. Dependency
  declarations (`depends_on`) enforce startup order.
- **Alternatives considered**:
  - Shell script with polling — rejected: reinvents health checks
  - Makefile targets — rejected: adds build-tool dependency

## Technology Versions

| Component | Version | Source |
|-----------|---------|--------|
| Docker Compose | 3.9 | Industry standard |
| PostgreSQL | 16 | Constitution mandate |
| PostGIS | 3.4 | Matches postgis/postgis:16-3.4 image |

## Open Questions (Deferred)

- **Seed data expansion**: Additional Tunisian cities/regions can be added in
  later sprints as the map feature evolves
- **OSM GIS data**: Loading actual OSM boundaries for the gis schema is deferred
  to when region-based filtering is needed (MVP-3+)
