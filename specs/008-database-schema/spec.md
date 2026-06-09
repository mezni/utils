# Feature Specification: Database Schema

**Feature Branch**: `008-database-schema`

**Created**: 2026-06-09

**Status**: Draft

**Input**: Sprint 2.2 — PostgreSQL + PostGIS schema migrations for partner, station, and charger tables. CHECK constraints on lat/lng ranges, connector types, and power values. Spatial index on station coordinates. Dev seeds replacing json-server db.json.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Database Schema Migrations Applied (Priority: P1)

A developer sets up the database from scratch by running migrations. Four sequential migrations create the schema: the ev-platform schema, the partner table with type and flag columns, the station table with spatial column and constraints, and the charger table with connector type and power constraints. All CHECK constraints are verified — invalid data (latitude > 90, negative power, unknown connector type) is rejected at the database level.

**Why this priority**: Every service in MVP-2 depends on the database schema. Without migrations, no service can store or retrieve data. The schema must be correct and complete before any service code is written.

**Independent Test**: Run all migrations against a fresh PostgreSQL 17 database with PostGIS. Verify all four tables exist with the correct columns, constraints, and indexes. Attempt INSERTs that violate each CHECK constraint — each is rejected with a clear error.

**Acceptance Scenarios**:

1. **Given** a fresh PostgreSQL + PostGIS database, **When** migration 0001 is applied, **Then** the `ev-platform` schema exists with a `schema_version` table tracking applied migrations.
2. **Given** migration 0001 is complete, **When** migration 0002 is applied, **Then** the `partner` table exists under `ev-platform` with columns: id, name, type, is_verified, is_live, is_active, created_at, created_by, updated_at, updated_by — all with correct types and constraints.
3. **Given** migrations 0001-0002 are complete, **When** migration 0003 is applied, **Then** the `station` table exists under `ev-platform` with columns: id, partner_id (FK to partner), name, address, latitude, longitude, location (PostGIS geometry), created_at, created_by, updated_at, updated_by — plus a spatial GIST index on location and CHECK constraints on latitude (-90 to 90) and longitude (-180 to 180).
4. **Given** migrations 0001-0003 are complete, **When** migration 0004 is applied, **Then** the `charger` table exists under `ev-platform` with columns: id, station_id (FK to station), connector_type, power_kw, status, created_at, created_by, updated_at, updated_by — plus CHECK constraints on connector_type (type2, type3, ccs, chademo), power_kw (> 0), and status (available, in_use, maintenance, offline).
5. **Given** all migrations are applied, **When** a row with latitude 100 is inserted into station, **Then** the database rejects the insert with a constraint violation error.
6. **Given** all migrations are applied, **When** a row with power_kw 0 is inserted into charger, **Then** the database rejects the insert with a constraint violation error.

---

### User Story 2 — Dev Seeds Populate Tables (Priority: P2)

A developer seeds the database with realistic test data equivalent to the existing json-server db.json. After seeding, the database contains 3 partners, 15 stations across Tunisian cities, 24 chargers, and 15 availability records — matching the MVP-1 seed data exactly.

**Why this priority**: Dev seeds enable manual testing and integration testing of services without requiring production data. Seeds must match the existing mock data to ensure backward compatibility.

**Independent Test**: Run the seed script against a fresh database with migrations applied. Query each table and verify row counts: partners = 3, stations = 15, chargers = 24, availability records = 15.

**Acceptance Scenarios**:

1. **Given** all migrations are applied to an empty database, **When** the seed script is executed, **Then** the partner table contains exactly 3 rows matching the partners from db.json.
2. **Given** seeded data, **When** the station table is queried, **Then** it contains exactly 15 stations with correct partner_id foreign key references.
3. **Given** seeded data, **When** the charger table is queried, **Then** it contains exactly 24 chargers with correct station_id references and valid connector types.
4. **Given** seeded data, **When** the station_availability table is queried, **Then** it contains exactly 15 availability records with correct station_id references.
5. **Given** seeded data, **When** the seed script is run again on the same database, **Then** it does not create duplicate rows (idempotent — truncate and re-insert or upsert).

---

### User Story 3 — Spatial Queries Return Correct Results (Priority: P2)

A developer writes a spatial query to find stations within a given radius of a point. The spatial index on station.location ensures the query uses an index scan rather than a sequential scan. The query returns only stations from partners that are verified, live, and active.

**Why this priority**: Spatial queries are the core of the Driver Service (Sprint 2.3). The spatial index must be confirmed working before service code is written. The partner visibility filter must be proven at the query level.

**Independent Test**: Run `EXPLAIN ANALYZE` on a ST_DWithin query against the seeded stations. The plan shows an index scan using the spatial GIST index. Then query with a radius of 100km from Tunis center — it returns only stations within that range whose partner is verified, live, and active.

**Acceptance Scenarios**:

1. **Given** seeded stations with location coordinates, **When** a `ST_DWithin` query is executed with `EXPLAIN ANALYZE`, **Then** the query plan shows an index scan using the `idx_station_location` GIST index.
2. **Given** seeded stations, **When** a query filters stations within 100km of Tunis center (36.8008, 10.1815), **Then** only stations within that radius are returned.
3. **Given** seeded data with one unverified partner, **When** a query joins station to partner with is_verified=true, is_live=true, is_active=true, **Then** stations belonging to the unverified partner are excluded from results.

---

### Edge Cases

- Migration run on an existing database with data — should fail gracefully if tables already exist (use IF NOT EXISTS or version tracking).
- Spatial index creation on a table with existing data — should succeed without errors.
- CHECK constraint violation error messages — must identify the violated constraint and the offending value.
- Seed script run on non-empty database — must truncate before inserting to guarantee clean state.
- Station coordinates at the exact poles (latitude ±90) — allowed by CHECK constraint.
- Connector type with unexpected casing — CHECK constraint is case-sensitive; seeds must use lowercase.
- Foreign key violations — inserting a station with a non-existent partner_id must fail with FK error.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Migration 0001 MUST create the `ev-platform` schema and a `schema_version` tracking table.
- **FR-002**: Migration 0002 MUST create the `partner` table with columns: id (TEXT PRIMARY KEY), name (TEXT NOT NULL), type (TEXT NOT NULL), is_verified (BOOLEAN NOT NULL DEFAULT false), is_live (BOOLEAN NOT NULL DEFAULT false), is_active (BOOLEAN NOT NULL DEFAULT true), created_at (TIMESTAMPTZ NOT NULL), created_by (TEXT NOT NULL), updated_at (TIMESTAMPTZ NOT NULL), updated_by (TEXT NOT NULL).
- **FR-003**: Migration 0002 MUST add a CHECK constraint on partner.type allowing only 'business' or 'personal'.
- **FR-004**: Migration 0003 MUST create the `station` table with columns: id (TEXT PRIMARY KEY), partner_id (TEXT NOT NULL REFERENCES partner(id)), name (TEXT NOT NULL), address (TEXT), latitude (DOUBLE PRECISION NOT NULL), longitude (DOUBLE PRECISION NOT NULL), location (GEOMETRY(Point, 4326) NOT NULL), created_at (TIMESTAMPTZ NOT NULL), created_by (TEXT NOT NULL), updated_at (TIMESTAMPTZ NOT NULL), updated_by (TEXT NOT NULL).
- **FR-005**: Migration 0003 MUST add CHECK constraints: latitude BETWEEN -90 AND 90, longitude BETWEEN -180 AND 180.
- **FR-006**: Migration 0003 MUST compute `location` as `ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)` — can be a generated column or set by trigger/application.
- **FR-007**: Migration 0003 MUST create a GIST spatial index on `station.location`.
- **FR-008**: Migration 0004 MUST create the `charger` table with columns: id (TEXT PRIMARY KEY), station_id (TEXT NOT NULL REFERENCES station(id)), connector_type (TEXT NOT NULL), power_kw (DOUBLE PRECISION NOT NULL), status (TEXT NOT NULL), created_at (TIMESTAMPTZ NOT NULL), created_by (TEXT NOT NULL), updated_at (TIMESTAMPTZ NOT NULL), updated_by (TEXT NOT NULL).
- **FR-009**: Migration 0004 MUST add CHECK constraints: connector_type IN ('type2', 'type3', 'ccs', 'chademo'), power_kw > 0, status IN ('available', 'in_use', 'maintenance', 'offline').
- **FR-010**: Migration 0004 MUST create the `station_availability` table with columns: id (TEXT PRIMARY KEY), station_id (TEXT NOT NULL REFERENCES station(id)), status (TEXT NOT NULL), updated_by (TEXT NOT NULL), updated_at (TIMESTAMPTZ NOT NULL).
- **FR-011**: Migration 0004 MUST add a CHECK constraint on station_availability.status IN ('available', 'partial', 'unavailable').
- **FR-012**: Dev seeds MUST populate partner, station, charger, and station_availability tables with the same data as the existing source/mock/db.json.
- **FR-013**: Dev seeds MUST be idempotent — running them multiple times produces the same final state.
- **FR-014**: All migration files MUST be located under a `database/migrations/` directory and named sequentially (0001_*.sql, 0002_*.sql, etc.).

### Key Entities

- **partner**: Business or personal entity that owns charging stations. Has lifecycle flags (is_verified, is_live, is_active) and full audit trail.
- **station**: Physical charging station location. Belongs to one partner. Has geographic coordinates with PostGIS geometry. Spatial index enables proximity queries.
- **charger**: Individual charging unit at a station. Has connector type, power rating, and operational status.
- **station_availability**: Time-series availability status records for stations. Each row records a status change with timestamp and actor.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All four migrations apply sequentially without errors on a fresh PostgreSQL 17 + PostGIS 3 database.
- **SC-002**: Each CHECK constraint rejects its corresponding invalid value with a clear error message identifying the constraint.
- **SC-003**: A spatial ST_DWithin query against seeded stations uses an index scan (confirmed by EXPLAIN ANALYZE).
- **SC-004**: Dev seeds populate all four tables with exactly 3 partners, 15 stations, 24 chargers, and 15 availability records.
- **SC-005**: Running dev seeds twice produces the same result as running them once (idempotent).
- **SC-006**: A developer can apply all migrations + seeds in under 2 minutes on a local machine.
- **SC-007**: All foreign key relationships are enforced — orphan records cannot be inserted.

## Assumptions

- PostgreSQL 17 with PostGIS 3 extension is installed and available.
- The database is created manually or by Docker Compose (not part of this sprint — Sprint 2.5 covers Docker Compose).
- Migration framework: raw SQL files executed by sqlx migrations (matching the sqlx-based approach in ev-db crate from Sprint 2.1).
- The location column is a computed GEOMETRY(Point, 4326) derived from latitude/longitude — can be a generated column or maintained by the application layer.
- Seeds are SQL INSERT scripts in database/seeds/ — not binary or CSV imports.
- No data migration from json-server is needed — seeds are written fresh to match the existing data.
- The station_availability table stores the full history of status changes (not just the latest) — consuming services query for the latest record per station.
