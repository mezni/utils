# Feature Specification: Database — GIS and Inventory Schemas

**Feature Branch**: `002-database-gis-inventory`

**Created**: 2026-06-07

**Status**: Draft

**Input**: Sprint 1.2 per roadmap — Database migrations for inventory (partner, station, charger, availability) and GIS (OSM spatial tables, station locations) schemas, plus development seed data and a migration runner.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Core Inventory Schema (Priority: P1)

A developer sets up the database from scratch by running migrations. The inventory schema is created with tables for partners, stations, chargers, and station availability. The database is ready for the Driver Service and Admin Service to store and query business data.

**Why this priority**: The inventory tables are the foundation for every backend service. Without them, neither the Driver Service nor the Admin Service can persist or retrieve data.

**Independent Test**: Run all migrations on a fresh PostgreSQL instance. Verify that the `inventory.partner`, `inventory.station`, `inventory.charger`, and `inventory.station_availability` tables exist with the correct columns and constraints.

**Acceptance Scenarios**:

1. **Given** a fresh PostgreSQL 16 database with PostGIS 3.4, **When** all migrations run from start to finish, **Then** no errors occur and all four inventory tables exist
2. **Given** the inventory schema exists, **When** a developer inspects `inventory.partner`, **Then** it has columns `id` (TEXT, PK), `name` (TEXT, NOT NULL), and `created_at` (TIMESTAMPTZ, DEFAULT now())
3. **Given** the inventory schema exists, **When** a developer inspects `inventory.station`, **Then** it has a foreign key to `inventory.partner(id)` and latitude/longitude columns are NOT NULL
4. **Given** the inventory schema exists, **When** a developer inspects `inventory.charger`, **Then** it has a foreign key to `inventory.station(id)` and a default status of 'available'
5. **Given** the inventory schema exists, **When** a developer runs the migrations a second time, **Then** no errors occur (idempotent)

---

### User Story 2 — GIS Schema for Spatial Queries (Priority: P1)

A developer runs migrations and the GIS schema is created with spatial tables for OSM data and station locations. Spatial indexes are in place, enabling efficient geographic queries for the "stations nearby" feature.

**Why this priority**: The GIS schema is required for spatial queries that power the core "stations nearby" user-facing feature in the Driver Service.

**Independent Test**: Run all migrations on a fresh PostgreSQL instance. Verify that all GIS tables exist with the correct geometry columns and GiST indexes.

**Acceptance Scenarios**:

1. **Given** all migrations have run, **When** the developer queries `gis.station_locations`, **Then** the table exists with a `geom` column of type `GEOMETRY(Point, 4326)` and a primary key referencing `inventory.station(id)`
2. **Given** the GIS schema exists, **When** the developer inspects indexes, **Then** GiST indexes exist on `gis.roads.geom`, `gis.boundaries.geom`, `gis.osm_nodes.geom`, `gis.osm_ways.geom`, `gis.amenity_points.geom`, and `gis.station_locations.geom`
3. **Given** all tables exist with spatial indexes, **When** the developer runs `ST_DWithin` on `gis.station_locations` within a 5 km radius, **Then** the query uses the GiST index

---

### User Story 3 — Development Seed Data (Priority: P2)

A developer runs seed scripts after migrations and has realistic sample data in their local database. The seeds insert partners, stations, and chargers so the developer can test frontend and backend features immediately without connecting to a production database.

**Why this priority**: Seed data accelerates development by providing realistic test data without relying on production or manual data entry.

**Independent Test**: Run seed scripts against a migrated database. Verify that exactly 3 partners, 15 stations, and 24 chargers are inserted and that referential integrity is maintained.

**Acceptance Scenarios**:

1. **Given** the database has all migrations applied, **When** the developer runs `db/seeds/dev_partners.sql`, **Then** exactly 3 partners are inserted
2. **Given** partners exist, **When** the developer runs `db/seeds/dev_stations.sql`, **Then** exactly 15 stations are inserted, each linked to an existing partner
3. **Given** stations exist, **When** the developer runs `db/seeds/dev_chargers.sql`, **Then** exactly 24 chargers are inserted, each linked to an existing station, with valid connector types and power ratings

---

### Edge Cases

- **Running migrations on an already-migrated database**: All migrations must be idempotent — running them a second time must not produce errors or duplicate rows
- **Seed data idempotency**: Running seed scripts a second time must not create duplicate records or violate unique constraints
- **PostGIS already installed**: The PostGIS extension creation must gracefully handle the case where it is already present
- **Partial migration failure**: If a migration fails partway through, the runner must stop and report the error clearly, leaving the database in a recoverable state
- **Missing migration directory**: The migration runner must handle missing or empty migration/seed directories gracefully

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Three database extensions MUST be installed: PostGIS, uuid-ossp, and pgcrypto. All must be idempotent (IF NOT EXISTS).
- **FR-002**: Two PostgreSQL schemas MUST be created: `inventory` and `gis`. Creation must be idempotent.
- **FR-003**: The `inventory` schema MUST contain four tables: `partner`, `station`, `charger`, and `station_availability`, matching the column definitions in the schema reference.
- **FR-004**: Foreign key constraints MUST exist: `station.partner_id → partner.id`, `charger.station_id → station.id`, `station_availability.station_id → station.id`. Station latitude and longitude MUST be NOT NULL.
- **FR-005**: Inventory indexes MUST exist: composite on `station(latitude, longitude)`, and individual indexes on `station(partner_id)`, `charger(station_id)`, and `station_availability(station_id)`.
- **FR-006**: The `gis` schema MUST contain six tables: `osm_nodes`, `osm_ways`, `roads`, `boundaries`, `amenity_points`, and `station_locations`, matching the column definitions in the schema reference.
- **FR-007**: GiST indexes MUST exist on the geometry column of each GIS table: `osm_nodes(geom)`, `osm_ways(geom)`, `roads(geom)`, `boundaries(geom)`, `amenity_points(geom)`, and `station_locations(geom)`.
- **FR-008**: A migration runner script MUST exist at `db/migrations/migrate.sh` that applies all `.sql` files in `db/migrations/` in ascending numeric order and stops on first error.
- **FR-009**: The migration runner MUST accept a `DATABASE_URL` environment variable for the database connection.
- **FR-010**: Seed scripts MUST exist in `db/seeds/` — `dev_partners.sql` (3 partners), `dev_stations.sql` (15 stations), and `dev_chargers.sql` (24 chargers) — with data referencing Tunisia locations.
- **FR-011**: All migrations MUST be individually idempotent (safe to re-run using IF NOT EXISTS / IF EXISTS patterns).
- **FR-012**: The three database extensions MUST be the only extensions installed by this sprint.

### Key Entities *(include if feature involves data)*

- **Partner**: An organization that owns charging stations. Has a NanoID (PRT-...) and a display name.
- **Station**: A physical location with one or more chargers. Belongs to a partner. Has coordinates, name, and optional address.
- **Charger**: An individual charging unit at a station. Has a connector type, power rating, and status. Belongs to a station.
- **StationAvailability**: The operational status of a station (available, unavailable, partial). Tracks who updated it and when.
- **StationLocation** (gis schema): The PostGIS point geometry for a station, derived from inventory.station. Includes nearest road and region references.
- **Roads, Boundaries, OSM Nodes/Ways, AmenityPoints** (gis schema): Spatial reference tables imported from OpenStreetMap data for route snapping, region lookup, and points of interest.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All six migrations complete from zero in under 30 seconds on a local development environment
- **SC-002**: A spatial query using `ST_DWithin(station_locations.geom, point, 5000)` returns results in under 100 ms on a seeded database with 15 stations
- **SC-003**: Seed scripts insert all 42 records (3 partners + 15 stations + 24 chargers) in under 5 seconds total
- **SC-004**: Running all migrations twice in succession produces zero errors and zero duplicate records
- **SC-005**: A new developer can run `./db/migrations/migrate.sh` (with valid DATABASE_URL) and have all tables in place in under 1 minute, without referring to documentation beyond the script name

## Assumptions

- PostgreSQL 16 with PostGIS 3.4 is available (defined in Docker Compose from Sprint 1.1)
- The migration runner will be called from a machine with `psql` installed and network access to the database
- Seed data uses realistic Tunisian city names and coordinates but is entirely synthetic — no real customer data
- The migration runner executes `.sql` files in filename order (0001, 0002, ... 0006)
- No migration will ever be edited after it is committed (per constitution rule)
- The `gis.station_locations` table is created with a foreign key to `inventory.station` but the GIS sync trigger (populating snapped_road_id, region_id) is deferred to Sprint 6.x
