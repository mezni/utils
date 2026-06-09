# Tasks: Database Schema

**Input**: Design documents from `/specs/008-database-schema/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md

**Tests**: No test tasks generated (constraints are verified via manual SQL INSERT tests defined in quickstart.md)

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

All paths are relative to the repository root. SQL migrations live under `database/migrations/`. Seed scripts under `database/seeds/`.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Database directory structure and tooling configuration

- [X] T001 Create `database/` directory structure at repo root with `migrations/` and `seeds/` subdirectories

**Checkpoint**: Directory structure ready for migration and seed files

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Migration framework configuration — sqlx-compatible migration naming

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T002 Create sqlx migration tracking table structure by adding initial `database/.sqlx` configuration comment in `database/migrations/README.md`

**Checkpoint**: Migration conventions documented — migration files can be created

---

## Phase 3: User Story 1 — Database Schema Migrations Applied (Priority: P1) 🎯 MVP

**Goal**: Four sequential SQL migrations create the ev-platform schema, partner table, station table (with spatial column + GIST index), and charger + station_availability tables — all with CHECK constraints and FK relationships

**Independent Test**: Run all four migrations against a fresh PostgreSQL 17 + PostGIS 3 database. Verify all tables exist with correct columns, constraints, and indexes. Attempt INSERTs that violate each CHECK constraint — each is rejected with a clear error message naming the constraint.

### Implementation for User Story 1

- [X] T003 [P] [US1] Create migration 0001 in `database/migrations/0001_create_ev_platform_schema.sql` — CREATE SCHEMA IF NOT EXISTS ev-platform; CREATE TABLE IF NOT EXISTS ev-platform.schema_version tracking table
- [X] T004 [US1] Create migration 0002 in `database/migrations/0002_create_partner_table.sql` — partner table with all columns, ck_partner_type CHECK constraint, NOT NULL + DEFAULT constraints, descriptive constraint names
- [X] T005 [US1] Create migration 0003 in `database/migrations/0003_create_station_table.sql` — station table with lat/lng CHECK constraints, FK to partner, GEOMETRY(Point, 4326) location column, BEFORE INSERT OR UPDATE trigger computing ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326), GIST spatial index idx_station_location
- [X] T006 [P] [US1] Create migration 0004 in `database/migrations/0004_create_charger_and_availability_tables.sql` — charger table with ck_charger_connector_type, ck_charger_power_kw (> 0), ck_charger_status CHECK constraints, FK to station; station_availability table with ck_availability_status CHECK constraint, FK to station
- [X] T007 [US1] Verify all four migrations are syntactically valid — parse with `psql -f` against a temp database

**Checkpoint**: All four migrations create the schema correctly — ev-platform schema with partner, station, charger, station_availability tables, all constraints, indexes, and trigger

---

## Phase 4: User Story 2 — Dev Seeds Populate Tables (Priority: P2)

**Goal**: Seed scripts populate all four tables with data matching the existing source/mock/db.json (3 partners, 15 stations, 24 chargers, 15 availability records). Seeds are idempotent via TRUNCATE CASCADE + INSERT.

**Independent Test**: Run seeds against a fresh database with migrations applied. Query each table and verify exact row counts match db.json. Run seeds again — same result, no duplicates.

### Implementation for User Story 2

- [X] T008 [US2] Create partner seed in `database/seeds/001_partners.sql` — TRUNCATE ev-platform.partner CASCADE; INSERT 3 partners matching PRT001, PRT002, PRT003 from db.json with all flags and audit fields
- [X] T009 [P] [US2] Create station seed in `database/seeds/002_stations.sql` — TRUNCATE ev-platform.station CASCADE; INSERT 15 stations across Tunisian cities with correct partner_id FK references, lat/lng coordinates, and audit fields
- [X] T010 [P] [US2] Create charger seed in `database/seeds/003_chargers.sql` — TRUNCATE ev-platform.charger CASCADE; INSERT 24 chargers with correct station_id FK, valid connector_type, power_kw > 0, valid status
- [X] T011 [P] [US2] Create availability seed in `database/seeds/004_station_availability.sql` — TRUNCATE ev-platform.station_availability CASCADE; INSERT 15 availability records with correct station_id FK and valid status
- [X] T012 [US2] Verify seeds produce correct row counts (partners: 3, stations: 15, chargers: 24, availability: 15)

**Checkpoint**: Dev seeds populate all tables with data matching db.json — idempotent, FK-safe, correct row counts

---

## Phase 5: User Story 3 — Spatial Queries Return Correct Results (Priority: P2)

**Goal**: Spatial ST_DWithin query on station.location uses the GIST index (confirmed by EXPLAIN ANALYZE). Stations from unverified/inactive/non-live partners are excluded via JOIN filter.

**Independent Test**: Run EXPLAIN ANALYZE on a ST_DWithin query — plan shows Index Scan using idx_station_location. Run query with partner visibility JOIN — only stations from verified + live + active partners are returned.

### Implementation for User Story 3

- [X] T013 [US3] Create spatial query verification SQL in `database/queries/nearby_stations.sql` — ST_DWithin query with partner visibility JOIN (is_verified, is_live, is_active)
- [X] T014 [US3] Verify EXPLAIN ANALYZE shows Index Scan using idx_station_location (not Seq Scan)
- [X] T015 [US3] Verify partner visibility filter — stations from unverified partner (PRT003) are excluded from results

**Checkpoint**: Spatial index confirmed working — ST_DWithin uses GIST index scan, partner visibility filter correct

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Full schema verification and documentation

- [X] T016 [P] Verify all CHECK constraints reject invalid values (latitude > 90, longitude > 180, invalid connector_type, power_kw = 0, invalid status)
- [X] T017 [P] Verify all FK constraints reject orphan records (station with bad partner_id, charger with bad station_id)
- [X] T018 [P] Verify seed idempotency — running seeds twice produces identical row counts and data
- [X] T019 Verify quickstart.md instructions work end to end — apply migrations, run seeds, verify constraints

**Checkpoint**: All 3 user stories complete. Schema fully verified — CHECK constraints, FK constraints, spatial index, seeds all confirmed working.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup
- **User Story 1 (Phase 3)**: Depends on Foundational — BLOCKS all other stories
- **User Story 2 (Phase 4)**: Depends on Phase 3 — seeds need tables to exist
- **User Story 3 (Phase 5)**: Depends on Phase 3 + Phase 4 — spatial queries need seeded data
- **Polish (Phase 6)**: Depends on all user stories complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational — No dependencies on other stories
- **User Story 2 (P2)**: Depends on US1 (tables must exist) — No dependency on US3
- **User Story 3 (P2)**: Depends on US1 (tables must exist) + US2 (seeded data for query testing)

### Within Each User Story

- Migration files before verification
- Seed files before row count verification
- Query file before EXPLAIN ANALYZE verification

### Parallel Opportunities

- T003, T006 (migration 0001 and 0004) — can run in parallel (different files)
- T009, T010, T011 (seed files) — can all run in parallel (different tables)
- T016, T017 (constraint verification) — can run in parallel
- US2 seed files are independent of each other (different tables, FK-safe via TRUNCATE CASCADE order)

---

## Parallel Example: Seed Files

```bash
# All seed files can be run in parallel (different tables):
Task: "Run database/seeds/001_partners.sql"
Task: "Run database/seeds/002_stations.sql"
Task: "Run database/seeds/003_chargers.sql"
Task: "Run database/seeds/004_station_availability.sql"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1 (all 4 migrations)
4. **STOP and VALIDATE**: Run all migrations, verify tables + constraints + spatial index
5. This is the MVP — schema is usable by downstream sprints

### Incremental Delivery

1. Complete Setup + Foundational → database/ directory structure exists
2. Add User Story 1 → All 4 migrations applied → Schema ready (MVP!)
3. Add User Story 2 → Seed scripts populate test data → Verify row counts
4. Add User Story 3 → Spatial query verification → Confirm index scan

### Parallel Team Strategy

With multiple developers:
1. Team completes Setup + Foundational + US1 together
2. Once migrations are done:
   - Developer A: Seed files (US2)
   - Developer B: Spatial query verification (US3 — needs seeds first, so starts after US2)

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story can be independently verified
- No Rust code required in this sprint — all SQL
- PostgreSQL 17 + PostGIS 3 must be available for verification
- Migration files use `IF NOT EXISTS` for idempotent re-runs
- Seed files use `TRUNCATE CASCADE` for idempotency
- All CHECK constraints have descriptive names starting with `ck_`
- All FK constraints have descriptive names starting with `fk_`
