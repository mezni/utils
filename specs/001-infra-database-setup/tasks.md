# Tasks: Infrastructure & Database Setup

**Input**: Design documents from `specs/001-infra-database-setup/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create project directory structure and configuration scaffolding

- [X] T001 Create infra/ directory structure per plan.md (`infra/migrations/`, `scripts/`)
- [X] T002 Create `.gitignore` with PostgreSQL data volume exclusions
- [X] T003 Add `AGENTS.md` reference to tasks.md (if not already present)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T004 Create `infra/docker-compose.yml` with both DB services, networks, volumes per contracts/docker-compose.md
- [X] T005 Create `infra/.env.example` with all env vars documented per contracts/env-vars.md
- [X] T006 Create `scripts/dev.sh` single-command startup wrapper with health-check polling

**Checkpoint**: Foundation ready — both containers start and become healthy; no schemas yet

---

## Phase 3: User Story 1 — Reproducible Local Development (Priority: P1) 🎯 MVP

**Goal**: A developer can start the entire platform with `scripts/dev.sh` and verify both databases are reachable

**Independent Test**: Run `scripts/dev.sh`, then `psql` to each database — both accept connections

### Implementation for User Story 1

- [X] T007 [P] [US1] Create `infra/migrations/001-platform-db-init.sql` — create schemas, enable PostGIS per data-model.md
- [X] T008 [P] [US1] Create `infra/migrations/004-analytics-db-init.sql` — create raw_events table with append-only rules per data-model.md
- [X] T009 [US1] Create `scripts/init-dbs.sh` — run platform_db migrations (001) then analytics_db migrations (004) with idempotency
- [X] T010 [US1] Wire migration script into `scripts/dev.sh` as a post-health-check step
- [X] T011 [US1] Update `scripts/dev.sh` to validate connectivity (pg_isready + schema inspection)
- [X] T012 [US1] Test restart persistence: `docker compose down && docker compose up` — data survives

**Checkpoint**: At this point, `scripts/dev.sh` starts both databases, runs init migrations, and validates connectivity. Schemas exist for inventory, gis, and analytics.

---

## Phase 4: User Story 2 — Database Schema Initialization (Priority: P1)

**Goal**: All inventory and GIS tables exist with correct columns, constraints, and spatial indexes

**Independent Test**: Connect to platform_db and run `\dt inventory.*` and `\dt gis.*` — all tables present with correct DDL

### Implementation for User Story 2

- [X] T013 [US2] Create `infra/migrations/002-inventory-schema.sql` — partner, station, charger tables + all indexes from data-model.md
- [X] T014 [US2] Create `infra/migrations/003-gis-schema.sql` — osm_region, osm_road tables + spatial indexes from data-model.md
- [X] T015 [US2] Append 002 and 003 into `scripts/init-dbs.sh` migration sequence
- [X] T016 [US2] Validate idempotency: re-run init-dbs.sh — no errors, no duplicate objects
- [X] T017 [US2] Verify spatial index via `EXPLAIN SELECT ... ST_DWithin(location, ...)` — index scan (not seq scan)
- [X] T018 [US2] Verify append-only rules on analytics_db: attempt UPDATE/DELETE on raw_events — both rejected

**Checkpoint**: At this point, platform_db has full inventory + GIS schema with spatial indexes. analytics_db enforces append-only rules.

---

## Phase 5: User Story 3 — Test Data with Real Coordinates (Priority: P2)

**Goal**: Realistic Tunisia seed data pre-loaded with multiple partners, stations, and charger types

**Independent Test**: Query `SELECT COUNT(*) FROM inventory.station` returns ≥ 3 stations with real Tunisian coordinates

### Implementation for User Story 3

- [X] T019 [US3] Create `infra/migrations/005-seed-data.sql` with:
  - [X] 2+ partners (e.g., TotalEnergies TN, STEG)
  - [X] 3+ stations with real Tunisia coordinates (Tunis, Sfax, Sousse)
  - [X] 5+ chargers across CCS2, CHAdeMO, Type2 types
- [X] T020 [US3] Append 005 to end of `scripts/init-dbs.sh` migration sequence
- [X] T021 [US3] Validate seed data: nearby search returning results ordered by distance with correct values
- [X] T022 [US3] Validate idempotency: re-run seed migration — no duplicate rows (use IF NOT EXISTS or idempotent insert pattern)

**Checkpoint**: At this point, all three user stories are complete. Seed data is loaded and queryable.

---

## Phase 5.5: Performance Validation (SC-005)

**Purpose**: Enable verification of the 1000-station / 100ms success criterion

- [X] T022b Create `scripts/generate-load-data.sh` — bulk-insert script that generates 1000 stations with random Tunisia coordinates
- [X] T022c [P] [US2] Run explain-plan on ST_DWithin 10km radius query with 1000 stations — assert index scan
- [X] T022d [P] [US2] Run `EXPLAIN ANALYZE` on same query — assert execution time < 100ms

---

## Phase 6: Verification & Polish

**Purpose**: End-to-end validation and documentation finalization

- [X] T023 [P] Run full `scripts/dev.sh` from clean state — measure time to healthy (< 5 min) — containers healthy in < 3 min
- [ ] T024 [P] Test port conflict scenario: start service when port 5432 is occupied — graceful error message (manual)
- [ ] T025 [P] Test container registry unavailable — graceful error message (manual)
- [X] T026 [P] Quickstart verification: follow quickstart.md instructions end-to-end, fix any gaps — .env URL password mismatch fixed
- [X] T027 [P] Run schema inspection checklist from checklists/requirements.md — all items pass
- [X] T028 [P] Verify all SC metrics from spec.md: SC-001 through SC-007

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational — docker-compose.yml must exist
- **User Story 2 (Phase 4)**: Depends on Phase 3 (001 and 004 migrations exist) — 002/003 add more tables
- **User Story 3 (Phase 5)**: Depends on Phase 4 — needs inventory schema to exist before seeding
- **Verification (Phase 6)**: Depends on all user stories being complete

### Within Each User Story

- Core migration files before scripts
- Scripts before wiring into dev.sh
- Implementation before validation

### Parallel Opportunities

- T007 and T008 (001 and 004 migrations for different DBs) can be written in parallel
- T013 and T014 (002 and 003 migrations) can be written in parallel
- All Phase 6 verification tasks marked [P] can run in parallel

---

## Implementation Strategy

### Sequential Delivery

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: User Story 1 (init migrations + connectivity)
4. Complete Phase 4: User Story 2 (inventory + GIS schema)
5. Complete Phase 5: User Story 3 (seed data)
6. Complete Phase 6: Verification & Polish

### Incremental Delivery

1. **Setup + Foundational** → Both containers start and report healthy
2. **User Story 1** → Schemas exist, connectivity validated → demo-able!
3. **User Story 2** → Full inventory/GIS schema with spatial indexes
4. **User Story 3** → Realistic seed data → ready for frontend development

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- All SQL migrations MUST be idempotent (IF NOT EXISTS guards)
- Commit after each task or logical group
- Most tasks produce exactly one file under `infra/` or `scripts/`
