# Implementation Plan: MVP-1 Sprint 0 — Infrastructure & Data Foundation

**Branch**: `main` | **Date**: 2026-06-13 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-infra-data-foundation/spec.md`

**Note**: This plan is filled in by the `/speckit.plan` command and follows the BorneMap MVP-1 execution model.

## Summary

Sprint 0 establishes the **critical infrastructure foundation** for MVP-1 by:
1. Spinning up PostgreSQL + PostGIS database infrastructure via Docker Compose
2. Downloading and filtering OpenStreetMap Tunisia extract for 50–300 EV charging stations
3. Seeding real station data into PostGIS with spatial indexing
4. Validating geospatial queries (ST_DWithin, distance ordering) work correctly with <200ms latency

This sprint produces the **system of record** for all geospatial data and validates PostGIS performance before Sprint 1 backend development.

## Technical Context

**Language/Version**: Bash (OSM import scripts), SQL (database setup), YAML (Docker Compose configuration)

**Primary Dependencies**: 
- Docker & Docker Compose (infrastructure)
- PostgreSQL 16 with PostGIS 3.4 extension
- Geofabrik OSM extracts (Tunisia region)

**Storage**: 
- `platform_db` (PostgreSQL + PostGIS): inventory and gis schemas, ~50–300 station records
- `analytics_db` (PostgreSQL): empty, reserved for MVP-4

**Testing**: 
- Manual SQL validation (ST_DWithin queries, distance calculations, indexing)
- Docker health checks
- No automated test framework for Sprint 0 (validation manual)

**Target Platform**: Local development machines (macOS, Linux, Windows with WSL2), CI/staging environments

**Project Type**: Infrastructure/DevOps setup for geospatial data platform

**Performance Goals**: 
- Docker infrastructure startup: <2 minutes cold start
- PostGIS queries: <200ms latency on developer laptops
- Distance accuracy: ±1% compared to reference calculations

**Constraints**: 
- Credentials managed via `.env` file (not hardcoded)
- No authentication in Sprint 0 (Keycloak deferred to MVP-3)
- GIS schema is read-only by design (no writes)

**Scale/Scope**: 
- 50–300 EV charging stations in Tunisia
- 2 databases (platform + analytics)
- Single region (Tunisia) for MVP-1

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### BorneMap Constitution Principles (v1.0.0)

✅ **I. Documentation-First Development**
- PASS: Spec defines 3 P1 user stories, 12 acceptance scenarios, 12 functional requirements
- PASS: Architecture rules for data ownership respected (gis is read-only, each service owns schemas)

✅ **II. LLM-Driven Deterministic Execution**
- PASS: Infrastructure setup is deterministic (Docker Compose state machine)
- PASS: Data import is scripted (no manual steps beyond running scripts)

✅ **III. MVP Isolation**
- PASS: Sprint 0 scope is scoped to infrastructure + OSM import only
- PASS: No cross-MVP features; authentication (MVP-3) and analytics (MVP-4) deferred

✅ **IV. Complete Testing Requirements**
- PASS: Manual validation of acceptance scenarios (3 P1 stories × 4 scenarios = 12 tests)
- PASS: Acceptance criteria are testable (ST_DWithin queries, distance calculations, indexing)
- PASS: E2E scenario: DevOps → Data Engineer → QA workflow

✅ **V. Architecture Discipline (Backend)**
- PASS: PostGIS isolated in database layer (gis schema is read-only)
- PASS: No business logic in database setup; pure infrastructure

✅ **VI. Architecture Discipline (Frontend)**
- NOT APPLICABLE: Sprint 0 is infrastructure; frontend begins in Sprint 3

✅ **VII. Data Ownership Rules**
- PASS: platform_db owned by infrastructure layer (inventory schema for stations)
- PASS: analytics_db reserved (append-only design, no writes yet)
- PASS: gis schema is read-only (protected)

✅ **VIII. Skill System Enforcement**
- PASS: MVP Scope Enforcement: No cross-MVP features
- PASS: Data Ownership: Each schema has clear ownership boundaries
- PASS: No API violations (infrastructure layer, not REST API)

### Gate Result: ✅ **PASS**

**Justifications**: 
- Sprint 0 is pure infrastructure (Docker, database, data import)
- All 3 P1 user stories align with constitution principles
- Data ownership rules respected (gis read-only, platform_db controlled)
- MVP isolation enforced (no Sprint 1+ features in scope)

## Project Structure

### Documentation (this feature)

```text
specs/001-infra-data-foundation/
├── spec.md              # Feature specification (input)
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (to be generated)
├── data-model.md        # Phase 1 output (to be generated)
├── quickstart.md        # Phase 1 output (to be generated)
├── contracts/           # Phase 1 output (to be generated - empty for infra)
├── checklists/
│   └── requirements.md   # Quality checklist
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

```text
infra/
├── docker-compose.yml          # PostgreSQL + PostGIS + Analytics DB
├── .env.example                # Database credentials template (dev defaults)
├── migrations/                 # SQL migration scripts
│   ├── 001_extensions.sql      # PostGIS extension setup
│   ├── 002_schema_inventory.sql # Inventory schema + station table
│   └── 003_seed_stations.sql    # OSM station data (50–300 stations)
└── osm-import/
    ├── download.sh             # Download Tunisia OSM extract from Geofabrik
    ├── filter.sh               # Filter for amenity=charging_station
    ├── transform.sh            # Convert to SQL INSERT format
    └── validate.sh             # Validate PostGIS queries and performance
```

**Structure Decision**: Infrastructure-focused structure for Sprint 0. Three main directories:
1. **docker-compose.yml + .env.example**: Database configuration and credentials
2. **migrations/**: SQL scripts executed on DB startup (001 extensions, 002 schemas, 003 seed data)
3. **osm-import/**: Bash scripts for OSM download → filter → transform → validate

No code directory needed (this is infrastructure automation, not application code).

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

No violations detected. All constitution principles satisfied.

### Assumptions Met

| Assumption | Status | Notes |
|-----------|--------|-------|
| Docker Environment | ✅ | Dev team has Docker & Compose installed |
| OSM Data Availability | ✅ | Geofabrik Tunisia extracts are public |
| Database Credentials | ✅ | `.env` file approach chosen (clarification Q1) |
| Storage Capacity | ✅ | 50–100GB disk space available |
| Network Access | ✅ | Internet access for OSM downloads |
| No Auth in Sprint 0 | ✅ | Deferred to MVP-3 |
| PostGIS Version | ✅ | postgis:16-3.4 image confirmed stable |
| Schema Stability | ✅ | inventory.station schema locked for MVP-1 |
