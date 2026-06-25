# Sprint 04 — Atomic Tasks

**Status**: TASKS DEFINED
**Date**: 2026-06-25

---

## Task Dependency Graph

```
TASK-001 (create ev schema + extensions)
    ↓
TASK-002 (create lookup tables + seed data)
    ↓
TASK-003 (create ev.partners)
    ↓
TASK-004 (create ev.stations + spatial index)
    ↓
TASK-005 (create ev.chargers + constraints)
    ↓
TASK-006 (create GIS → EV migration)
    ↓
TASK-007 (apply migrations + validate SQL)
    ↓
TASK-008 (integration tests)
    ↓
TASK-009 (generate delivery artifacts)
```

---

## TASK-001 — Create `ev` schema + extensions

| Field | Value |
|-------|-------|
| **Input** | `migrations/platform_db/ev/001_create_schema.sql` |
| **Output** | Schema `ev` exists with postgis + hstore + pgcrypto extensions |
| **Module Boundary** | `/migrations/platform_db/ev/` |
| **Validation** | `CREATE SCHEMA IF NOT EXISTS ev;` — idempotent |
| **Test** | Run twice, verify no error |
| **Security** | Schema owner: `admin-service` (future) |

**SQL:**
```sql
CREATE SCHEMA IF NOT EXISTS ev;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS hstore;
```

---

## TASK-002 — Create EV lookup tables + seed data

| Field | Value |
|-------|-------|
| **Input** | `migrations/platform_db/ev/002_lookup_tables.sql` |
| **Output** | 5 lookup tables with seed data |
| **Module Boundary** | `ev` schema |
| **Validation** | `CREATE TABLE IF NOT EXISTS` — idempotent |
| **Test** | Verify all tables exist + seed data inserted |
| **Security** | Read-only reference data |

**Tables:**
- `ev.access_types` — public, restricted, private
- `ev.data_sources` — osm, partner, manual
- `ev.connector_types` — CCS, CHAdeMO, Type 2, Type 1, GB/T, Tesla
- `ev.current_types` — AC, DC
- `ev.connector_statuses` — available, in_use, offline, faulted

---

## TASK-003 — Create `ev.partners`

| Field | Value |
|-------|-------|
| **Input** | `migrations/platform_db/ev/003_create_partners.sql` |
| **Output** | `ev.partners` table with OPR-nanoid(12) identity |
| **Module Boundary** | `ev` schema |
| **Validation** | `CREATE TABLE IF NOT EXISTS` — idempotent |
| **Test** | Verify partner_id format matches regex `^OPR-[a-z0-9]{12}$` |
| **Security** | UUID audit fields, soft-delete |

---

## TASK-004 — Create `ev.stations` + spatial index

| Field | Value |
|-------|-------|
| **Input** | `migrations/platform_db/ev/004_create_stations.sql` |
| **Output** | `ev.stations` table with GEOGRAPHY, HSTORE, GIST index |
| **Module Boundary** | `ev` schema |
| **Validation** | `CREATE TABLE IF NOT EXISTS` — idempotent |
| **Test** | Verify GEOGRAPHY column, GIST index, STA-nanoid format |
| **Security** | FK to partners, soft-delete, no UUID as entity ID |

---

## TASK-005 — Create `ev.chargers` + constraints

| Field | Value |
|-------|-------|
| **Input** | `migrations/platform_db/ev/005_create_chargers.sql` |
| **Output** | `ev.chargers` table with all constraints |
| **Module Boundary** | `ev` schema |
| **Validation** | `CREATE TABLE IF NOT EXISTS` — idempotent |
| **Test** | Verify CHG-nanoid format, count constraints, unique_connector |
| **Security** | FK chain: chargers → stations → partners, soft-delete |

---

## TASK-006 — Create GIS → EV migration

| Field | Value |
|-------|-------|
| **Input** | `migrations/platform_db/ev/006_migrate_gis_to_ev.sql` |
| **Output** | Idempotent migration: `gis.osm_charging_stations` → `ev.stations` |
| **Module Boundary** | Cross-schema (`gis` → `ev`) |
| **Validation** | `ON CONFLICT (osm_id) DO NOTHING` — idempotent |
| **Test** | Run migration twice, verify no duplicate osm_id |
| **Security** | Read-only gis source, only active records migrated |

---

## TASK-007 — Apply migrations + validate SQL

| Field | Value |
|-------|-------|
| **Input** | All 6 migration SQL files |
| **Output** | Migrations applied to running PostgreSQL + PostGIS |
| **Module Boundary** | Platform level |
| **Validation** | Apply in order, verify no errors |
| **Test** | Run all migrations against test database |
| **Security** | Verify no destructive operations |

---

## TASK-008 — Integration tests

| Field | Value |
|-------|-------|
| **Input** | Running PostgreSQL with ev schema |
| **Output** | Verified schema + constraints + migration |
| **Module Boundary** | ev schema |
| **Validation** | All 9 test cases pass |
| **Test Cases** | See plan §3 |

---

## TASK-009 — Generate delivery artifacts

| Field | Value |
|-------|-------|
| **Input** | All completed work |
| **Output** | SYSTEM_STATE.md, sprint_state.json, validation_report.md, sprint_review.md, follow_up.md |
| **Module Boundary** | `/docs/speckit/sprints/sprint-04/` |

---

## Execution Order

```
TASK-001 → TASK-002 → TASK-003 → TASK-004 → TASK-005 → TASK-006 → TASK-007 → TASK-008 → TASK-009
```

## Hard Stop Conditions

- [ ] Any migration SQL syntax error → HALT
- [ ] UUID used as entity ID → HALT (§2.4)
- [ ] Spatial index omitted → HALT
- [ ] Soft-delete omitted → HALT (§19 KNOWN-002)
- [ ] Schema ownership violated → HALT (§4.1)
- [ ] Scope expansion attempted → HALT
