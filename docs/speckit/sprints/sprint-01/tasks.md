# Sprint 01 — Atomic Tasks

**Status**: TASKS DEFINED
**Date**: 2026-06-24

---

## Task Dependency Graph

```
TASK-001 (create gis schema)
    ↓
TASK-002 (create staging table)
    ↓
TASK-003 (create curated table)
    ↓
TASK-004 (build OSM importer Dockerfile + dependencies)
    ↓
TASK-005 (implement OSM parsing + ETL pipeline)
    ↓
TASK-006 (implement staging → curated transformation)
    ↓
TASK-007 (implement find_nearby_stations SQL function)
    ↓
TASK-008 (add default params + ordering + limit enforcement)
    ↓
TASK-009 (validate SQLx migrations)
    ↓
TASK-010 (integration test: Docker + SQL function)
```

---

## TASK-001 — Create `gis` schema

| Field | Value |
|-------|-------|
| **Input** | `migrations/platform_db/gis/001_create_schema.sql` |
| **Output** | Schema `gis` exists in `platform_db` |
| **Module Boundary** | `/migrations/platform_db/gis/` |
| **Validation** | `CREATE SCHEMA IF NOT EXISTS gis;` — idempotent |
| **Test** | Run twice, verify no error |
| **Security** | Schema owner: `driver-service` (future) |

**SQL:**
```sql
CREATE SCHEMA IF NOT EXISTS gis;
```

---

## TASK-002 — Create staging table `gis.osm_charging_stations_temp`

| Field | Value |
|-------|-------|
| **Input** | `migrations/platform_db/gis/002_create_staging_table.sql` |
| **Output** | Staging table with OSM raw data |
| **Module Boundary** | `gis` schema |
| **Validation** | `CREATE TABLE IF NOT EXISTS` — idempotent |
| **Test** | `\dt gis.osm_charging_stations_temp` returns table |
| **Security** | No PII stored; raw OSM data only |

**SQL:**
```sql
CREATE TABLE IF NOT EXISTS gis.osm_charging_stations_temp (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    osm_id TEXT NOT NULL,
    name TEXT,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    tags JSONB DEFAULT '{}',
    imported_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_osm_cst_osm_id
    ON gis.osm_charging_stations_temp (osm_id);
```

---

## TASK-003 — Create curated table `gis.osm_charging_stations`

| Field | Value |
|-------|-------|
| **Input** | `migrations/platform_db/gis/003_create_curated_table.sql` |
| **Output** | Curated table with STA-nanoid PK, KNOWN bug fixes |
| **Module Boundary** | `gis` schema |
| **Validation** | `CREATE TABLE IF NOT EXISTS` — idempotent |
| **Test** | Verify `station_id` format matches regex `^STA-[a-z0-9]{12}$` |
| **Security** | `is_test` flag prevents test data leaking (KNOWN-001) |

**SQL:**
```sql
CREATE TABLE IF NOT EXISTS gis.osm_charging_stations (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_id TEXT NOT NULL UNIQUE,
    osm_id TEXT UNIQUE,
    name TEXT,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    operator TEXT,
    verified BOOLEAN NOT NULL DEFAULT false,
    is_test BOOLEAN NOT NULL DEFAULT false,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_osm_cs_location
    ON gis.osm_charging_stations (lat, lon);

CREATE INDEX IF NOT EXISTS idx_osm_cs_active
    ON gis.osm_charging_stations (deleted_at)
    WHERE deleted_at IS NULL AND is_test = FALSE;
```

---

## TASK-004 — Build OSM importer container (Docker)

| Field | Value |
|-------|-------|
| **Input** | `infra/docker/osm-importer/Dockerfile`, `requirements.txt` |
| **Output** | Docker image that can parse OSM PBF |
| **Module Boundary** | `/infra/docker/osm-importer/` |
| **Validation** | `docker build -t osm-importer .` succeeds |
| **Test** | Container starts and errors with missing DB (expected) |
| **Security** | Ephemeral batch container; no exposed ports |

**Files:**
- `infra/docker/osm-importer/Dockerfile`
- `infra/docker/osm-importer/requirements.txt`
- `infra/docker/osm-importer/scripts/import.sh`

---

## TASK-005 — Implement OSM parsing + ETL pipeline

| Field | Value |
|-------|-------|
| **Input** | Tunisia OSM PBF file, connector config |
| **Output** | `gis.osm_charging_stations_temp` populated |
| **Module Boundary** | `/infra/docker/osm-importer/scripts/parse_and_import.py` |
| **Validation** | `osmium` tags-filter + GeoJSON export |
| **Test** | Rows exist in staging table after execution |
| **Security** | No runtime system modification; DB credentials via env vars |

---

## TASK-006 — Implement staging → curated transformation

| Field | Value |
|-------|-------|
| **Input** | `gis.osm_charging_stations_temp` rows |
| **Output** | `gis.osm_charging_stations` populated with `STA-nanoid(12)` |
| **Module Boundary** | Same script as TASK-005 |
| **Validation** | Deduplicate by `osm_id`; generate valid nanoid |
| **Test** | Verify `station_id` format; verify no duplicate `osm_id` |
| **Security** | Idempotent: `ON CONFLICT (osm_id) DO NOTHING` |

---

## TASK-007 — Implement `find_nearby_stations` SQL function

| Field | Value |
|-------|-------|
| **Input** | `migrations/platform_db/gis/004_find_nearby_stations.sql` |
| **Output** | `gis.find_nearby_stations()` function |
| **Module Boundary** | `gis` schema |
| **Validation** | Haversine distance calculation; `STABLE` volatility |
| **Test** | Call with known coordinates, verify distance accuracy |
| **Security** | Function is `STABLE` (read-only); no data modification |

---

## TASK-008 — Add default parameters + ordering + limit enforcement

| Field | Value |
|-------|-------|
| **Input** | Function definition with defaults (same file as TASK-007) |
| **Output** | Function with `radius DEFAULT 5000`, `limit DEFAULT 50` |
| **Module Boundary** | Same as TASK-007 |
| **Validation** | Default params work; `ORDER BY distance_km ASC, station_id ASC` for determinism |
| **Test** | Call without radius/limit params, verify defaults applied |
| **Security** | `LIMIT` prevents unbounded result sets |

---

## TASK-009 — Validate SQLx migrations

| Field | Value |
|-------|-------|
| **Input** | All migration SQL files |
| **Output** | SQLx compile-time validation passes |
| **Module Boundary** | Workspace level |
| **Validation** | `cargo sqlx prepare --check` succeeds |
| **Test** | CI pipeline validation |
| **Security** | Hard stop if SQLx fails (Constitution §14) |

---

## TASK-010 — Integration test (Docker + SQL function)

| Field | Value |
|-------|-------|
| **Input** | Running PostgreSQL + osm-importer container |
| **Output** | Verified end-to-end pipeline |
| **Module Boundary** | Cross-module integration |
| **Validation** | Full pipeline: OSM file → staging → curated → query |
| **Test Cases** | 7 test cases for `find_nearby_stations` (see plan §5) |

---

## Execution Order

```
TASK-001 → TASK-002 → TASK-003 → TASK-004
                                  ↓
                    TASK-005 → TASK-006
                                        ↓
                              TASK-007 → TASK-008
                                              ↓
                                        TASK-009
                                              ↓
                                        TASK-010
```

## Hard Stop Conditions

- [ ] Any migration SQL syntax error → HALT
- [ ] SQLx prepare fails → HALT (§14)
- [ ] Scope expansion attempted → HALT
- [ ] Architecture boundary violated → HALT
- [ ] Docker build fails → HALT
