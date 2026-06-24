# Sprint 01 — Bootstrap platform_db + GIS Ingestion + OSM Tunisia Import + Geospatial Query Function

**Status**: SPEC WRITTEN (Phase 0)
**Date**: 2026-06-24
**Constitution Version**: v1.15.2

---

## Scope Lock (NON-NEGOTIABLE)

This sprint ONLY includes:

| Domain | Included | Excluded |
|--------|----------|----------|
| DB Schema | `platform_db.gis` | Any new schemas |
| Tables | `osm_charging_stations_temp`, `osm_charging_stations` | Any other tables |
| Importer | OSM Tunisia Docker batch job | Runtime dependency on importer |
| Geospatial | `find_nearby_stations` SQL function | API layer, services |
| Services | ❌ None | No `auth-service`, `driver-service`, `admin-service` changes |
| API | ❌ None | No HTTP endpoints |
| Frontend | ❌ None | No apps changes |

---

## Architecture Compliance

### Constitution Rules Enforced

| Rule ID | Rule | Status |
|---------|------|--------|
| §2.1 | Service Count Constraint (3 services) | ✅ No new services |
| §2.4 | Entity ID Standard (PREFIX-nanoid(12)) | ✅ `STA-nanoid(12)` for station_id |
| §4.1 | `platform_db` → `gis` schema | ✅ Owned by `driver-service` (future) |
| §14 | SQLx compile-time validation | ✅ Required in CI |
| §17 | Migration governance | ✅ Forward-only, idempotent |
| §3 | Service topology unchanged | ✅ No service changes |

### Entity ID Compliance

Following §2.4:

| Entity | Prefix | Example |
|--------|--------|---------|
| Charging Station | `STA` | `STA-k9x2lm8q1v7z` |

---

## Database Design

### Schema: `gis`

Owned by: `driver-service` (per §4.1 — ownership assigned, service not yet implemented)

### Table 1 — Staging Layer

```sql
CREATE TABLE gis.osm_charging_stations_temp (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    osm_id TEXT NOT NULL,
    name TEXT,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    tags JSONB DEFAULT '{}',
    imported_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Table 2 — Curated Layer

```sql
CREATE TABLE gis.osm_charging_stations (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_id TEXT NOT NULL UNIQUE,           -- STA-nanoid(12)
    osm_id TEXT UNIQUE,
    name TEXT,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    operator TEXT,
    verified BOOLEAN NOT NULL DEFAULT false,
    is_test BOOLEAN NOT NULL DEFAULT false,    -- KNOWN-001 fix
    deleted_at TIMESTAMPTZ,                    -- KNOWN-002 fix
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Known Bug Fixes Applied

| Bug ID | Issue | Fix |
|--------|-------|-----|
| KNOWN-001 | Test stations leaking | `is_test BOOLEAN DEFAULT false` |
| KNOWN-002 | Missing `deleted_at` | `deleted_at TIMESTAMPTZ` |

---

## OSM Importer (Docker Container)

### Location

```
infra/docker/osm-importer/
├── Dockerfile
├── scripts/
│   ├── import.sh
│   └── transform.sql
└── README.md
```

### Execution Model

```
docker compose up osm-importer
   ↓
parse Tunisia OSM dataset
   ↓
filter EV charging station nodes
   ↓
INSERT → gis.osm_charging_stations_temp
   ↓
TRANSFORM + INSERT → gis.osm_charging_stations
   ↓
exit (batch, no daemon)
```

### Constraints

- No dependency on backend services
- Must be idempotent (ON CONFLICT handling)
- Must be repeatable safely
- Must not modify runtime system

---

## SQL Function: `find_nearby_stations`

### Signature

```sql
CREATE OR REPLACE FUNCTION gis.find_nearby_stations(
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    radius INTEGER DEFAULT 5000,
    limit INTEGER DEFAULT 50
)
RETURNS TABLE(
    station_id TEXT,
    name TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    distance_km DOUBLE PRECISION
)
LANGUAGE sql STABLE
```

### Behavior

| Parameter | Default | Description |
|-----------|---------|-------------|
| `lat` | required | User's latitude |
| `lon` | required | User's longitude |
| `radius` | 5000 | Search radius in meters |
| `limit` | 50 | Max results returned |

### Rules

- MUST NOT return null distances
- MUST enforce `limit` in query
- MUST be deterministic ordering (tie-breaking by station_id)
- MUST operate only on `gis.osm_charging_stations`
- MUST filter WHERE `deleted_at IS NULL` AND `is_test = FALSE`
- Distance computed via Haversine formula (PostGIS if available)

---

## Sprint Output Requirements (§21)

| Artifact | Required |
|----------|----------|
| SYSTEM_STATE.md | ✅ |
| roadmap_status.md | ✅ |
| sprint_state.json | ✅ |
| sprint_review.md | ✅ |
| validation_report.md | ✅ |
| follow_up.md | ✅ |

---

## Directory Structure Created

```
/
├── docs/
│   └── speckit/
│       └── sprints/
│           └── sprint-01/
│               └── spec.md              ← THIS FILE
├── source/
│   └── services/
│       └── driver-service/              ← (future, not this sprint)
├── infra/
│   └── docker/
│       └── osm-importer/                ← (to be created)
└── migrations/
    └── platform_db/
        └── gis/                         ← (to be created)
```
