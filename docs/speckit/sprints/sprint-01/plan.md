# Sprint 01 — Implementation Plan

**Status**: PLANNED
**Date**: 2026-06-24

---

## 1. Architecture Design

### System Context (Sprint 01 scope)

```
┌─────────────────────────┐
│   OSM Tunisia Dataset   │
│   (PBF / GeoJSON)       │
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│   osm-importer (Docker) │  ← infra/docker/osm-importer/ (batch, ephemeral)
│                         │
│   1. Parse PBF          │
│   2. Filter EV stations │
│   3. Write staging      │
│   4. Transform + curate │
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│     platform_db         │
│  ┌───────────────────┐  │
│  │ gis               │  │  ← schema owned by driver-service
│  │ ├─ osm_charging_  │  │
│  │ │  stations_temp  │  │  ← staging
│  │ └─ osm_charging_  │  │
│  │    stations       │  │  ← curated
│  └───────────────────┘  │
│                         │
│  Function:              │
│  find_nearby_stations() │  ← geospatial query
└─────────────────────────┘
```

### Service Impact Map

| Service | Port | Impact Sprint 01 | Impact |
|---------|------|------------------|--------|
| `auth-service` | 3000 | None | No changes |
| `driver-service` | 3001 | Future owner of `gis` schema | Schema exists, service TBD |
| `admin-service` | 3002 | None | No changes |

### Dependency Graph

```
migrations/ (SQL)
    ↓
platform_db.gis schema
    ↓
staging table + curated table
    ↓
osm-importer (Docker) ──→ staging → curated
    ↓
find_nearby_stations() function
```

### External Dependencies

| Dependency | Purpose | Version |
|------------|---------|---------|
| PostgreSQL | Database | 16+ |
| PostGIS (optional) | Geospatial | 3.x |
| osmium / osmosis | OSM PBF parsing | Latest |
| Python psycopg2 | DB connector in importer | 2.9+ |
| nanoid Python lib | Generate STA- IDs | 2.0+ |

---

## 2. DB Schema Changes

### Migration 001 — Initialize `gis` schema

```sql
CREATE SCHEMA IF NOT EXISTS gis;
```

### Migration 002 — Create staging table

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

### Migration 003 — Create curated table

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

### Migration 004 — Create `find_nearby_stations` function

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
AS $$
    SELECT
        s.station_id,
        s.name,
        s.lat,
        s.lon,
        -- Haversine distance in km
        (6371 * acos(
            cos(radians(lat)) * cos(radians(s.lat)) *
            cos(radians(s.lon) - radians(lon)) +
            sin(radians(lat)) * sin(radians(s.lat))
        ))::DOUBLE PRECISION AS distance_km
    FROM gis.osm_charging_stations s
    WHERE s.deleted_at IS NULL
      AND s.is_test = FALSE
      AND (6371 * acos(
            cos(radians(lat)) * cos(radians(s.lat)) *
            cos(radians(s.lon) - radians(lon)) +
            sin(radians(lat)) * sin(radians(s.lat))
          ) * 1000) <= radius
    ORDER BY distance_km ASC, s.station_id ASC
    LIMIT limit;
$$;
```

---

## 3. OSM Importer Design

### Location: `infra/docker/osm-importer/`

#### File: `Dockerfile`

```dockerfile
FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    osmium-tool \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ ./scripts/
ENTRYPOINT ["bash", "scripts/import.sh"]
```

#### File: `requirements.txt`

```
psycopg2-binary>=2.9
nanoid>=2.0
```

#### File: `scripts/import.sh`

```bash
#!/bin/bash
set -euo pipefail

# Download OSM Tunisia dataset if not present
if [ ! -f /data/tunisia.osm.pbf ]; then
    echo "Downloading Tunisia OSM dataset..."
    wget -O /data/tunisia.osm.pbf \
        "https://download.geofabrik.de/africa/tunisia-latest.osm.pbf"
fi

# Parse and import
python3 scripts/parse_and_import.py \
    --pbf /data/tunisia.osm.pbf \
    --db-host "${DB_HOST}" \
    --db-port "${DB_PORT}" \
    --db-name "${DB_NAME}" \
    --db-user "${DB_USER}" \
    --db-password "${DB_PASSWORD}"
```

#### File: `scripts/parse_and_import.py`

```python
"""
OSM PBF parser for EV charging stations.
Extracts nodes with charging=yes or amenity=charging_station tags.
"""
import argparse
import json
import os
import subprocess
import psycopg2
from nanoid import generate

OSM_TAGS_FILTER = {
    "amenity": "charging_station",
}

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pbf", required=True)
    parser.add_argument("--db-host", default="localhost")
    parser.add_argument("--db-port", default="5432")
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-password", required=True)
    return parser.parse_args()

def extract_ev_stations(pbf_path):
    """Use osmium to extract charging station nodes as JSON."""
    result = subprocess.run(
        [
            "osmium", "export",
            "--output-format", "geojson",
            "--overwrite",
            "-o", "/tmp/ev_stations.geojson",
            pbf_path,
        ],
        capture_output=True, text=True
    )
    # Fallback: use osmium tags filter approach
    result = subprocess.run(
        [
            "osmium", "tags-filter",
            pbf_path,
            "amenity=charging_station",
            "charging=yes",
            "-o", "/tmp/ev_filtered.osm",
            "--overwrite",
        ],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"Warning: osmium filter issue: {result.stderr}")
        return []
    
    # Export filtered to GeoJSON
    subprocess.run([
        "osmium", "export",
        "--output-format", "geojson",
        "--overwrite",
        "-o", "/tmp/ev_stations.geojson",
        "/tmp/ev_filtered.osm",
    ], check=True)
    
    with open("/tmp/ev_stations.geojson") as f:
        geojson = json.load(f)
    
    stations = []
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        coords = feature.get("geometry", {}).get("coordinates", [None, None])
        
        stations.append({
            "osm_id": str(props.get("id", "")),
            "name": props.get("name", ""),
            "lat": coords[1],
            "lon": coords[0],
            "tags": props,
            "operator": props.get("operator"),
        })
    
    return stations

def insert_staging(conn, stations):
    with conn.cursor() as cur:
        for s in stations:
            cur.execute(
                """
                INSERT INTO gis.osm_charging_stations_temp
                    (osm_id, name, lat, lon, tags)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (s["osm_id"], s["name"], s["lat"], s["lon"],
                 json.dumps(s["tags"]))
            )
    conn.commit()

def transform_to_curated(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gis.osm_charging_stations
                (station_id, osm_id, name, lat, lon, operator, verified)
            SELECT
                'STA-' || nid,
                osm_id,
                NULLIF(TRIM(name), ''),
                lat,
                lon,
                NULLIF(TRIM(operator), ''),
                false
            FROM gis.osm_charging_stations_temp t
            CROSS JOIN LATERAL (
                SELECT substring(
                    'abcdefghijklmnopqrstuvwxyz0123456789',
                    (floor(random() * 36)::int + 1), 1
                ) || substring(
                    'abcdefghijklmnopqrstuvwxyz0123456789',
                    (floor(random() * 36)::int + 1), 1
                ) || substring(
                    'abcdefghijklmnopqrstuvwxyz0123456789',
                    (floor(random() * 36)::int + 1), 1
                ) || substring(
                    'abcdefghijklmnopqrstuvwxyz0123456789',
                    (floor(random() * 36)::int + 1), 1
                ) || substring(
                    'abcdefghijklmnopqrstuvwxyz0123456789',
                    (floor(random() * 36)::int + 1), 1
                ) || substring(
                    'abcdefghijklmnopqrstuvwxyz0123456789',
                    (floor(random() * 36)::int + 1), 1
                ) || substring(
                    'abcdefghijklmnopqrstuvwxyz0123456789',
                    (floor(random() * 36)::int + 1), 1
                ) || substring(
                    'abcdefghijklmnopqrstuvwxyz0123456789',
                    (floor(random() * 36)::int + 1), 1
                ) || substring(
                    'abcdefghijklmnopqrstuvwxyz0123456789',
                    (floor(random() * 36)::int + 1), 1
                ) || substring(
                    'abcdefghijklmnopqrstuvwxyz0123456789',
                    (floor(random() * 36)::int + 1), 1
                ) || substring(
                    'abcdefghijklmnopqrstuvwxyz0123456789',
                    (floor(random() * 36)::int + 1), 1
                ) || substring(
                    'abcdefghijklmnopqrstuvwxyz0123456789',
                    (floor(random() * 36)::int + 1), 1
                ) AS nid
            ) AS n
            WHERE NOT EXISTS (
                SELECT 1 FROM gis.osm_charging_stations
                WHERE osm_id = t.osm_id
            )
            ON CONFLICT (osm_id) DO NOTHING
            """
        )
    conn.commit()

def main():
    args = parse_args()
    
    conn = psycopg2.connect(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )
    
    print("Extracting EV charging stations from OSM...")
    stations = extract_ev_stations(args.pbf)
    print(f"Found {len(stations)} charging stations")
    
    print("Inserting into staging table...")
    insert_staging(conn, stations)
    
    print("Transforming to curated table...")
    transform_to_curated(conn)
    
    conn.close()
    print("Import complete.")

if __name__ == "__main__":
    main()
```

---

## 4. Directory Structure

```
/home/dali/WORK/BorneMap/
├── docs/
│   └── speckit/
│       └── sprints/
│           └── sprint-01/
│               ├── spec.md
│               └── plan.md            ← THIS FILE
├── infra/
│   └── docker/
│       └── osm-importer/
│           ├── Dockerfile
│           ├── requirements.txt
│           ├── docker-compose.yml (in parent)
│           └── scripts/
│               ├── import.sh
│               └── parse_and_import.py
├── source/
│   └── services/
│       └── driver-service/
├── migrations/
│   └── platform_db/
│       └── gis/
│           ├── 001_create_schema.sql
│           ├── 002_create_staging_table.sql
│           ├── 003_create_curated_table.sql
│           └── 004_find_nearby_stations.sql
└── docker-compose.yml
```

---

## 5. Testing Strategy

| Test Type | Scope | Tool |
|-----------|-------|------|
| SQL correctness | Migration SQL files | Manual review |
| Function correctness | `find_nearby_stations` | SQL test queries |
| Idempotency | Migration re-runs | EXECUTE twice, verify no errors |
| Docker execution | OSM importer container | `docker compose up` + verify DB rows |
| Edge cases | Empty result, null params | SQL assertions |

### Test Cases for `find_nearby_stations`

1. **Basic query** — Returns stations within 5km of Tunis center
2. **Custom radius** — 1000m returns fewer results
3. **Limit enforcement** — `LIMIT 10` returns ≤10 rows
4. **No results** — Remote coordinates return empty set
5. **Deterministic** — Same input = same output ordering
6. **Null handling** — `deleted_at` and `is_test` filters applied
7. **Distance accuracy** — Verified against known coordinates

---

## 6. Risk Assessment

| Risk | Mitigation |
|------|------------|
| OSM PBF download fails | Use GeoJSON fallback; add retry logic |
| Large PBF file processing | osmium is efficient; test with subset first |
| nanoid collision | Probability negligible with 12 chars |
| Migration conflict on re-run | All SQL uses IF NOT EXISTS / OR REPLACE |
| Docker network to Postgres | Use docker-compose network linkage |

---

## 7. Approval Gate

Before implementation proceeds:

- [x] Spec written to `/docs/speckit/sprints/sprint-01/spec.md`
- [x] Plan written to `/docs/speckit/sprints/sprint-01/plan.md`
- [ ] Constitution check passed
- [ ] Scope lock confirmed
- [ ] Architecture impact mapped
