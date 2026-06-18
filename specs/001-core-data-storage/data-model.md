# Data Model: Core Data & Storage Foundations

## Schema Overview

```
platform_db (PostgreSQL 16 + PostGIS 3.4)
├── gis schema              (OSM reference data — imported)
│   ├── osm_roads           (road network geometries)
│   ├── osm_cities          (populated place boundaries)
│   └── osm_points          (points of interest)
└── inventory schema        (application infrastructure — managed)
    ├── partner             (station operator/owner)
    ├── station             (physical charging location)
    └── charger             (individual charging unit)
```

---

## inventory.partner

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | TEXT | PK, prefix `OPR_` | NanoID partner identifier |
| name | TEXT | NOT NULL | Legal or trading name |
| contact_email | TEXT | | Business contact email |
| contact_phone | TEXT | | Business contact phone |
| created_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Record creation timestamp |
| updated_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Last modification timestamp |
| deleted_at | TIMESTAMPTZ | NULLABLE | Soft-delete timestamp |

**Relationships**: A partner has many stations (1:N).

---

## inventory.station

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | TEXT | PK, prefix `STA_` | NanoID station identifier |
| partner_id | TEXT | FK → inventory.partner.id, NULLABLE | Owning partner (NULL for private) |
| name | TEXT | NOT NULL | Station display name |
| address | TEXT | | Street address |
| city | TEXT | | City name |
| latitude | DOUBLE PRECISION | NOT NULL | WGS84 latitude |
| longitude | DOUBLE PRECISION | NOT NULL | WGS84 longitude |
| location | GEOGRAPHY(Point, 4326) | NOT NULL, GENERATED | PostGIS geography point |
| is_private | BOOLEAN | NOT NULL, DEFAULT FALSE | True for home chargers |
| metadata | JSONB | | Custom fields (hours, pricing notes) |
| created_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | |
| updated_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | |
| deleted_at | TIMESTAMPTZ | NULLABLE | Soft-delete timestamp |

**Indexes**:
- GIST index on `location` (geography column)
- BTREE index on `partner_id`
- BTREE index on `city`

**Constraints**:
- latitude: -90 to 90
- longitude: -180 to 180
- `location` is auto-populated from latitude/longitude via trigger or
  generated column: `ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography`

---

## inventory.charger

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | TEXT | PK, prefix `CHG_` | NanoID charger identifier |
| station_id | TEXT | FK → inventory.station.id, NOT NULL | Parent station |
| connector_type | TEXT | NOT NULL | Type (Type2, CCS, CHAdeMO) |
| power_kw | NUMERIC(5,1) | NOT NULL | Rated power output |
| status | TEXT | NOT NULL, DEFAULT 'unknown' | Operational status |
| created_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | |
| updated_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | |
| deleted_at | TIMESTAMPTZ | NULLABLE | Soft-delete timestamp |

**Indexes**:
- BTREE index on `station_id`

---

## gis Schema (Reference Data)

Imported via osm2pgsql with a custom style file selecting:

| Table | Content | Geometry Type |
|-------|---------|---------------|
| `gis.osm_roads` | Major and minor road network | MULTILINESTRING |
| `gis.osm_cities` | Populated place boundaries | MULTIPOLYGON / POINT |
| `gis.osm_points` | Points of interest (amenities, landmarks) | POINT |

All tables include:
- `osm_id` (BIGINT) — OpenStreetMap node/way/relation ID
- `name` (TEXT) — Display name
- `geometry` (GEOMETRY) — Spatial column with GIST index
- Additional tags as columns per osm2pgsql style file

---

## Stored Function: inventory.get_nearby_stations

```sql
FUNCTION inventory.get_nearby_stations(
    lng DOUBLE PRECISION,
    lat DOUBLE PRECISION,
    radius_meters DOUBLE PRECISION
) RETURNS TABLE(
    station_id TEXT,
    station_name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    distance_meters DOUBLE PRECISION,
    is_private BOOLEAN,
    partner_name TEXT
)
```

**Behavior**:
- Converts input coordinates to geography point
- Uses `ST_DWithin` with geography cast for indexed distance filter
- Orders results by `ST_Distance` ascending
- Only returns non-deleted stations (`deleted_at IS NULL`)
- Returns empty set (not NULL) when no stations match
