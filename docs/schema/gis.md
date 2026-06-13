# GIS Schema

## Version: 1.0
## Status: Active
## Owner: System ingestion scripts
## Constraint: READ-ONLY, NEVER modified by services

---

## 🌍 OVERVIEW

The GIS schema contains geographic data and reference information. This schema is **strictly read-only** and serves as a foundation for future geospatial features.

---

## 🚫 CRITICAL CONSTRAINTS

**ABSOLUTE RULES:**

- **NEVER written by services**
- **NEVER modified by runtime apps**
- **ONLY written by import scripts**
- **NO deletions allowed**
- **NO updates allowed**

This schema is a **reference layer**, not an operational layer.

---

## 📋 TABLES

### roads (optional future use)

**Description:** Road network data for navigation and routing

```sql
CREATE TABLE gis.roads (
    id TEXT PRIMARY KEY,
    name TEXT,
    geom GEOMETRY(LineString, 4326)
);
```

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| id | TEXT | Unique road identifier |
| name | TEXT | Road name |
| geom | GEOMETRY | Geographic geometry (LineString in WGS84) |

**Status:** Optional - for future navigation features

---

### stations_seed (OSM import layer)

**Description:** Station locations imported from OpenStreetMap

```sql
CREATE TABLE gis.station_seed (
    id TEXT PRIMARY KEY,
    name TEXT,
    geom GEOMETRY(Point, 4326)
);
```

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| id | TEXT | Unique OSM station identifier |
| name | TEXT | Station name from OSM |
| geom | GEOMETRY | Geographic geometry (Point in WGS84) |

**Purpose:**
- Source data for station initialization
- Reference for position verification
- Future data enrichment

**Status:** Active for MVP-1 data import

---

## 🎯 USAGE PATTERNS

### Reference Queries

```sql
-- Get station reference data by ID
SELECT * FROM gis.station_seed WHERE id = $1;
```

### Import Data Access

```sql
-- Read-only access for import scripts
SELECT * FROM gis.station_seed;
SELECT * FROM gis.roads;
```

---

## 🚫 OPERATIONS NOT ALLOWED

### No Service Writes

```sql
-- ❌ WRONG - Never do this
INSERT INTO gis.station_seed (id, name, geom) VALUES (...);
UPDATE gis.station_seed SET name = 'New Name' WHERE id = '...';
DELETE FROM gis.station_seed WHERE id = '...';
```

### No Runtime Modifications

```sql
-- ❌ WRONG - Never do this in application code
INSERT INTO gis.roads (id, name, geom) VALUES (...);
UPDATE gis.roads SET name = 'Updated Road' WHERE id = '...';
```

---

## 🔄 MAINTENANCE

### Import Scripts Only

All data updates must be done through:
1. ETL/ELT import scripts
2. Scheduled data refreshes
3. Manual data imports
4. OSM data updates

### Data Quality

- Import scripts must validate data
- Geographic coordinates must be in WGS84 (EPSG:4326)
- Must handle duplicate detection
- Must validate geometry validity

---

## 🧭 GEOGRAPHIC COORDINATES

### Coordinate System

- **CRS:** WGS84 (EPSG:4326)
- **Units:** Degrees (latitude/longitude)
- **Projection:** Geographic (not projected)

### Coordinates Format

```sql
-- Point geometry
ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
```

### Distance Calculations

For MVP-1, use mathematical formulas:
```sql
-- Distance in kilometers
6371 * acos(
    cos(radians(lat1)) * cos(radians(lat2)) *
    cos(radians(lon2) - radians(lon1)) +
    sin(radians(lat1)) * sin(radians(lat2))
)
```

---

## 📊 DATA RELATIONSHIPS

### station vs station_seed

```
gis.station_seed (import source)
    ↓ (import process)
inventory.station (operational data)
    ↓ (updates)
platform_db.system of record
```

**Import Flow:**
1. Load from gis.station_seed
2. Validate data
3. Create in inventory.station
4. Archive or delete from gis.station_seed

---

## 🎯 MVP-1 USAGE

### Data Import (Run Once)

```sql
-- Import script for MVP-1
INSERT INTO inventory.station (id, name, latitude, longitude, status, created_at)
SELECT
    id,
    name,
    ST_X(geom) as longitude,
    ST_Y(geom) as latitude,
    'active' as status,
    now() as created_at
FROM gis.station_seed
WHERE geom IS NOT NULL;
```

### Reference Queries

```sql
-- Station reference data for validation
SELECT id, name FROM gis.station_seed LIMIT 10;

-- Check import completeness
SELECT
    (SELECT COUNT(*) FROM gis.station_seed) as total_source,
    (SELECT COUNT(*) FROM inventory.station) as imported_count;
```

---

## 🚧 FUTURE EXTENSIONS

### Potential Additions

- **poi_features:** Points of interest
- **geofences:** Service areas and boundaries
- **routes:** Navigation routes
- **zones:** Geographic zones (urban, rural, etc.)

### Extension Rules

- All extensions must maintain read-only constraint
- Must support import/update via scripts
- No runtime modifications allowed

---

## 🎯 OWNERSHIP RULES

| Table | Owner | Access | Modification Allowed |
|-------|-------|--------|---------------------|
| gis.roads | System ingestion | Read-only | No (via import scripts) |
| gis.station_seed | System ingestion | Read-only | No (via import scripts) |

---

## 🧠 MVP ALIGNMENT

**MVP-1 uses ONLY:**
- gis.station_seed for data import
- Reference queries for validation
- NO runtime modifications

**MVP-2+:**
- Potentially use gis.roads for navigation
- Geofence features
- Advanced routing

---

*This schema provides reference geographic data that supports but does not modify the operational data layer.*