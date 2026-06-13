# Schema Overview

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🎯 DATABASE PRINCIPLES

- **PostgreSQL 16** is the only database engine
- **PostGIS** is required for geospatial queries
- No cross-service schema ownership
- No direct frontend DB access
- analytics is append-only
- gis is read-only
- users is owned by auth-service

---

## 🧱 DATABASES

### platform_db
- **Purpose:** System of record
- **Engine:** PostgreSQL 16 + PostGIS
- **Role:** Core business data storage

### analytics_db
- **Purpose:** Append-only events
- **Engine:** PostgreSQL 16
- **Role:** Analytics and tracking data
- **Constraint:** NO updates allowed, NO deletes allowed, only INSERT operations

### keycloak_db
- **Purpose:** Identity management
- **Engine:** PostgreSQL 16
- **Role:** Authentication data
- **Constraint:** Fully managed by Keycloak, NEVER accessed directly

---

## 🏗️ PLATFORM_DB (MAIN SYSTEM)

### Schema Structure
```
platform_db/
├── inventory     (admin-service owns)
├── gis           (read-only, OSM-derived)
└── users         (auth-service owns)
```

---

## 📍 INVENTORY SCHEMA (CORE BUSINESS DATA)

**Owner:** admin-service (MVP-2+)
**Read Access:** driver-service (MVP-1)

### station
```sql
CREATE TABLE inventory.station (
    id TEXT PRIMARY KEY,              -- STA-xxx
    name TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL,             -- active | inactive | maintenance
    power_kw INTEGER,
    connector_types TEXT[],
    partner_id TEXT,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
```

### charger
```sql
CREATE TABLE inventory.charger (
    id TEXT PRIMARY KEY,              -- CHR-xxx
    station_id TEXT REFERENCES inventory.station(id),
    type TEXT,                        -- type2, ccs, chademo
    power_kw INTEGER,
    status TEXT,                      -- available | busy | offline
    created_at TIMESTAMP DEFAULT now()
);
```

### indexes (critical for MVP-1)
```sql
CREATE INDEX idx_station_location
ON inventory.station (latitude, longitude);

CREATE INDEX idx_station_status
ON inventory.station (status);
```

---

## 🌍 GIS SCHEMA (READ-ONLY)

**Owner:** System ingestion (MVP-1 import scripts)
**Constraint:** NEVER written by services, NEVER modified by runtime apps

### roads (optional future use)
```sql
CREATE TABLE gis.roads (
    id TEXT PRIMARY KEY,
    name TEXT,
    geom GEOMETRY(LineString, 4326)
);
```

### stations_seed (OSM import layer)
```sql
CREATE TABLE gis.station_seed (
    id TEXT PRIMARY KEY,
    name TEXT,
    geom GEOMETRY(Point, 4326)
);
```

---

## 👤 USERS SCHEMA (AUTH SERVICE ONLY)

**Owner:** auth-service (MVP-3)
**Constraint:** ONLY accessed by auth-service

### user
```sql
CREATE TABLE users.user (
    id TEXT PRIMARY KEY,              -- USR-xxx
    email TEXT UNIQUE,
    role TEXT,                        -- public_driver | registered_driver | admin | partner
    partner_id TEXT,
    created_at TIMESTAMP DEFAULT now()
);
```

---

## 📊 ANALYTICS DB (APPEND-ONLY)

**Owner:** driver-service + admin-service
**Constraint:** NO updates allowed, NO deletes allowed, only INSERT operations

### events table
```sql
CREATE TABLE analytics.events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type TEXT NOT NULL,               -- MapViewed | StationOpened | NearbySearchExecuted
    user_id TEXT,
    station_id TEXT,
    payload JSONB,
    created_at TIMESTAMP DEFAULT now()
);
```

---

## 🧭 GEOSPATIAL MODEL (CRITICAL FOR MVP-1)

**Requirement:** PostGIS for geospatial queries

### Stations MUST support:

- Radius search
- Distance sorting

### Optimized query (driver-service)
```sql
SELECT *,
    (
        6371 * acos(
            cos(radians($1)) *
            cos(radians(latitude)) *
            cos(radians(longitude) - radians($2)) +
            sin(radians($1)) *
            sin(radians(latitude))
        )
    ) AS distance
FROM inventory.station
WHERE status = 'active'
HAVING distance < $3
ORDER BY distance ASC;
```

---

## 🔐 KEYCLOAK DATABASE

**keycloak_db**
- Fully managed by Keycloak
- NEVER accessed directly
- ONLY auth-service communicates with Keycloak

---

## 🔄 DATA OWNERSHIP MATRIX

| Domain | Schema | Owner |
|--------|--------|-------|
| Stations | inventory.station | admin-service |
| Chargers | inventory.charger | admin-service |
| GIS data | gis.* | ingestion scripts |
| Users | users.user | auth-service |
| Events | analytics.events | all services |

---

## 🚫 STRICT RULES

### Absolute Rules
- **Frontend never accesses DB**
- **No cross-schema writes**
- **gis is read-only**
- **analytics is append-only**
- **users only via auth-service**
- **No service owns multiple domains**

---

## 🧠 MVP ALIGNMENT

### MVP-1 Uses ONLY:
- inventory.station
- inventory.charger (read-only optional)
- analytics.events (insert only)

---

## ⚡ PERFORMANCE STRATEGY

### For MVP-1:
- Index latitude/longitude (basic)
- Later upgrade to PostGIS geography type
- Cache nearby queries at service level (future MVP)

---

## 🧠 FINAL PRINCIPLE

**Database is not a storage layer. It is a domain boundary enforcement system.**

---

*This schema documentation defines the data structures and ownership rules that enforce proper system boundaries.*