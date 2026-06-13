# Inventory Schema

## Version: 1.0
## Status: Active
## Owner: admin-service (MVP-2+)
## Read Access: driver-service (MVP-1)

---

## 📍 OVERVIEW

The inventory schema contains core business data for stations and chargers. This is the system of record for operational EV charging infrastructure.

---

## 🧱 TABLES

### station

**Description:** Physical station locations with operational details

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

**Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | TEXT | Yes | Unique station identifier (STA-xxx) |
| name | TEXT | Yes | Station name |
| latitude | DOUBLE PRECISION | Yes | Station latitude (for MVP-1) |
| longitude | DOUBLE PRECISION | Yes | Station longitude (for MVP-1) |
| status | TEXT | Yes | Current status (active, inactive, maintenance) |
| power_kw | INTEGER | No | Maximum power capacity in kW |
| connector_types | TEXT[] | No | Supported connector types |
| partner_id | TEXT | No | Associated partner organization |
| created_at | TIMESTAMP | Yes | Record creation time |
| updated_at | TIMESTAMP | Yes | Last update time |

**Constraints:**
- latitude and longitude required for MVP-1
- status is required and must be one of: active, inactive, maintenance
- partner_id is optional for MVP-1

---

### charger

**Description:** Individual chargers within stations

```sql
CREATE TABLE inventory.charger (
    id TEXT PRIMARY KEY,              -- CHR-xxx
    station_id TEXT REFERENCES inventory.station(id) ON DELETE CASCADE,
    type TEXT,                        -- type2, ccs, chademo
    power_kw INTEGER,
    status TEXT,                      -- available | busy | offline
    created_at TIMESTAMP DEFAULT now()
);
```

**Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | TEXT | Yes | Unique charger identifier (CHR-xxx) |
| station_id | TEXT | Yes | Reference to parent station |
| type | TEXT | Yes | Charger type (type2, ccs, chademo) |
| power_kw | INTEGER | Yes | Charger power in kW |
| status | TEXT | Yes | Current status (available, busy, offline) |
| created_at | TIMESTAMP | Yes | Record creation time |

**Constraints:**
- station_id is required and must exist
- type must be one of: type2, ccs, chademo
- status must be one of: available, busy, offline

---

## 🔍 INDEXES

### Critical Indexes for MVP-1

```sql
-- Station location indexing for nearby search
CREATE INDEX idx_station_location
ON inventory.station (latitude, longitude);

-- Station status filtering
CREATE INDEX idx_station_status
ON inventory.station (status);
```

---

## 🧭 GEOSPATIAL QUERIES

### Distance-Based Queries

For MVP-1, use mathematical distance formula:

```sql
-- Query stations within radius
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

**Parameters:**
- $1: target latitude
- $2: target longitude
- $3: search radius in km

---

## 📊 QUERY PATTERNS

### Get all active stations

```sql
SELECT * FROM inventory.station WHERE status = 'active';
```

### Get station by ID

```sql
SELECT * FROM inventory.station WHERE id = $1;
```

### Get chargers for station

```sql
SELECT * FROM inventory.charger WHERE station_id = $1;
```

---

## 🚦 CONSTRAINTS

### MVP-1 Constraints

- **driver-service can READ:** All station fields
- **admin-service can READ/WRITE:** All station and charger fields
- **No DELETE operations:** For MVP-1, only UPDATE allowed
- **Status-based filtering:** Station status is a critical filter

---

## 🔄 MIGRATION RULES

### Status Transitions

```
active → inactive:  Disabled station
active → maintenance: Scheduled maintenance
maintenance → active: Station operational
inactive → active: Reactivated station
```

### No Updates for MVP-1

- Basic station information should be immutable for MVP-1
- Only operational status changes allowed
- GPS coordinates remain unchanged

---

## 🧠 MVP ALIGNMENT

**MVP-1 uses ONLY:**
- inventory.station table
- Inventory station queries for nearby search
- Basic status filtering

**MVP-2+ adds:**
- Complete CRUD operations
- Partner relationships
- Advanced operational features
- Real-time status updates

---

## 🎯 OWNERSHIP RULES

| Table | Owner | Access | Write Allowed |
|-------|-------|--------|---------------|
| inventory.station | admin-service | driver-service (read), admin-service (read/write) | admin-service only |
| inventory.charger | admin-service | admin-service (read/write) | admin-service only |

---

*This schema defines the core business data for EV charging infrastructure.*