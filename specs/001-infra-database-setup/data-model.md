# Data Model: Infrastructure & Database Setup

## Database Architecture

```
┌──────────────────────────────┐    ┌──────────────────────────────┐
│        platform_db           │    │        analytics_db          │
│  PostgreSQL 16 + PostGIS     │    │  PostgreSQL 16 (plain)       │
│  Port: 5432                  │    │  Port: 5433                  │
│                              │    │                              │
│  Schemas:                    │    │  Tables:                     │
│  ├── inventory (WRITE)       │    │  └── raw_events (append-only)│
│  ├── gis (READ-ONLY)         │    │                              │
│  └── users (auth scope)      │    │                              │
└──────────────────────────────┘    └──────────────────────────────┘
```

## Inventory Schema (platform_db.inventory)

### partner

Charging network operators.

| Column | Type | Nullable | Constraints |
|--------|------|----------|-------------|
| id | VARCHAR(50) | NO | PK, format: PRT-{nanoid} |
| name | VARCHAR(255) | NO | UNIQUE |
| contact_email | VARCHAR(255) | NO | |
| created_at | TIMESTAMP | NO | DEFAULT NOW() |
| updated_at | TIMESTAMP | NO | DEFAULT NOW() |

**Indexes**: idx_partner_email ON contact_email

### station

Charging station locations.

| Column | Type | Nullable | Constraints |
|--------|------|----------|-------------|
| id | VARCHAR(50) | NO | PK, format: STA-{nanoid} |
| name | VARCHAR(255) | NO | |
| address | VARCHAR(255) | NO | |
| lat | DOUBLE PRECISION | NO | WGS 84, range [-90, 90] |
| lng | DOUBLE PRECISION | NO | WGS 84, range [-180, 180] |
| location | GEOMETRY(Point, 4326) | NO | GENERATED ALWAYS AS ST_Point(lng, lat) STORED |
| status | VARCHAR(20) | NO | DEFAULT 'offline', enum: available/busy/offline/unknown |
| opening_hours | VARCHAR(255) | YES | |
| partner_id | VARCHAR(50) | NO | FK → inventory.partner(id) |
| created_at | TIMESTAMP | NO | DEFAULT NOW() |
| updated_at | TIMESTAMP | NO | DEFAULT NOW() |
| deleted_at | TIMESTAMP | YES | Soft-delete marker |

**Indexes**:
- idx_station_location_gist: GIST(location) WHERE deleted_at IS NULL
- idx_station_partner_id: (partner_id) WHERE deleted_at IS NULL
- idx_station_status: (status) WHERE deleted_at IS NULL

### charger

Individual charging connectors within a station.

| Column | Type | Nullable | Constraints |
|--------|------|----------|-------------|
| id | VARCHAR(50) | NO | PK, format: CHR-{nanoid} |
| station_id | VARCHAR(50) | NO | FK → inventory.station(id) |
| type | VARCHAR(20) | NO | Enum: CCS2/CHAdeMO/Type2/GBT/Type1 |
| power_kw | FLOAT | NO | > 0 |
| status | VARCHAR(20) | NO | DEFAULT 'offline', enum: available/busy/faulted/offline |
| price_per_kwh | FLOAT | NO | >= 0 |
| created_at | TIMESTAMP | NO | DEFAULT NOW() |
| updated_at | TIMESTAMP | NO | DEFAULT NOW() |
| deleted_at | TIMESTAMP | YES | Soft-delete marker |

**Indexes**:
- idx_charger_station_id: (station_id) WHERE deleted_at IS NULL
- idx_charger_type: (type) WHERE deleted_at IS NULL
- idx_charger_status: (status) WHERE deleted_at IS NULL

## GIS Schema (platform_db.gis)

### osm_region

Administrative boundaries (future use).

| Column | Type | Nullable | Constraints |
|--------|------|----------|-------------|
| id | BIGINT | NO | PK |
| name | VARCHAR(255) | YES | |
| admin_level | INTEGER | YES | |
| boundary | GEOMETRY(Polygon, 4326) | YES | |
| created_at | TIMESTAMP | YES | DEFAULT NOW() |

### osm_road

Road network for routing (future use).

| Column | Type | Nullable | Constraints |
|--------|------|----------|-------------|
| id | BIGINT | NO | PK |
| name | VARCHAR(255) | YES | |
| highway_type | VARCHAR(50) | YES | |
| geometry | GEOMETRY(LineString, 4326) | YES | |
| created_at | TIMESTAMP | YES | DEFAULT NOW() |

## Analytics Schema (analytics_db.public)

### raw_events

Immutable clickstream event log.

| Column | Type | Nullable | Constraints |
|--------|------|----------|-------------|
| id | BIGSERIAL | NO | PK, auto-increment |
| event_type | VARCHAR(50) | NO | Enum: station_viewed/station_searched/nearby_searched/... |
| session_id | VARCHAR(50) | NO | |
| user_id | VARCHAR(50) | YES | |
| payload | JSONB | NO | |
| occurred_at | TIMESTAMP | NO | Client UTC timestamp |
| ingested_at | TIMESTAMP | NO | DEFAULT CURRENT_TIMESTAMP |
| client_ip | INET | YES | |

**Rules**:
- No UPDATE — enforced via RULE
- No DELETE — enforced via RULE
- INSERT only

**Indexes**:
- idx_raw_events_event_type: (event_type)
- idx_raw_events_occurred_at: (occurred_at DESC)
- idx_raw_events_session_id: (session_id)
- idx_raw_events_user_id: (user_id) WHERE user_id IS NOT NULL

## Entity Relationships

```
partner (1) ──→ (N) station (1) ──→ (N) charger
                                    raw_events (standalone, no FK)
```

## Data Integrity Rules

1. **Soft delete**: Infrastructure entities (partner, station, charger) use
   `deleted_at` timestamp. Queries filter `WHERE deleted_at IS NULL`.
2. **Hard delete**: Not applied to infrastructure. User data (future) may use
   hard delete.
3. **Timestamps**: All UTC, ISO 8601. `created_at` immutable, `updated_at`
   refreshed on write.
4. **Spatial**: SRID 4326 (WGS 84). `location` auto-generated from lat/lng.
5. **Referential integrity**: Foreign keys enforced. No cascading deletes.
