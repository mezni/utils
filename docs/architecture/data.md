# Data Architecture

BorneMap uses a single PostgreSQL 16 + PostGIS database with four strictly-separated schemas. Each schema has exclusive data ownership and defined write permissions. This document specifies the schema design, cross-schema rules, and synchronization mechanisms.

---

## Database Overview

**Technology:** PostgreSQL 16 with PostGIS extension
**Replication:** None (single instance)
**Backups:** Daily automated to host
**Connection Pooling:** Service-managed via sqlx

Single database instance with four schemas provides:
- Clear data ownership boundaries
- Atomic transactions within schema
- Trigger-based synchronization
- Simple operational model (one database to manage)

---

## Schema Specification

### 1. inventory Schema

**Owner:** Admin Service  
**Responsibility:** Business entities (partners, stations, chargers, availability)  
**Written By:** Admin Service only  
**Read By:** Admin Service, Driver Service, GIS trigger function

#### Tables

**partner**
```
partner_id          TEXT PRIMARY KEY (PRT-...)
name               TEXT NOT NULL
contact_email      TEXT NOT NULL
phone              TEXT
address            TEXT
city               TEXT
created_at         TIMESTAMP DEFAULT NOW()
updated_at         TIMESTAMP DEFAULT NOW()
deleted_at         TIMESTAMP (soft delete)
```

**station**
```
station_id          TEXT PRIMARY KEY (STN-...)
partner_id          TEXT NOT NULL FK→partner
name               TEXT NOT NULL
description        TEXT
latitude           DECIMAL(10,8) NOT NULL
longitude          DECIMAL(11,8) NOT NULL
address            TEXT NOT NULL
city               TEXT NOT NULL
governorate        TEXT NOT NULL
postal_code        TEXT
amenities          TEXT[] (parking, wifi, cafe, etc.)
photos             TEXT[] (URLs)
rating             DECIMAL(3,2)
review_count       INTEGER DEFAULT 0
created_at         TIMESTAMP DEFAULT NOW()
updated_at         TIMESTAMP DEFAULT NOW()
deleted_at         TIMESTAMP (soft delete)

UNIQUE(partner_id, name)  -- Partner can't have duplicate station names
```

**charger**
```
charger_id          TEXT PRIMARY KEY (CHG-...)
station_id          TEXT NOT NULL FK→station
connector_type      TEXT NOT NULL (Type1, Type2, CCS, CHAdeMO, Tesla)
power_output_kw     DECIMAL(5,2)
availability_status TEXT NOT NULL (Available, InUse, Maintenance, Offline)
last_updated_at     TIMESTAMP
created_at          TIMESTAMP DEFAULT NOW()
updated_at          TIMESTAMP DEFAULT NOW()
deleted_at          TIMESTAMP (soft delete)
```

**station_availability** (for manual updates)
```
availability_id     TEXT PRIMARY KEY
station_id          TEXT NOT NULL FK→station
charger_id          TEXT FK→charger (NULL = whole station)
available_count     INTEGER
in_use_count        INTEGER
maintenance_count   INTEGER
updated_at          TIMESTAMP
updated_by_user_id  TEXT
```

#### Indexes
- **Primary Keys** on all ID columns
- **Foreign Keys** on partner_id, station_id, charger_id
- **GiST Index** on station(lat/long) for nearby searches — handled by gis schema trigger
- **Partial Indexes** on deleted_at IS NULL for soft deletes

#### Constraints
- Partner names are unique
- Stations belong to exactly one partner
- Chargers belong to exactly one station
- Availability status is enum-like (validated in app)

---

### 2. users Schema

**Owner:** Driver Service  
**Responsibility:** User profiles, authentication-derived data, user-generated content  
**Written By:** Driver Service only  
**Read By:** Driver Service, Admin Service (reporting and moderation)

#### Tables

**user_account**
```
user_account_id     TEXT PRIMARY KEY (USR-...)
keycloak_sub        TEXT UNIQUE NOT NULL (from JWT)
email               TEXT UNIQUE NOT NULL
first_name          TEXT
last_name           TEXT
profile_completed   BOOLEAN DEFAULT FALSE
created_at          TIMESTAMP DEFAULT NOW()
updated_at          TIMESTAMP DEFAULT NOW()
deleted_at          TIMESTAMP (soft delete)
last_login_at       TIMESTAMP
```

**user_profile** (optional extended profile)
```
user_id             TEXT PRIMARY KEY FK→user_account
avatar_url          TEXT
phone               TEXT
address             TEXT
preferences         JSONB (language, notifications, etc.)
updated_at          TIMESTAMP
```

**partner_membership** (enforces one-partner-per-user)
```
user_id             TEXT PRIMARY KEY FK→user_account
partner_id          TEXT NOT NULL FK(inventory.partner)
role                TEXT (partner_admin, partner_user)
joined_at           TIMESTAMP DEFAULT NOW()

-- Primary key on user_id enforces one row per user = one partner per user
```

**favorite_station**
```
favorite_id         TEXT PRIMARY KEY
user_id             TEXT NOT NULL FK→user_account
station_id          TEXT NOT NULL FK(inventory.station)
saved_at            TIMESTAMP DEFAULT NOW()

UNIQUE(user_id, station_id)  -- User can't favorite same station twice
```

**station_review**
```
review_id           TEXT PRIMARY KEY (REV-...)
user_id             TEXT NOT NULL FK→user_account
station_id          TEXT NOT NULL FK(inventory.station)
rating              INTEGER (1-5)
comment             TEXT
moderation_status   TEXT (pending, approved, rejected)
created_at          TIMESTAMP DEFAULT NOW()
updated_at          TIMESTAMP DEFAULT NOW()
deleted_at          TIMESTAMP (soft delete)
```

#### Indexes
- **Primary Keys** on all ID columns
- **Foreign Keys** on user_id, station_id, partner_id
- **Unique Indexes** on keycloak_sub, email
- **Unique Indexes** on (user_id, station_id) for favorites and reviews
- **Composite Index** on (station_id, moderation_status) for reporting

#### Constraints
- One user_account per keycloak_sub
- One user_account per email
- One partner_membership per user_id (enforces one partner per user)
- Rating is 1-5
- Moderation status is enum: pending, approved, rejected

#### Critical Rule
**Public drivers are not stored in users schema.** Only users who have completed first-login provisioning (authenticated and called a Driver Service endpoint) have a record in user_account.

---

### 3. gis Schema

**Owner:** Trigger function (automatic sync)  
**Responsibility:** Spatial enrichment, OpenStreetMap data, derived location data  
**Written By:** PostgreSQL trigger function only  
**Read By:** Driver Service (spatial queries)

#### Tables

**osm_nodes** (OpenStreetMap)
```
osm_id              BIGINT PRIMARY KEY
latitude            DECIMAL(10,8)
longitude           DECIMAL(11,8)
tags                JSONB
imported_at         TIMESTAMP
```

**osm_ways** (OpenStreetMap)
```
osm_id              BIGINT PRIMARY KEY
name                TEXT
tags                JSONB
geom                geometry(LineString, 4326)
imported_at         TIMESTAMP
```

**roads**
```
road_id             BIGINT PRIMARY KEY
osm_way_id          BIGINT FK→osm_ways
name                TEXT
road_type           TEXT (primary, secondary, residential, etc.)
geom                geometry(LineString, 4326)
imported_at         TIMESTAMP

INDEX (geom USING GIST)  -- CRITICAL: Required for spatial queries
```

**boundaries**
```
boundary_id         BIGINT PRIMARY KEY
osm_id              BIGINT
name                TEXT
governorate         TEXT
district            TEXT
boundary_type       TEXT
geom                geometry(Polygon, 4326)
imported_at         TIMESTAMP

INDEX (geom USING GIST)  -- CRITICAL: Required for spatial queries
```

**amenity_points** (Parking, wifi, cafes, etc.)
```
amenity_id          BIGINT PRIMARY KEY
osm_id              BIGINT
name                TEXT
amenity_type        TEXT (parking, cafe, wifi, hospital, etc.)
geom                geometry(Point, 4326)
imported_at         TIMESTAMP
```

**station_locations** (Derived from inventory.station)
```
location_id         TEXT PRIMARY KEY
station_id          TEXT NOT NULL UNIQUE FK(inventory.station)
geom                geometry(Point, 4326)
nearest_road_id     BIGINT FK→roads
nearest_road_name   TEXT
boundary_id         BIGINT FK→boundaries
governorate         TEXT
district            TEXT
nearby_amenities    JSONB (array of amenity types nearby)
created_at          TIMESTAMP
updated_at          TIMESTAMP
```

#### Trigger Logic

**Trigger:** `on_station_insert_update_delete` fires after INSERT, UPDATE, DELETE on `inventory.station`

**Behavior on INSERT or UPDATE:**
1. Compute geometry point from station lat/lng
2. Find nearest road via spatial index (ST_DWithin + ST_Distance)
3. Find containing boundary via ST_Contains
4. Collect nearby amenities via ST_DWithin
5. UPSERT into station_locations with all computed values

**Behavior on DELETE:**
1. Delete corresponding station_locations row

**Error Handling:**
- Trigger has exception handler
- Log failures to `gis_sync_log` table
- **Do NOT block the transaction** — station write succeeds even if sync fails

#### Spatial Indexes (CRITICAL)
```sql
-- These MUST exist before the trigger is active
CREATE INDEX idx_roads_geom_gist ON gis.roads USING GIST (geom);
CREATE INDEX idx_boundaries_geom_gist ON gis.boundaries USING GIST (geom);
CREATE INDEX idx_station_locations_geom_gist ON gis.station_locations USING GIST (geom);
```

Missing indexes are a **Class A issue**.

#### Critical Rules
- **gis is derived enrichment** — inventory.station is authoritative for location
- **No app code writes to station_locations** — trigger function only
- **Trigger failures do not block** station writes
- **GIS can be resynced** via `gis.resync_all_stations()` stored procedure

---

### 4. analytics Schema

**Owner:** Clickstream Service  
**Responsibility:** Event ingestion, tracking, aggregation  
**Written By:** Clickstream Service only  
**Read By:** Admin Service (reporting only)

#### Tables

**raw_events**
```
event_id            TEXT PRIMARY KEY (EVT-...)
event_name          TEXT NOT NULL (from canonical taxonomy)
session_id          TEXT NOT NULL
user_id             TEXT (nullable — public users)
occurred_at         TIMESTAMP NOT NULL
received_at         TIMESTAMP DEFAULT NOW()
client_version      TEXT
device_type         TEXT (web, mobile, tablet)
os                  TEXT (ios, android, windows, macos, etc.)
properties          JSONB (event-specific data)
```

**event_aggregates** (pre-computed for reporting)
```
aggregate_id        TEXT PRIMARY KEY
metric_name         TEXT (unique_users_per_day, searches_by_location, etc.)
bucket_date         DATE
bucket_hour         INTEGER (0-23, nullable for daily buckets)
value               INTEGER or DECIMAL
labels              JSONB (location, station_id, etc.)
computed_at         TIMESTAMP
```

**gis_sync_log** (for monitoring trigger failures)
```
log_id              TEXT PRIMARY KEY
station_id          TEXT FK(inventory.station)
error_message       TEXT
logged_at           TIMESTAMP
```

#### Indexes
- **Primary Keys** on all ID columns
- **Composite Index** on (event_name, occurred_at) for querying by event
- **Index** on user_id for user-specific queries
- **Index** on session_id for session-based analysis
- **Index** on metric_name, bucket_date for reporting queries

#### Event Taxonomy
All event_name values must be in canonical taxonomy defined in `docs/guides/event-taxonomy.md`.

**Validation Rule:** Unknown event_name rejected by Clickstream Service with HTTP 400.

#### Critical Rules
- **Analytics data lives only in analytics schema**
- Never store analytics in inventory or users
- No app code reads raw_events except Admin Service for reporting
- Event IDs (EVT-...) enable deduplication

---

## Cross-Schema Access Rules

**Only this table of accesses is permitted:**

| From | To | Table | Operation | Purpose |
|------|----|----|-----------|---------|
| Admin Service | inventory | all | CRUD | Partner, station, charger management |
| Admin Service | users | partner_membership, user_profile | READ | Moderation context, user lookup |
| Admin Service | users | station_review | READ/UPDATE | Moderation of reviews |
| Admin Service | analytics | raw_events | READ | Reporting queries |
| Admin Service | analytics | event_aggregates | WRITE | Compute and store aggregates |
| Driver Service | inventory | partner, station, charger | READ | Discovery, details, search |
| Driver Service | gis | station_locations, roads, boundaries, amenity_points | READ | Spatial queries, enrichment |
| Driver Service | users | user_account, user_profile, favorite_station, station_review | CRUD | First-login, profile, favorites, reviews |
| Driver Service | analytics | none | — | Clickstream handled by frontend |
| Clickstream Service | analytics | raw_events | WRITE | Event ingestion |
| Clickstream Service | all others | none | — | No other schema access |
| Trigger Function | inventory | station | READ | Sync trigger on INSERT/UPDATE/DELETE |
| Trigger Function | gis | station_locations, roads, boundaries, amenity_points | READ/WRITE | Compute and update station locations |

**Any access not in this table is a constitution violation.**

---

## Identifier Scheme

All business entities use **prefixed NanoIDs** (21 characters):

| Prefix | Entity | Schema | Table | Example |
|--------|--------|--------|-------|---------|
| USR | User Account | users | user_account | USR-j4k2m9p3x1q8v5w2c6 |
| PRT | Partner | inventory | partner | PRT-h8n3k2v9x4m1p7q5r2 |
| STN | Station | inventory | station | STN-w2p5k8j3m1v4x9q2h6 |
| CHG | Charger | inventory | charger | CHG-r7m2x5k9q1v4j3h2p8 |
| REV | Review | users | station_review | REV-v4h1q8m5k2x9p3j7w1 |
| EVT | Event | analytics | raw_events | EVT-m2w5p8x1k4v3j7q9r2 |

**Rules:**
- NanoIDs are URL-safe (no special characters)
- Prefixes enable type identification without schema lookup
- Sequential integers **never** exposed in public APIs
- Used consistently in APIs, logs, and events

---

## GIS Synchronization Mechanism

### Source of Truth
**inventory.station** is the authoritative source for station location data (lat/lng).

### Sync Flow

**On INSERT into inventory.station:**
1. Trigger fires automatically within the same transaction
2. Compute Point geometry from latitude, longitude
3. Execute spatial queries to find:
   - Nearest road (ST_DWithin, ST_Distance)
   - Containing boundary (ST_Contains)
   - Nearby amenities (ST_DWithin)
4. UPSERT row into gis.station_locations
5. If sync fails: log error, allow transaction to commit

**On UPDATE of inventory.station (including lat/lng):**
1. Same as INSERT

**On DELETE from inventory.station:**
1. Delete corresponding gis.station_locations row
2. If delete fails: log error, allow transaction to commit

### Resync Procedure

**Scenario:** OSM data re-imported, trigger logic changed, manual recovery needed

**Procedure:** Call stored procedure:
```sql
SELECT gis.resync_all_stations();
```

**Effect:** Rebuilds all gis.station_locations from inventory.station

**Time:** ~10ms per station (for 1000 stations, ~10 seconds total)

---

## Migration Strategy

Each service owns migrations for its schema:

| Service | Schema | Migration Ownership |
|---------|--------|-------------|
| Admin Service | inventory | Admin Service code (up to date at startup) |
| Driver Service | users | Driver Service code (up to date at startup) |
| GIS Import Script | gis | Separate OSM import process |
| Clickstream Service | analytics | Clickstream Service code (up to date at startup) |

**Startup Rule:**
Each Rust service runs `sqlx::migrate!` before accepting requests. If migrations are not current, service fails fast with clear error.

---

## Backup & Recovery

### Daily Backups
- Full database dump to host filesystem
- Naming: `backups/bornemap-$(date +%Y%m%d-%H%M%S).sql.gz`
- Retention: 30 days
- Tested weekly via restore to staging

### Point-in-Time Recovery
- WAL archiving enabled
- Can recover to any point within 7 days
- Process documented in ops/backup.md

### Disaster Recovery
- Offsite copy of backups (host machine backup)
- Recovery time: ~30 minutes (restore dump + verify)
- Recovery point: latest daily backup
- Tested monthly

---

## Performance Optimization

### Indexes
- Spatial (GIST) indexes critical for nearby searches
- Composite indexes on query predicates
- Partial indexes on soft-deleted rows

### Query Patterns
- Nearby search: GIST index on lat/lng → sub-1ms response
- Station detail: Primary key lookup → <1ms
- Full-text search: B-tree index on name, address
- Filter/sort: B-tree indexes on common predicates

### Monitoring
- Query response times tracked via logging
- Slow query log monitored (>100ms)
- VACUUM and ANALYZE run nightly

---

## Schema Diagram

```
inventory (Admin writes)
├─ partner (PRT-...)
├─ station (STN-..., lat/lng)
├─ charger (CHG-...)
└─ station_availability

      ↓ (trigger)

gis (Trigger writes, Driver reads)
├─ roads (GIST index)
├─ boundaries (GIST index)
├─ amenity_points
└─ station_locations (derived from station)


users (Driver writes)
├─ user_account (USR-...)
├─ user_profile
├─ partner_membership (FK to inventory.partner)
├─ favorite_station (FK to inventory.station)
└─ station_review (REV-...)


analytics (Clickstream writes)
├─ raw_events (EVT-...)
└─ event_aggregates
```

---

## Constraints & Rules

### Uniqueness
- user_account.keycloak_sub — UNIQUE per user
- user_account.email — UNIQUE (case-insensitive)
- partner.name — UNIQUE
- station.(partner_id, name) — UNIQUE per partner
- favorite_station.(user_id, station_id) — UNIQUE per user
- partner_membership.user_id — PRIMARY KEY (one per user)

### Foreign Keys
- station.partner_id → partner
- charger.station_id → station
- favorite_station.user_id → user_account
- favorite_station.station_id → inventory.station
- station_review.user_id → user_account
- station_review.station_id → inventory.station
- partner_membership.user_id → user_account
- partner_membership.partner_id → inventory.partner

### Cascading Deletes
- station DELETED → chargers deleted (cascade)
- partner DELETED → stations deleted (cascade), partner_membership deleted (cascade)
- user_account DELETED → favorites, reviews, profile deleted (cascade)

---

## Data Validation

### Application-Level
- NanoID format validation (21 chars, alphanumeric)
- Email format (RFC 5322)
- Latitude/Longitude range (-90 to 90, -180 to 180)
- Decimal precision (DECIMAL(10,8) for lat/lng)
- Enum values (status, amenity_type, etc.)

### Database-Level
- NOT NULL constraints
- UNIQUE constraints
- Foreign key constraints
- CHECK constraints on ranges
- Trigger logic enforces additional rules

---

**Document Version:** 1.0  
**Status:** Active  
**Last Updated:** 2026-06-05
