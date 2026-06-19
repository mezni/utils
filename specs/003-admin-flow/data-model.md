# Data Model: Admin Service Core Operations

## Overview

This document defines the data model for Admin Service core operations, focusing on the `inventory` schema (partners, stations, chargers) and the `analytics_db.audit_log` table. All entities use NanoID with type-specific prefixes (OPR- / STA- / CHG-).

## Entity Relationships

```
inventory.partners (1) ----< (*) inventory.stations
inventory.stations (1) ----< (*) inventory.chargers
analytics_db.audit_log (0..*) <- all entities
```

---

## Entities

### 1. Partner (inventory.partners)

Represents a partner/operator organization in the charging network.

**Table**: `inventory.partners`

**Columns**:

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | TEXT | PRIMARY KEY, CHECK (id ~ '^(OPR-.+)') | Unique identifier with OPR- prefix |
| name | VARCHAR(255) | NOT NULL | Partner name |
| network_type | VARCHAR(20) | NOT NULL, CHECK (network_type IN ('INDIVIDUAL', 'COMPANY')) | Network type (individual vs company) |
| support_phone | VARCHAR(50) | | Support phone number (optional) |
| support_email | VARCHAR(255) | | Support email address (optional) |
| is_verified | BOOLEAN | DEFAULT FALSE | Verification status |
| created_by | TEXT | | Creator user ID (optional) |
| updated_by | TEXT | | Updater user ID (optional) |
| created_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Creation timestamp |
| updated_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Last update timestamp |
| deleted_at | TIMESTAMPTZ | | Soft delete timestamp (nullable) |

**Indexes**:
- `idx_partners_active`: Partial index on name where deleted_at IS NULL for efficient filtering of active partners

**Validation Rules**:
- `id` must match regex: `^(OPR-.+)`
- `name` must be 1-255 characters
- `network_type` must be one of: 'INDIVIDUAL' or 'COMPANY'
- `is_verified` defaults to FALSE

**State Transitions**:
- CREATE: Create new partner (id, name, network_type required)
- UPDATE: Update partner details (name, contact info, verification status)
- SOFT_DELETE: Set `deleted_at` timestamp (per constitution: "Soft delete enforced exclusively on infrastructure entities")

---

### 2. Station (inventory.stations)

Represents a physical charging location. Can have multiple chargers associated.

**Table**: `inventory.stations`

**Columns**:

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | TEXT | PRIMARY KEY, CHECK (id ~ '^(STA-.+)') | Unique identifier with STA- prefix |
| partner_id | TEXT | NOT NULL, FK to inventory.partners(id) | Parent partner |
| osm_id | BIGINT | | OpenStreetMap ID (nullable) |
| name | VARCHAR(255) | NOT NULL | Station name |
| address | TEXT | | Physical address (optional) |
| location | GEOGRAPHY(Point, 4326) | NOT NULL | Geolocation (WGS84) |
| tags | HSTORE | | Metadata (optional) |
| created_by | TEXT | | Creator user ID (optional) |
| updated_by | TEXT | | Updater user ID (optional) |
| created_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Creation timestamp |
| updated_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Last update timestamp |
| deleted_at | TIMESTAMPTZ | | Soft delete timestamp (nullable) |

**Indexes**:
- `idx_stations_partner`: Partial index on partner_id where deleted_at IS NULL
- `idx_stations_location`: GiST index on location for spatial queries

**Validation Rules**:
- `id` must match regex: `^(STA-.+)`
- `name` must be 1-255 characters
- `partner_id` must reference an existing, non-deleted partner
- `location` must be a valid point with SRID 4326
- `osm_id` is optional (partner-created stations may not have OSM reference)

**State Transitions**:
- CREATE: Create new station (partner_id, name, location required)
- UPDATE: Update station details (name, address, location, tags)
- SOFT_DELETE: Set `deleted_at` timestamp

**Foreign Key**:
- `partner_id` → `inventory.partners(id)` with ON DELETE CASCADE (per constitution: "ON DELETE CASCADE")

---

### 3. Charger (inventory.chargers)

Represents an individual charging point within a station.

**Table**: `inventory.chargers`

**Columns**:

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | TEXT | PRIMARY KEY, CHECK (id ~ '^(CHG-.+)') | Unique identifier with CHG- prefix |
| station_id | TEXT | NOT NULL, FK to inventory.stations(id) | Parent station |
| connector_type_id | SMALLINT | NOT NULL, FK to inventory.connector_types(id) | Connector type (CCS1, CCS2, CHAdeMO, etc.) |
| status_id | SMALLINT | NOT NULL, FK to inventory.connector_statuses(id) | Current status (available, occupied, maintenance) |
| current_type_id | SMALLINT | NOT NULL, FK to inventory.current_types(id) | Current type (AC, DC) |
| power_kw | DECIMAL(5,2) | | Power rating in kilowatts (nullable) |
| voltage | INT | | Voltage in volts (nullable) |
| amperage | INT | | Amperage in amps (nullable) |
| count_available | INT | DEFAULT 1, CHECK (count_available >= 0) | Number of available chargers |
| count_total | INT | DEFAULT 1, CHECK (count_total >= 1 AND count_total >= count_available) | Total number of chargers at this unit |
| created_by | TEXT | | Creator user ID (optional) |
| updated_by | TEXT | | Updater user ID (optional) |
| created_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Creation timestamp |
| updated_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Last update timestamp |
| deleted_at | TIMESTAMPTZ | | Soft delete timestamp (nullable) |

**Indexes**:
- `idx_chargers_station`: Partial index on station_id where deleted_at IS NULL

**Foreign Keys**:
- `station_id` → `inventory.stations(id)` with ON DELETE CASCADE
- `connector_type_id` → `inventory.connector_types(id)`
- `status_id` → `inventory.connector_statuses(id)`
- `current_type_id` → `inventory.current_types(id)`

**Validation Rules**:
- `id` must match regex: `^(CHG-.+)`
- `count_total` must be >= 1 and >= `count_available`
- `count_available` must be >= 0
- `power_kw`: nullable, max 5 characters (2 decimal places)
- `voltage`: nullable, typical range 220-480V for chargers
- `amperage`: nullable, typical range 10-200A for chargers

**State Transitions**:
- CREATE: Create new charger (station_id, connector_type_id, status_id, current_type_id required)
- UPDATE: Update charger technical specifications (power, voltage, amperage, status)
- SOFT_DELETE: Set `deleted_at` timestamp

**Uniqueness Constraint**:
- `UNIQUE(station_id, connector_type_id, current_type_id)`: Prevents duplicate charger units within same station

---

### 4. Audit Log (analytics_db.audit_log)

Represents a record of every mutation performed in the system.

**Table**: `analytics_db.audit_log`

**Columns**:

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | UUID | PRIMARY KEY, DEFAULT gen_random_uuid() | Unique audit record ID |
| actor_id | TEXT | NOT NULL | Actor user ID (USR- prefix, from X-User-Id header) |
| action | TEXT | NOT NULL | Action type (e.g., 'partner.created', 'station.updated') |
| target_type | TEXT | NOT NULL | Target entity type ('partner', 'station', 'charger') |
| target_id | TEXT | NOT NULL | Target entity ID (OPR- / STA- / CHG- prefix) |
| before_snapshot | JSONB | | Snapshot before mutation (NULL on CREATE) |
| after_snapshot | JSONB | | Snapshot after mutation |
| payload | JSONB | | Additional context (request body, metadata) |
| created_at | TIMESTAMPTZ | NOT NULL, DEFAULT NOW() | Record timestamp |

**Indexes**:
- `idx_audit_actor`: Partial index on actor_id for user-specific audit history
- `idx_audit_target`: Partial index on target_type and target_id for entity-specific audit history
- `idx_audit_created`: Descending index on created_at for recent audit log queries

**Validation Rules**:
- `action` must follow pattern: `<entity_type>.<action>` (e.g., 'partner.created', 'station.updated')
- `target_type` must be one of: 'partner', 'station', 'charger'
- `target_id` must match regex: `^(OPR-|STA-|CHG-.+)`

**Snapshot Format**:
- `before_snapshot`: Full JSON representation of entity before mutation (null for CREATE operations)
- `after_snapshot`: Full JSON representation of entity after mutation
- Both fields are JSONB for flexible schema evolution

**Behavior**:
- Audits are written AFTER successful transaction commit (per constitution: "Failure → log error, proceed. Audit is observability, not transactional")
- Audits do NOT trigger database rollbacks (per constitution: "Audit log failure does not roll back mutation")
- Audits are performed ONLY in orchestrator layer (repository layer is audit-unaware)

---

### 5. Materialized Views

Pre-computed aggregations for performance optimization.

#### 5.1 Inventory Station Summaries (inventory.mv_stations_summary)

**Purpose**: Fast read-only view for partner station listings and dashboards.

**Refresh Strategy**:
- Refreshed synchronously after every write operation to `inventory.stations` (per constitution: "Refresh strategy: Admin Service triggers REFRESH MATERIALIZED VIEW CONCURRENTLY synchronously after every station/charger write")

**Refresh Implementation**:
- Use `REFRESH MATERIALIZED VIEW CONCURRENTLY` to avoid table locks
- Use 2-5s soft timeout guard (per spec: "Synchronously awaited with 2–5s soft timeout guard")
- On timeout: log warning and continue (failure is tolerated)

**Definition** (simplified):
```sql
CREATE MATERIALIZED VIEW inventory.mv_stations_summary AS
SELECT
    s.id,
    s.name,
    s.location,
    s.partner_id,
    p.name as partner_name,
    COUNT(c.id) as charger_count,
    COUNT(c.id) FILTER (WHERE c.status_id = 1) as available_count
FROM inventory.stations s
LEFT JOIN inventory.chargers c ON c.station_id = s.id AND c.deleted_at IS NULL
LEFT JOIN inventory.partners p ON p.id = s.partner_id AND p.deleted_at IS NULL
WHERE s.deleted_at IS NULL
GROUP BY s.id, s.name, s.location, s.partner_id, p.name;
```

---

#### 5.2 Geospatial Station Summaries (inventory.mv_stations_geo)

**Purpose**: Spatial queries for map rendering (e.g., "find stations near X").

**Refresh Strategy**:
- Synchronous refresh after every write operation
- Use GiST index for efficient spatial joins

**Definition** (simplified):
```sql
CREATE MATERIALIZED VIEW inventory.mv_stations_geo AS
SELECT
    s.id,
    s.name,
    s.location,
    s.partner_id,
    COUNT(c.id) as charger_count,
    ST_AsGeoJSON(s.location) as location_geojson
FROM inventory.stations s
LEFT JOIN inventory.chargers c ON c.station_id = s.id AND c.deleted_at IS NULL
WHERE s.deleted_at IS NULL
GROUP BY s.id, s.name, s.location, s.partner_id;
```

**Indexes**:
- GiST index on `location` for spatial filtering

---

## Data Validation Summary

| Entity | Validation Type | Rules |
|--------|----------------|-------|
| Partner | Schema constraints | Unique ID with OPR- prefix, network_type enum, NOT NULL fields |
| Station | Schema + Foreign Key | Unique ID with STA- prefix, valid partner_id reference, point geography |
| Charger | Schema + Foreign Key + Uniqueness | Unique ID with CHG- prefix, valid station_id, count >= 0, unique (station, connector, current_type) |
| Audit Log | Application validation | Valid action pattern, target_type enum, valid target_id format |

---

## Repository Layer Design (Audit-Unaware)

Per constitution: "Diff computed in service layer — repository layer MUST be audit-unaware"

**Repository Methods** (Do NOT log to audit log):
- `create_partner(tx, payload)` → `Partner`
- `update_partner(tx, id, payload)` → `Partner`
- `delete_partner_soft(tx, id)` → `bool`
- `get_partner_by_id(tx, id)` → `Option<Partner>`
- `get_partners_by_partner_id(tx, partner_id)` → `Vec<Partner>`
- Similar methods for stations and chargers

**Why Audit-Unaware?**
- Repository layer is only responsible for data access
- Audit logic is in orchestrator layer (full context for diff computation)
- Keeps repositories simple and testable
- Follows single responsibility principle

---

## Diff Computation (Service Layer)

**Who computes diffs?**
- Orchestrator layer (`admin_orchestrator.rs`), NOT repository layer

**When are diffs computed?**
- BEFORE mutation: Fetch current state from database (or assume NULL for CREATE)
- AFTER mutation: Fetch new state from database (or use mutated payload)

**Where are diffs stored?**
- In `analytics_db.audit_log.before_snapshot` (BEFORE mutation, NULL for CREATE)
- In `analytics_db.audit_log.after_snapshot` (AFTER mutation)

**Implementation Pattern**:
```rust
// Orchestrator computes diffs before logging audit
async fn log_partner_creation(&self, pool: &PgPool, claims: &Claims, partner: &Partner) -> Result<()> {
    let before_snapshot = None; // CREATE operation, no previous state

    let after_snapshot = serde_json::json!({
        "id": partner.id,
        "name": partner.name,
        "network_type": partner.network_type,
        // ... all fields
    });

    sqlx::query!(
        "INSERT INTO analytics_db.audit_log (
            actor_id, action, target_type, target_id,
            before_snapshot, after_snapshot, payload, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        ",
        claims.sub,
        "partner.created",
        "partner",
        &partner.id,
        before_snapshot,
        after_snapshot,
        serde_json::json!({"name": partner.name}),
    ).execute(pool).await?;

    Ok(())
}
```

---

## Data Consistency Guarantees

| Operation | Transaction Boundary | Post-Commit Steps | Failure Policy |
|-----------|---------------------|-------------------|----------------|
| Partner CRUD | Explicit `sqlx::Transaction` | MV refresh, Redis bust, Audit log | Do NOT rollback on failure |
| Station CRUD | Explicit `sqlx::Transaction` | MV refresh, Redis bust, Audit log | Do NOT rollback on failure |
| Charger CRUD | Explicit `sqlx::Transaction` | MV refresh, Redis bust, Audit log | Do NOT rollback on failure |

**Key Points**:
- All multi-table writes wrapped in explicit transactions (per constitution)
- Post-commit steps execute AFTER commit (not before)
- Redis bust and MV refresh failures log warnings but do NOT roll back transaction
- Audit log failures log errors but do NOT roll back transaction
- Synchronous post-commit steps ensure consistency

---

## Summary

This data model defines:
- 3 main entities: Partner, Station, Charger
- 1 audit entity: Audit Log
- 2 materialized views: mv_stations_summary, mv_stations_geo
- Clear separation between repository (audit-unaware) and orchestrator (audit-aware) layers
- All entities use NanoID with type-specific prefixes
- Comprehensive validation rules and constraints
- Soft delete pattern for infrastructure entities (per constitution)
- Complete audit trail with before/after snapshots
