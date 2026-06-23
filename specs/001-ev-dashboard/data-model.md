# Data Model: EV Dashboard Platform Kernel

**Feature**: EV Dashboard Platform Kernel
**Date**: 2026-06-23
**Branch**: 001-ev-dashboard

## Overview

This document defines the data entities for the EV Dashboard Platform Kernel, including their fields, relationships, validation rules, and database schema.

---

## Entities

### Partner

Represents an EV network operator organization.

**Fields**:

| Field | Type | Constraints | Description |
|---|---|---|---|
| `id` | TEXT | PRIMARY KEY, NOT NULL | External ID in format `PRT-<12-char>` |
| `name` | TEXT | NOT NULL, UNIQUE | Partner organization name |
| `created_at` | TIMESTAMP | NOT NULL, DEFAULT NOW() | Audit timestamp |

**Validation Rules**:
- `id` format: `PRT-{12 alphanumeric characters}`
- `name` length: 1-200 characters
- `name` must be unique across all partners
- `name` must contain only alphanumeric characters, spaces, and hyphens

**Business Invariants**:
- Partner is the top-level entity
- Partners cannot be modified after creation (immutable)

**Rust Domain Entity**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partner {
    pub id: String,
    pub name: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl Partner {
    pub fn validate(&self) -> Result<(), AppError> {
        if self.id.len() < 15 || !self.id.starts_with("PRT-") {
            return Err(AppError::Validation("Invalid partner ID format".into()));
        }

        if self.name.trim().is_empty() {
            return Err(AppError::Validation("Name cannot be empty".into()));
        }

        if self.name.len() > 200 {
            return Err(AppError::Validation("Name cannot exceed 200 characters".into()));
        }

        if !self.name.chars().all(|c| c.is_alphanumeric() || c == ' ' || c == '-') {
            return Err(AppError::Validation("Name can only contain letters, numbers, spaces, and hyphens".into()));
        }

        Ok(())
    }
}
```

**Database Table**:
```sql
CREATE TABLE ev.partners (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_partners_name ON ev.partners(name);
```

---

### Station

Represents a physical charging location.

**Fields**:

| Field | Type | Constraints | Description |
|---|---|---|---|
| `id` | TEXT | PRIMARY KEY, NOT NULL | External ID in format `STA-<12-char>` |
| `name` | TEXT | NOT NULL | Station location name |
| `location` | TEXT | NULL | Physical address or location details |
| `partner_id` | TEXT | NOT NULL, FOREIGN KEY | References `ev.partners.id` |
| `created_at` | TIMESTAMP | NOT NULL, DEFAULT NOW() | Audit timestamp |

**Validation Rules**:
- `id` format: `STA-{12 alphanumeric characters}`
- `name` length: 1-200 characters
- `name` must contain only alphanumeric characters, spaces, and hyphens
- `partner_id` must reference an existing partner
- `partner_id` is immutable (cannot be changed after creation)

**Relationships**:
- **One-to-Many**: Station belongs to one Partner (via `partner_id`)
- **One-to-Many**: Partner has many Stations

**Cascading Deletes**:
- When a Partner is deleted, ALL associated Stations are automatically deleted (CASCADE)

**Business Invariants**:
- A Station must have a valid Partner reference
- A Station's Partner cannot be changed after creation

**Rust Domain Entity**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Station {
    pub id: String,
    pub name: String,
    pub location: Option<String>,
    pub partner_id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl Station {
    pub fn validate(&self) -> Result<(), AppError> {
        if self.id.len() < 15 || !self.id.starts_with("STA-") {
            return Err(AppError::Validation("Invalid station ID format".into()));
        }

        if self.name.trim().is_empty() {
            return Err(AppError::Validation("Name cannot be empty".into()));
        }

        if self.name.len() > 200 {
            return Err(AppError::Validation("Name cannot exceed 200 characters".into()));
        }

        if !self.name.chars().all(|c| c.is_alphanumeric() || c == ' ' || c == '-') {
            return Err(AppError::Validation("Name can only contain letters, numbers, spaces, and hyphens".into()));
        }

        if self.partner_id.len() < 15 || !self.partner_id.starts_with("PRT-") {
            return Err(AppError::Validation("Invalid partner ID reference".into()));
        }

        Ok(())
    }
}
```

**Database Table**:
```sql
CREATE TABLE ev.stations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT,
    partner_id TEXT NOT NULL REFERENCES ev.partners(id) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_stations_partner_id ON ev.stations(partner_id);
CREATE INDEX idx_stations_name ON ev.stations(name);
```

---

### Charger

Represents a charging unit within a station.

**Fields**:

| Field | Type | Constraints | Description |
|---|---|---|---|
| `id` | TEXT | PRIMARY KEY, NOT NULL | External ID in format `CHR-<12-char>` |
| `station_id` | TEXT | NOT NULL, FOREIGN KEY | References `ev.stations.id` |
| `status` | TEXT | NOT NULL, DEFAULT 'active' | Charger operational status |
| `power_rating` | INTEGER | NOT NULL | Power rating in kilowatts (kW) |
| `created_at` | TIMESTAMP | NOT NULL, DEFAULT NOW() | Audit timestamp |

**Validation Rules**:
- `id` format: `CHR-{12 alphanumeric characters}`
- `status` enum: `active`, `inactive`, `maintenance`, `offline`
- `power_rating`: Positive integer, range 1-1000 kW (future: can be extended)

**Relationships**:
- **One-to-Many**: Charger belongs to one Station (via `station_id`)
- **One-to-Many**: Station has many Chargers

**Cascading Deletes**:
- When a Station is deleted, ALL associated Chargers are automatically deleted (CASCADE)

**Business Invariants**:
- A Charger must have a valid Station reference
- A Charger's Station cannot be changed after creation

**Status Transition Rules**:
- `active`: Ready for use
- `inactive`: Temporarily unavailable
- `maintenance`: Under maintenance
- `offline`: Completely unavailable

**Rust Domain Entity**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Charger {
    pub id: String,
    pub station_id: String,
    pub status: String,
    pub power_rating: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl Charger {
    pub fn validate(&self) -> Result<(), AppError> {
        if self.id.len() < 15 || !self.id.starts_with("CHR-") {
            return Err(AppError::Validation("Invalid charger ID format".into()));
        }

        if self.station_id.len() < 15 || !self.station_id.starts_with("STA-") {
            return Err(AppError::Validation("Invalid station ID reference".into()));
        }

        if self.power_rating <= 0 {
            return Err(AppError::Validation("Power rating must be positive".into()));
        }

        if self.power_rating > 1000 {
            return Err(AppError::Validation("Power rating cannot exceed 1000 kW".into()));
        }

        if !matches!(self.status.as_str(), "active" | "inactive" | "maintenance" | "offline") {
            return Err(AppError::Validation("Invalid charger status".into()));
        }

        Ok(())
    }
}
```

**Database Table**:
```sql
CREATE TABLE ev.chargers (
    id TEXT PRIMARY KEY,
    station_id TEXT NOT NULL REFERENCES ev.stations(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'active',
    power_rating INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_chargers_station_id ON ev.chargers(station_id);
CREATE INDEX idx_chargers_status ON ev.chargers(status);
```

---

## Relationships

```
Partner (PRT-xxx)
    │
    ├── 1..* Station (STA-xxx)
    │       │
    │       └── 1..* Charger (CHR-xxx)
    │
    └── 0..1 Dashboard KPIs (count aggregates)
```

**Relationship Rules**:
1. **Partner → Station**:
   - One Partner can have many Stations
   - One Station belongs to exactly one Partner
   - Deleted Partner automatically deletes all associated Stations (CASCADE)

2. **Station → Charger**:
   - One Station can have many Chargers
   - One Charger belongs to exactly one Station
   - Deleted Station automatically deletes all associated Chargers (CASCADE)

3. **All → Dashboard KPIs**:
   - Dashboard aggregates counts of all entities
   - No direct database relationship

---

## Database Schema

### Schema Namespace

**Namespace**: `ev`

**Reason**: Organized schema namespace for EV infrastructure data, separate from other application schemas if multiple are present.

---

### Complete Schema

```sql
-- Create schema
CREATE SCHEMA IF NOT EXISTS ev;

-- Partners table
CREATE TABLE ev.partners (
    id TEXT PRIMARY KEY,                    -- PRT-<12-char>
    name TEXT NOT NULL,                     -- Partner organization name
    created_at TIMESTAMP NOT NULL DEFAULT NOW()  -- Audit field
);

CREATE UNIQUE INDEX idx_partners_name ON ev.partners(name);

-- Stations table
CREATE TABLE ev.stations (
    id TEXT PRIMARY KEY,                    -- STA-<12-char>
    name TEXT NOT NULL,                     -- Station location name
    location TEXT,                          -- Physical address (optional)
    partner_id TEXT NOT NULL,               -- FK to ev.partners.id
    created_at TIMESTAMP NOT NULL DEFAULT NOW()  -- Audit field
);

CREATE INDEX idx_stations_partner_id ON ev.stations(partner_id);
CREATE UNIQUE INDEX idx_stations_name ON ev.stations(name);
CREATE INDEX idx_stations_location ON ev.stations(location);

-- Chargers table
CREATE TABLE ev.chargers (
    id TEXT PRIMARY KEY,                    -- CHR-<12-char>
    station_id TEXT NOT NULL,               -- FK to ev.stations.id
    status TEXT NOT NULL DEFAULT 'active',  -- Charger operational status
    power_rating INTEGER NOT NULL,          -- Power rating in kW
    created_at TIMESTAMP NOT NULL DEFAULT NOW()  -- Audit field
);

CREATE INDEX idx_chargers_station_id ON ev.chargers(station_id);
CREATE INDEX idx_chargers_status ON ev.chargers(status);
```

---

## Migration Files

### Migration 001: Create Schema

```sql
-- migrations/001_create_schema.sql
CREATE SCHEMA IF NOT EXISTS ev;
```

### Migration 002: Create Partners

```sql
-- migrations/002_create_partners.sql
CREATE TABLE ev.partners (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_partners_name ON ev.partners(name);
```

### Migration 003: Create Stations

```sql
-- migrations/003_create_stations.sql
CREATE TABLE ev.stations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT,
    partner_id TEXT NOT NULL REFERENCES ev.partners(id) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_stations_partner_id ON ev.stations(partner_id);
CREATE UNIQUE INDEX idx_stations_name ON ev.stations(name);
```

### Migration 004: Create Chargers

```sql
-- migrations/004_create_chargers.sql
CREATE TABLE ev.chargers (
    id TEXT PRIMARY KEY,
    station_id TEXT NOT NULL REFERENCES ev.stations(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'active',
    power_rating INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_chargers_station_id ON ev.chargers(station_id);
CREATE INDEX idx_chargers_status ON ev.chargers(status);
```

---

## Data Integrity

### Foreign Key Constraints

- **stations.partner_id** → `ev.partners.id` with CASCADE delete
- **chargers.station_id** → `ev.stations.id` with CASCADE delete

### Constraint Enforcement

- **PRIMARY KEY**: All entities have unique `id`
- **UNIQUE**: Partners have unique names
- **NOT NULL**: Critical fields cannot be null
- **DEFAULT**: Timestamps default to `NOW()`

### Cascading Delete Rules

```
DELETE FROM ev.partners WHERE id = 'PRT-xxx'
  → Automatically deletes all rows from ev.stations WHERE partner_id = 'PRT-xxx'
      → Automatically deletes all rows from ev.chargers WHERE station_id IN (
           SELECT id FROM ev.stations WHERE partner_id = 'PRT-xxx'
         )
```

---

## Query Patterns

### Dashboard KPIs (Aggregation)

```sql
-- Get all counts
SELECT
    (SELECT COUNT(*) FROM ev.partners) as partners_count,
    (SELECT COUNT(*) FROM ev.stations) as stations_count,
    (SELECT COUNT(*) FROM ev.chargers) as chargers_count;
```

### List Partners with Pagination

```sql
-- Get paginated list
SELECT * FROM ev.partners
ORDER BY created_at DESC
LIMIT $1 OFFSET $2;
```

### List Stations with Partner Filter

```sql
-- Get paginated list, optionally filtered by partner
SELECT * FROM ev.stations
WHERE partner_id = $1 OR $1 IS NULL
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;
```

---

## Entity Relationships Summary

| Entity | ID Format | Parent Entity | Children Entities | Primary Key |
|---|---|---|---|---|
| Partner | `PRT-<12-char>` | - | Station | `id` |
| Station | `STA-<12-char>` | Partner | Charger | `id` |
| Charger | `CHR-<12-char>` | Station | - | `id` |

---

## Next Steps

1. Generate API contracts in `/contracts/` directory
2. Create quickstart guide
3. Update AGENTS.md with plan reference
