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
| `id` | TEXT | PRIMARY KEY, NOT NULL, UNIQUE, CHECK (id ~ '^PRT-[A-Za-z0-9]{12}$') | External ID in format `PRT-{12 alphanumeric characters}` |
| `name` | TEXT | NOT NULL, UNIQUE | Partner organization name |
| `status` | TEXT | NOT NULL, DEFAULT 'ACTIVE' | Partner status (enum: ACTIVE, INACTIVE, MAINTENANCE, DISABLED) |
| `is_valid` | BOOLEAN | NOT NULL, DEFAULT TRUE | Partner validity flag |
| `created_by` | TEXT | NOT NULL, FK → admins.id | Administrator who created this partner |
| `updated_by` | TEXT | NOT NULL, FK → admins.id | Administrator who last updated this partner |
| `created_at` | TIMESTAMP | NOT NULL, DEFAULT NOW() | Audit timestamp |
| `updated_at` | TIMESTAMP | NOT NULL, DEFAULT NOW() | Audit timestamp (auto-updated) |
| `deleted_at` | TIMESTAMP | NULL | Soft delete timestamp (NULL = active) |

**Validation Rules**:
- `id` format: `PRT-{12 alphanumeric characters}` (Base62)
- `id` generated deterministically from string seed (NOT random nanoid)
- `name`: Required, unique, 1-200 characters, alphanumeric + spaces + hyphens only
- `status`: Must be one of: 'ACTIVE', 'INACTIVE', 'MAINTENANCE', 'DISABLED'
- `is_valid`: Boolean flag for partner validity (default TRUE)
- `created_by`: Must reference existing admin (admins table - assumed to exist in separate module)
- `updated_by`: Must reference existing admin
- **Soft Delete Rule**: Row is active ONLY if `deleted_at IS NULL`

**Identity Generation (Deterministic)**:
```rust
// platform-core/src/id/partner.rs
use nanoid::nanoid;

// Deterministic generation from string seed
pub fn generate_partner_id(seed: &str) -> String {
    format!("PRT-{}", deterministic_nanoid(seed, 12))
}

// Deterministic nanoid implementation
fn deterministic_nanoid(seed: &str, length: usize) -> String {
    // Use a simple hash-based deterministic generation
    // This ensures IDs are consistent across instances
    let seed_bytes = seed.as_bytes();
    let mut seed_hash = 0u64;

    for byte in seed_bytes {
        seed_hash = seed_hash.wrapping_mul(31).wrapping_add(*byte as u64);
    }

    nanoid!(length, &seed_hash.to_string().into_bytes())
}
```

**Business Invariants**:
- Partner is the top-level entity
- Partners cannot be modified after creation (immutable `id`, but `name`, `status`, etc. can be updated)
- Partners have an `is_valid` flag for soft deletion/provisioning control

**Rust Domain Entity**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partner {
    pub id: String,
    pub name: String,
    pub status: String,
    pub is_valid: bool,
    pub created_by: String,
    pub updated_by: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl Partner {
    pub fn validate(&self) -> Result<(), AppError> {
        // Validate ID format: PRT-{12 alphanumeric chars}
        if !self.id.starts_with("PRT-") {
            return Err(AppError::Validation("Invalid partner ID format. Expected PRT-{12 chars}".into()));
        }

        if self.id.len() != 18 {
            return Err(AppError::Validation("Invalid partner ID length. Expected 18 characters".into()));
        }

        // Validate name
        if self.name.trim().is_empty() {
            return Err(AppError::Validation("Name cannot be empty".into()));
        }

        if self.name.len() > 200 {
            return Err(AppError::Validation("Name cannot exceed 200 characters".into()));
        }

        if !self.name.chars().all(|c| c.is_alphanumeric() || c == ' ' || c == '-') {
            return Err(AppError::Validation("Name can only contain letters, numbers, spaces, and hyphens".into()));
        }

        // Validate status enum
        if !matches!(self.status.as_str(), "ACTIVE" | "INACTIVE" | "MAINTENANCE" | "DISABLED") {
            return Err(AppError::Validation("Invalid partner status. Must be: ACTIVE, INACTIVE, MAINTENANCE, or DISABLED".into()));
        }

        Ok(())
    }

    pub fn is_active(&self) -> bool {
        self.deleted_at.is_none()
    }
}
```

**Database Table**:
```sql
CREATE TABLE ev.partners (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    is_valid BOOLEAN NOT NULL DEFAULT TRUE,
    created_by TEXT NOT NULL REFERENCES admins(id),
    updated_by TEXT NOT NULL REFERENCES admins(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP NULL
);

CREATE UNIQUE INDEX idx_partners_name ON ev.partners(name);
CREATE INDEX idx_partners_deleted_at ON ev.partners(deleted_at);

-- CHECK constraint for ID format (PostgreSQL regex)
ALTER TABLE ev.partners
ADD CONSTRAINT chk_partners_id CHECK (id ~ '^PRT-[A-Za-z0-9]{12}$');
```

**Active Partners View**:
```sql
CREATE VIEW ev.active_partners AS
SELECT * FROM ev.partners WHERE deleted_at IS NULL;
```

---

### Station

Represents a physical charging location.

**Fields**:

| Field | Type | Constraints | Description |
|---|---|---|---|
| `id` | TEXT | PRIMARY KEY, NOT NULL, UNIQUE, CHECK (id ~ '^STA-[A-Za-z0-9]{12}$') | External ID in format `STA-{12 alphanumeric characters}` |
| `partner_id` | TEXT | NOT NULL, FK → ev.partners.id ON DELETE CASCADE | References partner external ID |
| `name` | TEXT | NOT NULL | Station location name |
| `location` | TEXT | NULL | Physical address or location details |
| `status` | TEXT | NOT NULL, DEFAULT 'ACTIVE' | Station status (enum: ACTIVE, INACTIVE, MAINTENANCE, DISABLED) |
| `created_by` | TEXT | NOT NULL, FK → admins.id | Administrator who created this station |
| `updated_by` | TEXT | NOT NULL, FK → admins.id | Administrator who last updated this station |
| `created_at` | TIMESTAMP | NOT NULL, DEFAULT NOW() | Audit timestamp |
| `updated_at` | TIMESTAMP | NOT NULL, DEFAULT NOW() | Audit timestamp (auto-updated) |
| `deleted_at` | TIMESTAMP | NULL | Soft delete timestamp (NULL = active) |

**Validation Rules**:
- `id` format: `STA-{12 alphanumeric characters}` (Base62)
- `id` generated deterministically from string seed (NOT random nanoid)
- `partner_id`: Must reference an existing active partner (PRT-*, deleted_at IS NULL)
- `name`: Required, 1-200 characters, alphanumeric + spaces + hyphens only
- `location`: Optional free-form text
- `status`: Must be one of: 'ACTIVE', 'INACTIVE', 'MAINTENANCE', 'DISABLED'
- **Soft Delete Rule**: Row is active ONLY if `deleted_at IS NULL`
- **Cascading Delete Rule (Hard Delete Only)**: When partner is hard-deleted (NOT soft-deleted), all stations are auto-removed via database CASCADE

**Relationships**:
- **One-to-Many**: Station belongs to one Partner (via `partner_id`)
- **One-to-Many**: Partner has many Stations
- **One-to-Many**: Station has many Chargers

**Cascading Delete Rules**:
- **Hard Delete**: Partner hard delete → ALL stations automatically deleted (ON DELETE CASCADE in database)
- **Soft Delete**: Partner soft delete → Stations NOT automatically deleted (remain active with their own soft delete state)

**Business Invariants**:
- A Station must have a valid, active Partner reference
- A Station's Partner cannot be changed after creation
- A Station's Partner is immutable (cannot be deleted if this station exists)

**Rust Domain Entity**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Station {
    pub id: String,
    pub partner_id: String,
    pub name: String,
    pub location: Option<String>,
    pub status: String,
    pub created_by: String,
    pub updated_by: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl Station {
    pub fn validate(&self) -> Result<(), AppError> {
        // Validate ID format: STA-{12 alphanumeric chars}
        if !self.id.starts_with("STA-") || self.id.len() != 18 {
            return Err(AppError::Validation("Invalid station ID format. Expected STA-{12 chars}".into()));
        }

        // Validate name
        if self.name.trim().is_empty() {
            return Err(AppError::Validation("Name cannot be empty".into()));
        }

        if self.name.len() > 200 {
            return Err(AppError::Validation("Name cannot exceed 200 characters".into()));
        }

        if !self.name.chars().all(|c| c.is_alphanumeric() || c == ' ' || c == '-') {
            return Err(AppError::Validation("Name can only contain letters, numbers, spaces, and hyphens".into()));
        }

        // Validate partner_id format
        if !self.partner_id.starts_with("PRT-") || self.partner_id.len() != 18 {
            return Err(AppError::Validation("Invalid partner ID reference. Expected PRT-{12 chars}".into()));
        }

        // Validate status enum
        if !matches!(self.status.as_str(), "ACTIVE" | "INACTIVE" | "MAINTENANCE" | "DISABLED") {
            return Err(AppError::Validation("Invalid station status. Must be: ACTIVE, INACTIVE, MAINTENANCE, or DISABLED".into()));
        }

        Ok(())
    }

    pub fn is_active(&self) -> bool {
        self.deleted_at.is_none()
    }
}
```

**Database Table**:
```sql
CREATE TABLE ev.stations (
    id TEXT PRIMARY KEY,
    partner_id TEXT NOT NULL REFERENCES ev.partners(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    location TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_by TEXT NOT NULL REFERENCES admins(id),
    updated_by TEXT NOT NULL REFERENCES admins(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP NULL
);

CREATE INDEX idx_stations_partner_id ON ev.stations(partner_id);
CREATE UNIQUE INDEX idx_stations_name ON ev.stations(name);
CREATE INDEX idx_stations_deleted_at ON ev.stations(deleted_at);

-- CHECK constraint for ID format
ALTER TABLE ev.stations
ADD CONSTRAINT chk_stations_id CHECK (id ~ '^STA-[A-Za-z0-9]{12}$');
```

**Active Stations View**:
```sql
CREATE VIEW ev.active_stations AS
SELECT * FROM ev.stations WHERE deleted_at IS NULL;
```

---

### Charger

Represents a charging unit within a station.

**Fields**:

| Field | Type | Constraints | Description |
|---|---|---|---|
| `id` | TEXT | PRIMARY KEY, NOT NULL, UNIQUE, CHECK (id ~ '^CHR-[A-Za-z0-9]{12}$') | External ID in format `CHR-{12 alphanumeric characters}` |
| `station_id` | TEXT | NOT NULL, FK → ev.stations.id ON DELETE CASCADE | References station external ID |
| `status` | TEXT | NOT NULL, DEFAULT 'ACTIVE' | Charger operational status (enum: ACTIVE, INACTIVE, MAINTENANCE, DISABLED) |
| `power_rating` | INTEGER | NOT NULL, CHECK (power_rating > 0 AND power_rating <= 1000) | Power rating in kilowatts (kW) |
| `created_by` | TEXT | NOT NULL, FK → admins.id | Administrator who created this charger |
| `updated_by` | TEXT | NOT NULL, FK → admins.id | Administrator who last updated this charger |
| `created_at` | TIMESTAMP | NOT NULL, DEFAULT NOW() | Audit timestamp |
| `updated_at` | TIMESTAMP | NOT NULL, DEFAULT NOW() | Audit timestamp (auto-updated) |
| `deleted_at` | TIMESTAMP | NULL | Soft delete timestamp (NULL = active) |

**Validation Rules**:
- `id` format: `CHR-{12 alphanumeric characters}` (Base62)
- `id` generated deterministically from string seed (NOT random nanoid)
- `station_id`: Must reference an existing active station (STA-*, deleted_at IS NULL)
- `status`: Must be one of: 'ACTIVE', 'INACTIVE', 'MAINTENANCE', 'DISABLED'
- `power_rating`: Positive integer, 1-1000 kW
- `power_rating` unit: kilowatts (kW) - explicitly defined
- **Soft Delete Rule**: Row is active ONLY if `deleted_at IS NULL`
- **Cascading Delete Rule (Hard Delete Only)**: When station is hard-deleted (NOT soft-deleted), all chargers are auto-removed via database CASCADE

**Relationships**:
- **One-to-Many**: Charger belongs to one Station (via `station_id`)
- **One-to-Many**: Station has many Chargers

**Status Transition Rules**:
- `ACTIVE`: Ready for use
- `INACTIVE`: Temporarily unavailable (user can use, but marked inactive)
- `MAINTENANCE`: Under maintenance, not available
- `DISABLED`: Completely unavailable

**Business Invariants**:
- A Charger must have a valid, active Station reference
- A Charger's Station cannot be changed after creation
- A Charger's Station is immutable (cannot be deleted if this charger exists)

**Rust Domain Entity**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Charger {
    pub id: String,
    pub station_id: String,
    pub status: String,
    pub power_rating: i32,
    pub created_by: String,
    pub updated_by: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub deleted_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl Charger {
    pub fn validate(&self) -> Result<(), AppError> {
        // Validate ID format: CHR-{12 alphanumeric chars}
        if !self.id.starts_with("CHR-") || self.id.len() != 18 {
            return Err(AppError::Validation("Invalid charger ID format. Expected CHR-{12 chars}".into()));
        }

        // Validate station_id format
        if !self.station_id.starts_with("STA-") || self.station_id.len() != 18 {
            return Err(AppError::Validation("Invalid station ID reference. Expected STA-{12 chars}".into()));
        }

        // Validate power rating
        if self.power_rating <= 0 {
            return Err(AppError::Validation("Power rating must be positive".into()));
        }

        if self.power_rating > 1000 {
            return Err(AppError::Validation("Power rating cannot exceed 1000 kW".into()));
        }

        // Validate status enum
        if !matches!(self.status.as_str(), "ACTIVE" | "INACTIVE" | "MAINTENANCE" | "DISABLED") {
            return Err(AppError::Validation("Invalid charger status. Must be: ACTIVE, INACTIVE, MAINTENANCE, or DISABLED".into()));
        }

        Ok(())
    }

    pub fn is_active(&self) -> bool {
        self.deleted_at.is_none()
    }
}
```

**Database Table**:
```sql
CREATE TABLE ev.chargers (
    id TEXT PRIMARY KEY,
    station_id TEXT NOT NULL REFERENCES ev.stations(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    power_rating INTEGER NOT NULL,
    created_by TEXT NOT NULL REFERENCES admins(id),
    updated_by TEXT NOT NULL REFERENCES admins(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP NULL
);

CREATE INDEX idx_chargers_station_id ON ev.chargers(station_id);
CREATE INDEX idx_chargers_status ON ev.chargers(status);
CREATE INDEX idx_chargers_deleted_at ON ev.chargers(deleted_at);

-- CHECK constraint for ID format
ALTER TABLE ev.chargers
ADD CONSTRAINT chk_chargers_id CHECK (id ~ '^CHR-[A-Za-z0-9]{12}$');

-- CHECK constraint for power rating
ALTER TABLE ev.chargers
ADD CONSTRAINT chk_power_rating CHECK (power_rating > 0 AND power_rating <= 1000);
```

**Active Chargers View**:
```sql
CREATE VIEW ev.active_chargers AS
SELECT * FROM ev.chargers WHERE deleted_at IS NULL;
```

---

## Relationships

```
Partner (PRT-xxx)
   │ 1 → N
   ▼
Station (STA-xxx)
   │ 1 → N
   ▼
Charger (CHR-xxx)
```

**Relationship Rules**:
1. **Partner → Station**:
   - One Partner can have many Stations
   - One Station belongs to exactly one Partner
   - **Hard Delete**: Partner hard delete (not soft) → ALL stations automatically deleted (database CASCADE)
   - **Soft Delete**: Partner soft delete → Stations NOT automatically deleted (remain with their own soft delete state)

2. **Station → Charger**:
   - One Station can have many Chargers
   - One Charger belongs to exactly one Station
   - **Hard Delete**: Station hard delete (not soft) → ALL chargers automatically deleted (database CASCADE)
   - **Soft Delete**: Station soft delete → Chargers NOT automatically deleted (remain with their own soft delete state)

3. **All → Dashboard KPIs**:
   - Dashboard aggregates counts of ACTIVE records only (deleted_at IS NULL)
   - Deleted partners/stations/chargers are not counted

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
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    is_valid BOOLEAN NOT NULL DEFAULT TRUE,
    created_by TEXT NOT NULL REFERENCES admins(id),
    updated_by TEXT NOT NULL REFERENCES admins(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP NULL
);

CREATE UNIQUE INDEX idx_partners_name ON ev.partners(name);
CREATE INDEX idx_partners_deleted_at ON ev.partners(deleted_at);

ALTER TABLE ev.partners
ADD CONSTRAINT chk_partners_id CHECK (id ~ '^PRT-[A-Za-z0-9]{12}$');

-- Stations table (CASCADE delete on hard delete of partner)
CREATE TABLE ev.stations (
    id TEXT PRIMARY KEY,
    partner_id TEXT NOT NULL REFERENCES ev.partners(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    location TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_by TEXT NOT NULL REFERENCES admins(id),
    updated_by TEXT NOT NULL REFERENCES admins(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP NULL
);

CREATE INDEX idx_stations_partner_id ON ev.stations(partner_id);
CREATE UNIQUE INDEX idx_stations_name ON ev.stations(name);
CREATE INDEX idx_stations_deleted_at ON ev.stations(deleted_at);

ALTER TABLE ev.stations
ADD CONSTRAINT chk_stations_id CHECK (id ~ '^STA-[A-Za-z0-9]{12}$');

-- Chargers table (CASCADE delete on hard delete of station)
CREATE TABLE ev.chargers (
    id TEXT PRIMARY KEY,
    station_id TEXT NOT NULL REFERENCES ev.stations(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    power_rating INTEGER NOT NULL,
    created_by TEXT NOT NULL REFERENCES admins(id),
    updated_by TEXT NOT NULL REFERENCES admins(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP NULL
);

CREATE INDEX idx_chargers_station_id ON ev.chargers(station_id);
CREATE INDEX idx_chargers_status ON ev.chargers(status);
CREATE INDEX idx_chargers_deleted_at ON ev.chargers(deleted_at);

ALTER TABLE ev.chargers
ADD CONSTRAINT chk_chargers_id CHECK (id ~ '^CHR-[A-Za-z0-9]{12}$');

ALTER TABLE ev.chargers
ADD CONSTRAINT chk_power_rating CHECK (power_rating > 0 AND power_rating <= 1000);

-- Views for active records (enforced query rules)
CREATE VIEW ev.active_partners AS SELECT * FROM ev.partners WHERE deleted_at IS NULL;
CREATE VIEW ev.active_stations AS SELECT * FROM ev.stations WHERE deleted_at IS NULL;
CREATE VIEW ev.active_chargers AS SELECT * FROM ev.chargers WHERE deleted_at IS NULL;
```

---

## Identity Generation Rules (Deterministic)

**Generation Layer**: Infrastructure only (platform-core)

**Algorithm**:
- Use Base62 character set (a-z, A-Z, 0-9)
- Use collision-resistant deterministic generation from string seed (NOT random)
- Length fixed at 12 characters
- Format: {ENTITY}-{12 chars} where ENTITY is PRT, STA, or CHR

**Implementation**:
```rust
// platform-core/src/id/mod.rs
use nanoid::nanoid;

// Deterministic ID generation from string seed
pub fn generate_partner_id(seed: &str) -> String {
    format!("PRT-{}", deterministic_nanoid(seed, 12))
}

pub fn generate_station_id(seed: &str) -> String {
    format!("STA-{}", deterministic_nanoid(seed, 12))
}

pub fn generate_charger_id(seed: &str) -> String {
    format!("CHR-{}", deterministic_nanoid(seed, 12))
}

// Deterministic nanoid implementation using hash
fn deterministic_nanoid(seed: &str, length: usize) -> String {
    let seed_bytes = seed.as_bytes();
    let mut seed_hash = 0u64;

    for byte in seed_bytes {
        seed_hash = seed_hash.wrapping_mul(31).wrapping_add(*byte as u64);
    }

    nanoid!(length, &seed_hash.to_string().into_bytes())
}
```

**Validation**:
```rust
pub fn validate_partner_id(id: &str) -> bool {
    id.len() == 18 && id.starts_with("PRT-") && id.chars().skip(4).all(|c| c.is_alphanumeric())
}

pub fn validate_station_id(id: &str) -> bool {
    id.len() == 18 && id.starts_with("STA-") && id.chars().skip(4).all(|c| c.is_alphanumeric())
}

pub fn validate_charger_id(id: &str) -> bool {
    id.len() == 18 && id.starts_with("CHR-") && id.chars().skip(4).all(|c| c.is_alphanumeric())
}
```

---

## Soft Delete Strategy (Explicit)

**Strategy**: Soft delete with `deleted_at` timestamp

**Rules**:
1. Deleted entities are NOT removed from database (rows remain with `deleted_at = NOW()`)
2. All queries MUST filter out soft-deleted rows by default (WHERE deleted_at IS NULL)
3. Applications MUST use views or explicit WHERE clauses for active records only
4. **Cascade Deletes**: Apply ONLY on hard delete operations, not soft delete

**Cascade Delete Rule (Hard Delete Only)**:
- When a Partner is **hard deleted**, ALL associated Stations are automatically deleted (database CASCADE)
- When a Station is **hard deleted**, ALL associated Chargers are automatically deleted (database CASCADE)
- Soft delete does NOT trigger cascade (stations/chargers remain active)

**Rationale**:
- Audit trail preservation (deleted_at records when and who deleted)
- Recovery capability (undelete if needed)
- Consistent with Clean Architecture simplicity
- Avoids data loss

**Database Implementation**:
```sql
-- Example: Hard delete partner (stations cascade automatically)
DELETE FROM ev.partners WHERE id = 'PRT-abc123456789';

-- Example: Soft delete partner (stations NOT automatically deleted)
UPDATE ev.partners SET deleted_at = NOW() WHERE id = 'PRT-abc123456789';

-- Example: Query only active partners (stations may still exist)
SELECT * FROM ev.partners WHERE deleted_at IS NULL;
```

---

## Data Integrity

### Foreign Key Constraints

- **stations.partner_id** → `ev.partners(id)` with ON DELETE CASCADE (applies to hard deletes only)
- **chargers.station_id** → `ev.stations(id)` with ON DELETE CASCADE (applies to hard deletes only)

### Constraint Enforcement

- **PRIMARY KEY**: All entities have unique `id`
- **UNIQUE**: Partners have unique names
- **NOT NULL**: Critical fields cannot be null
- **DEFAULT**: Timestamps default to `NOW()`, status defaults to 'ACTIVE'
- **CHECK**: ID format validation, power rating range, status enum

### Indexes

```sql
-- Partners
CREATE UNIQUE INDEX idx_partners_name ON ev.partners(name);
CREATE INDEX idx_partners_deleted_at ON ev.partners(deleted_at);

-- Stations
CREATE INDEX idx_stations_partner_id ON ev.stations(partner_id);
CREATE UNIQUE INDEX idx_stations_name ON ev.stations(name);
CREATE INDEX idx_stations_deleted_at ON ev.stations(deleted_at);

-- Chargers
CREATE INDEX idx_chargers_station_id ON ev.chargers(station_id);
CREATE INDEX idx_chargers_status ON ev.chargers(status);
CREATE INDEX idx_chargers_deleted_at ON ev.chargers(deleted_at);
```

---

## Repository Interfaces (Explicit)

**Purpose**: Define clear contracts for repository implementations (Clean Architecture)

**Implementation** (Rust traits):
```rust
// domain/repositories/partner_repository.rs
#[async_trait]
pub trait PartnerRepository: Send + Sync {
    // Domain layer defines traits
    async fn create(&self, name: String, created_by: String, updated_by: String) -> Result<Partner, AppError>;
    async fn get_by_id(&self, id: String) -> Result<Option<Partner>, AppError>;
    async fn list(&self, page: u32, limit: u32) -> Result<(Vec<Partner>, u64), AppError>;
    async fn hard_delete(&self, id: String) -> Result<(), AppError>;  // CASCADE to stations
    async fn soft_delete(&self, id: String, deleted_by: String) -> Result<(), AppError>;  // No cascade
    async fn undelete(&self, id: String, updated_by: String) -> Result<Partner, AppError>;
}

// domain/repositories/station_repository.rs
#[async_trait]
pub trait StationRepository: Send + Sync {
    // Domain layer defines traits
    async fn create(&self, name: String, location: Option<String>, partner_id: String, created_by: String, updated_by: String) -> Result<Station, AppError>;
    async fn get_by_id(&self, id: String) -> Result<Option<Station>, AppError>;
    async fn list(&self, page: u32, limit: u32, partner_id: Option<String>) -> Result<(Vec<Station>, u64), AppError>;
    async fn hard_delete(&self, id: String) -> Result<(), AppError>;  // CASCADE to chargers
    async fn soft_delete(&self, id: String, deleted_by: String) -> Result<(), AppError>;  // No cascade
    async fn undelete(&self, id: String, updated_by: String) -> Result<Station, AppError>;
}

// domain/repositories/charger_repository.rs
#[async_trait]
pub trait ChargerRepository: Send + Sync {
    // Domain layer defines traits
    async fn create(&self, station_id: String, status: String, power_rating: i32, created_by: String, updated_by: String) -> Result<Charger, AppError>;
    async fn get_by_id(&self, id: String) -> Result<Option<Charger>, AppError>;
    async fn list(&self, page: u32, limit: u32, station_id: Option<String>) -> Result<(Vec<Charger>, u64), AppError>;
    async fn update_status(&self, id: String, status: String, updated_by: String) -> Result<Charger, AppError>;
    async fn hard_delete(&self, id: String) -> Result<(), AppError>;  // No cascade (no children)
    async fn soft_delete(&self, id: String, deleted_by: String) -> Result<(), AppError>;  // No cascade
    async fn undelete(&self, id: String, updated_by: String) -> Result<Charger, AppError>;
}
```

---

## Service Layer (Explicit)

**Purpose**: Define application layer services for business logic

**Implementation**:
```rust
// application/services/partner_service.rs
pub struct PartnerService {
    repository: Arc<dyn PartnerRepository>,
}

impl PartnerService {
    pub fn new(repository: Arc<dyn PartnerRepository>) -> Self {
        Self { repository }
    }

    pub async fn create(&self, name: String, created_by: String, updated_by: String) -> Result<Partner, AppError> {
        // Domain invariants
        if name.trim().is_empty() {
            return Err(AppError::Validation("Name cannot be empty".into()));
        }

        if name.len() > 200 {
            return Err(AppError::Validation("Name cannot exceed 200 characters".into()));
        }

        if !name.chars().all(|c| c.is_alphanumeric() || c == ' ' || c == '-') {
            return Err(AppError::Validation("Name can only contain letters, numbers, spaces, and hyphens".into()));
        }

        self.repository.create(name, created_by, updated_by).await
    }

    pub async fn hard_delete(&self, id: String, deleted_by: String) -> Result<(), AppError> {
        // Domain invariants
        let partner = self.repository.get_by_id(id.clone()).await?
            .ok_or_else(|| AppError::NotFound(format!("Partner {} not found", id)))?;

        if !partner.is_active() {
            return Err(AppError::Validation("Partner is already deleted".into()));
        }

        // Hard delete triggers CASCADE (stations automatically deleted by database)
        self.repository.hard_delete(id).await
    }

    pub async fn soft_delete(&self, id: String, deleted_by: String) -> Result<(), AppError> {
        // Domain invariants
        let partner = self.repository.get_by_id(id.clone()).await?
            .ok_or_else(|| AppError::NotFound(format!("Partner {} not found", id)))?;

        if !partner.is_active() {
            return Err(AppError::Validation("Partner is already deleted".into()));
        }

        // Soft delete does NOT cascade (stations remain active)
        self.repository.soft_delete(id, deleted_by).await
    }

    pub async fn undelete(&self, id: String, updated_by: String) -> Result<Partner, AppError> {
        // Domain invariants
        let partner = self.repository.get_by_id(id.clone()).await?
            .ok_or_else(|| AppError::NotFound(format!("Partner {} not found", id)))?;

        if partner.is_active() {
            return Err(AppError::Validation("Partner is already active".into()));
        }

        self.repository.undelete(id, updated_by).await
    }
}

// application/services/station_service.rs
pub struct StationService {
    repository: Arc<dyn StationRepository>,
    partner_repository: Arc<dyn PartnerRepository>,
}

impl StationService {
    pub fn new(
        repository: Arc<dyn StationRepository>,
        partner_repository: Arc<dyn PartnerRepository>,
    ) -> Self {
        Self { repository, partner_repository }
    }

    pub async fn create(&self, name: String, location: Option<String>, partner_id: String, created_by: String, updated_by: String) -> Result<Station, AppError> {
        // Validate partner exists and is active
        let partner = self.partner_repository.get_by_id(partner_id.clone()).await?
            .ok_or_else(|| AppError::NotFound(format!("Partner {} not found", partner_id)))?;

        if !partner.is_active() {
            return Err(AppError::Validation("Partner must be active".into()));
        }

        // Domain invariants
        if name.trim().is_empty() {
            return Err(AppError::Validation("Name cannot be empty".into()));
        }

        self.repository.create(name, location, partner_id, created_by, updated_by).await
    }

    pub async fn hard_delete(&self, id: String, deleted_by: String) -> Result<(), AppError> {
        // Domain invariants
        let station = self.repository.get_by_id(id.clone()).await?
            .ok_or_else(|| AppError::NotFound(format!("Station {} not found", id)))?;

        if !station.is_active() {
            return Err(AppError::Validation("Station is already deleted".into()));
        }

        // Hard delete triggers CASCADE (chargers automatically deleted by database)
        self.repository.hard_delete(id).await
    }

    pub async fn soft_delete(&self, id: String, deleted_by: String) -> Result<(), AppError> {
        // Domain invariants
        let station = self.repository.get_by_id(id.clone()).await?
            .ok_or_else(|| AppError::NotFound(format!("Station {} not found", id)))?;

        if !station.is_active() {
            return Err(AppError::Validation("Station is already deleted".into()));
        }

        // Soft delete does NOT cascade (chargers remain active)
        self.repository.soft_delete(id, deleted_by).await
    }

    pub async fn undelete(&self, id: String, updated_by: String) -> Result<Station, AppError> {
        // Domain invariants
        let station = self.repository.get_by_id(id.clone()).await?
            .ok_or_else(|| AppError::NotFound(format!("Station {} not found", id)))?;

        if station.is_active() {
            return Err(AppError::Validation("Station is already active".into()));
        }

        self.repository.undelete(id, updated_by).await
    }
}

// application/services/charger_service.rs
pub struct ChargerService {
    repository: Arc<dyn ChargerRepository>,
    station_repository: Arc<dyn StationRepository>,
}

impl ChargerService {
    pub fn new(
        repository: Arc<dyn ChargerRepository>,
        station_repository: Arc<dyn StationRepository>,
    ) -> Self {
        Self { repository, station_repository }
    }

    pub async fn create(&self, station_id: String, status: String, power_rating: i32, created_by: String, updated_by: String) -> Result<Charger, AppError> {
        // Validate station exists and is active
        let station = self.station_repository.get_by_id(station_id.clone()).await?
            .ok_or_else(|| AppError::NotFound(format!("Station {} not found", station_id)))?;

        if !station.is_active() {
            return Err(AppError::Validation("Station must be active".into()));
        }

        // Domain invariants
        if power_rating <= 0 {
            return Err(AppError::Validation("Power rating must be positive".into()));
        }

        if power_rating > 1000 {
            return Err(AppError::Validation("Power rating cannot exceed 1000 kW".into()));
        }

        if !matches!(status.as_str(), "ACTIVE" | "INACTIVE" | "MAINTENANCE" | "DISABLED") {
            return Err(AppError::Validation("Invalid charger status. Must be: ACTIVE, INACTIVE, MAINTENANCE, or DISABLED".into()));
        }

        self.repository.create(station_id, status, power_rating, created_by, updated_by).await
    }

    pub async fn hard_delete(&self, id: String, deleted_by: String) -> Result<(), AppError> {
        // Domain invariants
        let charger = self.repository.get_by_id(id.clone()).await?
            .ok_or_else(|| AppError::NotFound(format!("Charger {} not found", id)))?;

        if !charger.is_active() {
            return Err(AppError::Validation("Charger is already deleted".into()));
        }

        // No cascade (chargers have no children)
        self.repository.hard_delete(id).await
    }

    pub async fn soft_delete(&self, id: String, deleted_by: String) -> Result<(), AppError> {
        // Domain invariants
        let charger = self.repository.get_by_id(id.clone()).await?
            .ok_or_else(|| AppError::NotFound(format!("Charger {} not found", id)))?;

        if !charger.is_active() {
            return Err(AppError::Validation("Charger is already deleted".into()));
        }

        // No cascade (chargers have no children)
        self.repository.soft_delete(id, deleted_by).await
    }

    pub async fn undelete(&self, id: String, updated_by: String) -> Result<Charger, AppError> {
        // Domain invariants
        let charger = self.repository.get_by_id(id.clone()).await?
            .ok_or_else(|| AppError::NotFound(format!("Charger {} not found", id)))?;

        if charger.is_active() {
            return Err(AppError::Validation("Charger is already active".into()));
        }

        self.repository.undelete(id, updated_by).await
    }
}
```

---

## Query Patterns

### Dashboard KPIs (Active Records Only)

```sql
-- Get all counts (only active records)
SELECT
    (SELECT COUNT(*) FROM ev.partners WHERE deleted_at IS NULL) AS partners_count,
    (SELECT COUNT(*) FROM ev.stations WHERE deleted_at IS NULL) AS stations_count,
    (SELECT COUNT(*) FROM ev.chargers WHERE deleted_at IS NULL) AS chargers_count;
```

### List Partners with Pagination

```sql
-- Get paginated list (only active records)
SELECT * FROM ev.partners
WHERE deleted_at IS NULL
ORDER BY created_at DESC
LIMIT $1 OFFSET $2;
```

### List Partners by Status

```sql
-- Get paginated list filtered by status
SELECT * FROM ev.partners
WHERE deleted_at IS NULL AND status = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;
```

### List Stations with Partner Filter

```sql
-- Get paginated list, optionally filtered by partner
SELECT * FROM ev.stations
WHERE deleted_at IS NULL AND (partner_id = $1 OR $1 IS NULL)
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;
```

### List Chargers with Station Filter

```sql
-- Get paginated list, optionally filtered by station
SELECT * FROM ev.chargers
WHERE deleted_at IS NULL AND (station_id = $1 OR $1 IS NULL)
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;
```

---

## Entity Relationships Summary

| Entity | ID Format | Parent Entity | Children Entities | Status Enum | Power Rating | Delete Strategy | is_valid |
|---|---|---|---|---|---|---|---|
| Partner | `PRT-<12-char>` | - | Station | ACTIVE, INACTIVE, MAINTENANCE, DISABLED | N/A | Hard delete = CASCADE; Soft delete = no cascade | BOOLEAN (default TRUE) |
| Station | `STA-<12-char>` | Partner (active only) | Charger | ACTIVE, INACTIVE, MAINTENANCE, DISABLED | N/A | Hard delete = CASCADE; Soft delete = no cascade | N/A |
| Charger | `CHR-<12-char>` | Station (active only) | - | ACTIVE, INACTIVE, MAINTENANCE, DISABLED | 1-1000 kW | Hard delete = no cascade; Soft delete = no cascade | N/A |

---

## Soft Delete Lifecycle

### Creation Flow

1. Admin provides data (name, status, etc.)
2. System validates (name format, status enum, partner reference)
3. System generates deterministic ID from string seed (infrastructure layer)
4. System sets `created_at`, `created_by`
5. System sets `updated_at`, `updated_by`
6. System inserts row with `deleted_at = NULL` (active)

### Update Flow

1. Admin provides new values
2. System validates new values
3. System sets `updated_at`, `updated_by`
4. System updates row

### Hard Delete Flow

1. Admin requests deletion
2. System validates entity exists and is active
3. System enforces cascade rules (database CASCADE for hard deletes)
4. System sets `deleted_at = NOW()`
5. System updates `updated_at`, `updated_by`
6. Row remains in database but is filtered out by default queries
7. Related entities automatically deleted via CASCADE (stations/chargers)

### Soft Delete Flow

1. Admin requests deletion
2. System validates entity exists and is active
3. System sets `deleted_at = NOW()`
4. System updates `updated_at`, `updated_by`
5. Row remains in database but is filtered out by default queries
6. Related entities NOT automatically deleted (stations/chargers remain with their own state)

### Undelete Flow (if needed)

1. Admin requests undelete
2. System validates entity is soft-deleted
3. System sets `deleted_at = NULL`
4. System updates `updated_at`, `updated_by`
5. Entity becomes active again

---

## Next Steps

1. Update API contracts to include hard delete (DELETE) and soft delete (soft_delete endpoint)
2. Update quickstart.md with soft delete patterns
3. Update AGENTS.md with deterministic ID and cascade delete rules
4. Update specification to reflect soft delete user stories
5. Generate implementation tasks with the new requirements
