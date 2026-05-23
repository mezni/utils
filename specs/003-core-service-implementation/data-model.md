# Data Model: Core Service Implementation

**Feature**: [spec.md](./spec.md)
**Plan**: [plan.md](./plan.md)
**Date**: 2026-05-23

## Purpose

This document defines the data model for the core service implementation, including entities, attributes, relationships, and validation rules based on the feature specification and constitution requirements. The model is designed for Rust + Actix Web implementation with SQLx.

## Core Entities

### Company

Represents the top-level business entity that owns stations and chargers.

**Table**: `companies`

**Fields**:
- `id` VARCHAR(255) PRIMARY KEY - Company identifier using CMP-<nanoid> format
- `name` VARCHAR(255) NOT NULL - Company legal name
- `description` TEXT - Company description
- `email` VARCHAR(255) - Company contact email
- `phone` VARCHAR(50) - Company contact phone
- `website` VARCHAR(255) - Company website
- `address` TEXT - Company headquarters address
- `logo_url` VARCHAR(500) - URL to company logo
- `is_active` BOOLEAN DEFAULT true - Whether company is active
- `created_at` TIMESTAMPTZ NOT NULL DEFAULT NOW() - Creation timestamp
- `updated_at` TIMESTAMPTZ NOT NULL DEFAULT NOW() - Last update timestamp
- `deleted_at` TIMESTAMPTZ NULL - Soft delete timestamp (Constitution Principle IV)

**Rust Struct**:
```rust
#[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct Company {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub website: Option<String>,
    pub address: Option<String>,
    pub logo_url: Option<String>,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}
```

**Validation Rules**:
- `id`: Must match pattern `^CMP-[a-zA-Z0-9]{8,12}$`
- `name`: Required, max 255 characters
- `email`: Must be valid email format if provided
- `phone`: Must match phone number pattern if provided
- `website`: Must be valid URL if provided

**Relationships**:
- One-to-Many with Station (A company can have multiple stations)

### Station

Represents a charging station owned by a company or individual.

**Table**: `stations`

**Fields**:
- `id` VARCHAR(255) PRIMARY KEY - Station identifier using STA-<nanoid> format
- `company_id` VARCHAR(255) NOT NULL - Foreign key to companies.id
- `name` VARCHAR(255) NOT NULL - Station name
- `description` TEXT - Station description
- `address` TEXT NOT NULL - Station address
- `latitude` DECIMAL(10, 8) NOT NULL - Geographic latitude
- `longitude` DECIMAL(11, 8) NOT NULL - Geographic longitude
- `phone` VARCHAR(50) - Station contact phone
- `email` VARCHAR(255) - Station contact email
- `website` VARCHAR(255) - Station website
- `access_type` VARCHAR(50) NOT NULL DEFAULT 'public' - Access type (public, private, restricted)
- `operating_hours` JSON - Operating hours structure
- `amenities` JSON[] - Available amenities array
- `is_active` BOOLEAN DEFAULT true - Whether station is active
- `created_at` TIMESTAMPTZ NOT NULL DEFAULT NOW() - Creation timestamp
- `updated_at` TIMESTAMPTZ NOT NULL DEFAULT NOW() - Last update timestamp
- `deleted_at` TIMESTAMPTZ NULL - Soft delete timestamp (Constitution Principle IV)

**Rust Struct**:
```rust
#[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct Station {
    pub id: String,
    pub company_id: String,
    pub name: String,
    pub description: Option<String>,
    pub address: String,
    pub latitude: f64,
    pub longitude: f64,
    pub phone: Option<String>,
    pub email: Option<String>,
    pub website: Option<String>,
    pub access_type: AccessType,
    pub operating_hours: Option<serde_json::Value>,
    pub amenities: Option<Vec<String>>,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::Type, serde::Serialize, serde::Deserialize, Debug)]
#[sqlx(type_name = "access_type")]
pub enum AccessType {
    Public,
    Private,
    Restricted,
}
```

**Validation Rules**:
- `id`: Must match pattern `^STA-[a-zA-Z0-9]{8,12}$`
- `company_id`: Required, must reference existing company
- `name`: Required, max 255 characters
- `latitude`: Required, must be between -90 and 90
- `longitude`: Required, must be between -180 and 180
- `access_type`: Must be one of: 'public', 'private', 'restricted'

**Relationships**:
- Many-to-One with Company (A station belongs to exactly one company)
- One-to-Many with Charger (A station can have multiple chargers)

### Charger

Represents an individual charging unit at a station.

**Table**: `chargers`

**Fields**:
- `id` VARCHAR(255) PRIMARY KEY - Charger identifier using CHR-<nanoid> format
- `station_id` VARCHAR(255) NOT NULL - Foreign key to stations.id
- `name` VARCHAR(255) NOT NULL - Charger name/identifier
- `charger_type` VARCHAR(50) NOT NULL - Charger type (AC, DC, DCFC)
- `power_kw` DECIMAL(6, 2) NOT NULL - Power output in kilowatts
- `voltage` DECIMAL(5, 1) - Voltage in volts
- `amperage` DECIMAL(5, 1) - Amperage in amps
- `connectors` JSON[] - Available connector types with formats
- `status` VARCHAR(50) NOT NULL DEFAULT 'available' - Status (available, occupied, out_of_service, planned)
- `network_id` VARCHAR(100) - Network identifier if applicable
- `last_maintenance_date` DATE - Date of last maintenance
- `next_maintenance_date` DATE - Date of next scheduled maintenance
- `is_active` BOOLEAN DEFAULT true - Whether charger is active
- `created_at` TIMESTAMPTZ NOT NULL DEFAULT NOW() - Creation timestamp
- `updated_at` TIMESTAMPTZ NOT NULL DEFAULT NOW() - Last update timestamp
- `deleted_at` TIMESTAMPTZ NULL - Soft delete timestamp (Constitution Principle IV)
- `version` INTEGER NOT NULL DEFAULT 1 - Optimistic concurrency version (R-008)

**Rust Struct**:
```rust
#[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct Charger {
    pub id: String,
    pub station_id: String,
    pub name: String,
    pub charger_type: ChargerType,
    pub power_kw: f32,
    pub voltage: Option<f32>,
    pub amperage: Option<f32>,
    pub connectors: Option<Vec<Connector>>,
    pub status: ChargerStatus,
    pub network_id: Option<String>,
    pub last_maintenance_date: Option<NaiveDate>,
    pub next_maintenance_date: Option<NaiveDate>,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub version: i32,
}

#[derive(sqlx::Type, serde::Serialize, serde::Deserialize, Debug)]
#[sqlx(type_name = "charger_type")]
pub enum ChargerType {
    AC,
    DC,
    DCFC,
}

#[derive(sqlx::Type, serde::Serialize, serde::Deserialize, Debug)]
#[sqlx(type_name = "charger_status")]
pub enum ChargerStatus {
    Available,
    Occupied,
    OutOfService,
    Planned,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Connector {
    pub connector_type: String,
    pub power_kw: Option<f32>,
    pub status: Option<String>,
}
```

**Validation Rules**:
- `id`: Must match pattern `^CHR-[a-zA-Z0-9]{8,12}$`
- `station_id`: Required, must reference existing station
- `name`: Required, max 255 characters
- `charger_type`: Required, must be one of: 'AC', 'DC', 'DCFC'
- `power_kw`: Required, must be greater than 0
- `status`: Must be one of: 'available', 'occupied', 'out_of_service', 'planned'
- `version`: Required, must be greater than or equal to 1

**Relationships**:
- Many-to-One with Station (A charger belongs to exactly one station)

### User

Represents authenticated users who interact with the system.

**Table**: `users`

**Fields**:
- `id` VARCHAR(255) PRIMARY KEY - User identifier from Keycloak
- `keycloak_id` VARCHAR(255) NOT NULL UNIQUE - Keycloak user ID
- `email` VARCHAR(255) NOT NULL UNIQUE - User email
- `first_name` VARCHAR(100) - User first name
- `last_name` VARCHAR(100) - User last name
- `roles` JSON[] NOT NULL DEFAULT '["user"]' - User roles array
- `is_active` BOOLEAN DEFAULT true - Whether user is active
- `created_at` TIMESTAMPTZ NOT NULL DEFAULT NOW() - Creation timestamp
- `updated_at` TIMESTAMPTZ NOT NULL DEFAULT NOW() - Last update timestamp

**Rust Struct**:
```rust
#[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct User {
    pub id: String,
    pub keycloak_id: String,
    pub email: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub roles: Vec<String>,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

**Validation Rules**:
- `keycloak_id`: Required, unique, matches Keycloak user ID format
- `email`: Required, unique, valid email format
- `roles`: Required, must be valid JSON array of role names

**Relationships**:
- One-to-Many with Favorite (A user can have multiple favorites)
- One-to-Many with Review (A user can write multiple reviews)

### Favorite

Represents a user's favorite charging stations.

**Table**: `favorites`

**Fields**:
- `id` VARCHAR(255) PRIMARY KEY - Favorite identifier using FAV-<nanoid> format
- `user_id` VARCHAR(255) NOT NULL - Foreign key to users.id
- `station_id` VARCHAR(255) NOT NULL - Foreign key to stations.id
- `note` TEXT - User note about this favorite
- `created_at` TIMESTAMPTZ NOT NULL DEFAULT NOW() - Creation timestamp
- `updated_at` TIMESTAMPTZ NOT NULL DEFAULT NOW() - Last update timestamp

**Rust Struct**:
```rust
#[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct Favorite {
    pub id: String,
    pub user_id: String,
    pub station_id: String,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

**Validation Rules**:
- `id`: Must match pattern `^FAV-[a-zA-Z0-9]{8,12}$`
- `user_id`: Required, must reference existing user
- `station_id`: Required, must reference existing station
- **Unique Constraint**: (user_id, station_id) must be unique

**Relationships**:
- Many-to-One with User (A favorite belongs to exactly one user)
- Many-to-One with Station (A favorite is for exactly one station)

### Review

Represents user reviews for charging stations.

**Table**: `reviews`

**Fields**:
- `id` VARCHAR(255) PRIMARY KEY - Review identifier using REV-<nanoid> format
- `user_id` VARCHAR(255) NOT NULL - Foreign key to users.id
- `station_id` VARCHAR(255) NOT NULL - Foreign key to stations.id
- `rating` SMALLINT NOT NULL CHECK (rating >= 1 AND rating <= 5) - Rating from 1-5
- `title` VARCHAR(255) NOT NULL - Review title
- `comment` TEXT NOT NULL - Review comment
- `is_moderated` BOOLEAN DEFAULT false - Whether review has been moderated
- `moderation_status` VARCHAR(50) DEFAULT 'pending' - Moderation status (pending, approved, rejected)
- `moderated_by` VARCHAR(255) - Foreign key to users.id (moderator)
- `moderated_at` TIMESTAMPTZ NULL - When review was moderated
- `created_at` TIMESTAMPTZ NOT NULL DEFAULT NOW() - Creation timestamp
- `updated_at` TIMESTAMPTZ NOT NULL DEFAULT NOW() - Last update timestamp

**Rust Struct**:
```rust
#[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct Review {
    pub id: String,
    pub user_id: String,
    pub station_id: String,
    pub rating: i16,
    pub title: String,
    pub comment: String,
    pub is_moderated: bool,
    pub moderation_status: ModerationStatus,
    pub moderated_by: Option<String>,
    pub moderated_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(sqlx::Type, serde::Serialize, serde::Deserialize, Debug)]
#[sqlx(type_name = "moderation_status")]
pub enum ModerationStatus {
    Pending,
    Approved,
    Rejected,
}
```

**Validation Rules**:
- `id`: Must match pattern `^REV-[a-zA-Z0-9]{8,12}$`
- `user_id`: Required, must reference existing user
- `station_id`: Required, must reference existing station
- `rating`: Required, must be between 1 and 5
- `title`: Required, max 255 characters
- `comment`: Required
- `moderation_status`: Must be one of: 'pending', 'approved', 'rejected'

**Relationships**:
- Many-to-One with User (A review is written by exactly one user)
- Many-to-One with Station (A review is for exactly one station)
- Many-to-One with User (moderated_by) (A review is moderated by at most one user)

### Outbox

Represents domain events to be published to RabbitMQ (Constitution Principle III).

**Table**: `outbox`

**Fields**:
- `id` BIGSERIAL PRIMARY KEY - Auto-incrementing ID
- `event_id` VARCHAR(255) NOT NULL UNIQUE - Unique event identifier
- `event_type` VARCHAR(100) NOT NULL - Type of event
- `aggregate_type` VARCHAR(100) NOT NULL - Type of aggregate (Company, Station, Charger)
- `aggregate_id` VARCHAR(255) NOT NULL - ID of the aggregate
- `payload` JSONB NOT NULL - Event payload
- `metadata` JSONB DEFAULT '{}' - Event metadata
- `status` VARCHAR(50) NOT NULL DEFAULT 'pending' - Status (pending, published, failed)
- `created_at` TIMESTAMPTZ NOT NULL DEFAULT NOW() - Creation timestamp
- `published_at` TIMESTAMPTZ NULL - When event was published
- `error_message` TEXT - Error message if publishing failed

**Rust Struct**:
```rust
#[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct OutboxEvent {
    pub id: i64,
    pub event_id: String,
    pub event_type: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub payload: serde_json::Value,
    pub metadata: serde_json::Value,
    pub status: EventStatus,
    pub created_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
}

#[derive(sqlx::Type, serde::Serialize, serde::Deserialize, Debug)]
#[sqlx(type_name = "event_status")]
pub enum EventStatus {
    Pending,
    Published,
    Failed,
}
```

**Validation Rules**:
- `event_id`: Required, unique
- `event_type`: Required
- `aggregate_type`: Required, must be one of: 'Company', 'Station', 'Charger'
- `aggregate_id`: Required, must reference existing aggregate
- `payload`: Required, valid JSON
- `status`: Must be one of: 'pending', 'published', 'failed'

**Relationships**:
- None (this is an event store table)

### AuditLog

Tracks changes to core entities for compliance and debugging.

**Table**: `audit_logs`

**Fields**:
- `id` BIGSERIAL PRIMARY KEY - Auto-incrementing ID
- `entity_type` VARCHAR(100) NOT NULL - Type of entity (Company, Station, Charger, etc.)
- `entity_id` VARCHAR(255) NOT NULL - ID of the entity
- `action` VARCHAR(50) NOT NULL - Action performed (CREATE, UPDATE, DELETE)
- `changes` JSONB NOT NULL - Changes made (before/after values)
- `user_id` VARCHAR(255) NOT NULL - Foreign key to users.id (who made the change)
- `user_email` VARCHAR(255) NOT NULL - Email of user who made the change
- `ip_address` INET - IP address of user
- `user_agent` TEXT - User agent string
- `created_at` TIMESTAMPTZ NOT NULL DEFAULT NOW() - Creation timestamp

**Rust Struct**:
```rust
#[derive(sqlx::FromRow, serde::Serialize, serde::Deserialize)]
pub struct AuditLog {
    pub id: i64,
    pub entity_type: String,
    pub entity_id: String,
    pub action: AuditAction,
    pub changes: serde_json::Value,
    pub user_id: String,
    pub user_email: String,
    pub ip_address: Option<IpAddr>,
    pub user_agent: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(sqlx::Type, serde::Serialize, serde::Deserialize, Debug)]
#[sqlx(type_name = "audit_action")]
pub enum AuditAction {
    Create,
    Update,
    Delete,
}
```

**Validation Rules**:
- `entity_type`: Required
- `entity_id`: Required
- `action`: Required, must be one of: 'CREATE', 'UPDATE', 'DELETE'
- `changes`: Required, valid JSON
- `user_id`: Required, must reference existing user
- `user_email`: Required, valid email format

**Relationships**:
- Many-to-One with User (An audit log entry is created by exactly one user)

## Database Constraints

### Foreign Key Constraints

```sql
-- Station to Company
ALTER TABLE stations 
ADD CONSTRAINT fk_station_company 
FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;

-- Charger to Station
ALTER TABLE chargers 
ADD CONSTRAINT fk_charger_station 
FOREIGN KEY (station_id) REFERENCES stations(id) ON DELETE CASCADE;

-- Favorite to User
ALTER TABLE favorites 
ADD CONSTRAINT fk_favorite_user 
FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;

-- Favorite to Station
ALTER TABLE favorites 
ADD CONSTRAINT fk_favorite_station 
FOREIGN KEY (station_id) REFERENCES stations(id) ON DELETE CASCADE;

-- Review to User
ALTER TABLE reviews 
ADD CONSTRAINT fk_review_user 
FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;

-- Review to Station
ALTER TABLE reviews 
ADD CONSTRAINT fk_review_station 
FOREIGN KEY (station_id) REFERENCES stations(id) ON DELETE CASCADE;

-- Review to Moderator
ALTER TABLE reviews 
ADD CONSTRAINT fk_review_moderator 
FOREIGN KEY (moderated_by) REFERENCES users(id) ON DELETE SET NULL;

-- AuditLog to User
ALTER TABLE audit_logs 
ADD CONSTRAINT fk_audit_user 
FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;
```

### Unique Constraints

```sql
-- Unique user email
ALTER TABLE users 
ADD CONSTRAINT uniq_user_email UNIQUE (email);

-- Unique user keycloak_id
ALTER TABLE users 
ADD CONSTRAINT uniq_user_keycloak_id UNIQUE (keycloak_id);

-- Unique favorite combination
ALTER TABLE favorites 
ADD CONSTRAINT uniq_favorite_user_station UNIQUE (user_id, station_id);

-- Unique outbox event_id
ALTER TABLE outbox 
ADD CONSTRAINT uniq_outbox_event_id UNIQUE (event_id);
```

### Check Constraints

```sql
-- Valid latitude range
ALTER TABLE stations 
ADD CONSTRAINT chk_station_latitude 
CHECK (latitude >= -90 AND latitude <= 90);

-- Valid longitude range
ALTER TABLE stations 
ADD CONSTRAINT chk_station_longitude 
CHECK (longitude >= -180 AND longitude <= 180);

-- Valid rating range
ALTER TABLE reviews 
ADD CONSTRAINT chk_review_rating 
CHECK (rating >= 1 AND rating <= 5);

-- Positive power output
ALTER TABLE chargers 
ADD CONSTRAINT chk_charger_power_kw 
CHECK (power_kw > 0);

-- Valid version number
ALTER TABLE chargers 
ADD CONSTRAINT chk_charger_version 
CHECK (version >= 1);
```

## Indexes

### Primary Indexes

```sql
-- Primary keys are automatically indexed by PostgreSQL
```

### Foreign Key Indexes

```sql
CREATE INDEX idx_stations_company_id ON stations(company_id);
CREATE INDEX idx_chargers_station_id ON chargers(station_id);
CREATE INDEX idx_favorites_user_id ON favorites(user_id);
CREATE INDEX idx_favorites_station_id ON favorites(station_id);
CREATE INDEX idx_reviews_user_id ON reviews(user_id);
CREATE INDEX idx_reviews_station_id ON reviews(station_id);
CREATE INDEX idx_reviews_moderated_by ON reviews(moderated_by);
CREATE INDEX idx_audit_logs_user_id ON audit_logs(user_id);
```

### Query Optimization Indexes

```sql
-- Soft-delete performance
CREATE INDEX idx_companies_deleted_at ON companies(deleted_at) WHERE deleted_at IS NULL;
CREATE INDEX idx_stations_deleted_at ON stations(deleted_at) WHERE deleted_at IS NULL;
CREATE INDEX idx_chargers_deleted_at ON chargers(deleted_at) WHERE deleted_at IS NULL;

-- Geographic queries
CREATE INDEX idx_stations_location ON stations USING GIST (point(longitude, latitude));

-- Status-based queries
CREATE INDEX idx_chargers_status ON chargers(status);
CREATE INDEX idx_reviews_moderation_status ON reviews(moderation_status);

-- Outbox processing
CREATE INDEX idx_outbox_status_created_at ON outbox(status, created_at);
CREATE INDEX idx_outbox_event_type ON outbox(event_type);

-- Audit log queries
CREATE INDEX idx_audit_logs_entity ON audit_logs(entity_type, entity_id);
CREATE INDEX idx_audit_logs_action_created_at ON audit_logs(action, created_at);
```

## Data Integrity

### Cascade Delete Rules

- Deleting a company CASCADE deletes its stations and their chargers (soft-delete)
- Deleting a station CASCADE deletes its chargers (soft-delete)
- Deleting a user CASCADE deletes their favorites and reviews
- Deleting a station CASCADE deletes its favorites and reviews

### Soft-Delete Behavior

All infrastructure entities (companies, stations, chargers) implement soft-delete:
- `deleted_at` timestamp is set when entity is deleted
- All queries must include `WHERE deleted_at IS NULL` unless explicitly accessing deleted entities
- Cascade soft-delete applies to child entities

### Concurrency Control

All entities that support concurrent updates include a `version` field:
- Version is automatically incremented on each update
- Updates use optimistic locking with version check
- Concurrent modifications result in HTTP 409 Conflict response

### Event Publishing

Domain changes are published through the outbox pattern:
- All domain events are written to the outbox table in the same transaction
- A separate process publishes events to RabbitMQ
- Events are guaranteed to be delivered at-least-once
- Consumers must implement idempotency