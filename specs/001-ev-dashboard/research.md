# Research: EV Dashboard Platform Kernel

**Feature**: EV Dashboard Platform Kernel
**Date**: 2026-06-23
**Branch**: 001-ev-dashboard

## Overview

This document consolidates technical research findings for the EV Dashboard Platform Kernel feature. All NEEDS CLARIFICATION items from the implementation plan have been resolved.

---

## Technology Decisions

### 1. Identity Generation (Deterministic)

**Decision**: Use deterministic ID generation from string seed (hash-based nanoid), NOT random nanoid

**Rationale**:
- IDs must be consistent across instances and environments
- Deterministic generation ensures reproducible test scenarios
- Hash-based nanoid ensures consistent IDs from same seed
- Avoids UUID (not allowed per constitution)

**Alternatives Considered**:
- Random nanoid: IDs differ across instances, non-reproducible
- UUID v4: Too long, violates constitution, exposes internal state
- Sequential IDs: Leaks ordering information, not externally stable
- Hash from name: Collision risk, not collision-resistant

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

    // Use hash as deterministic seed for nanoid
    nanoid!(length, &seed_hash.to_string().into_bytes())
}
```

**Usage**:
- Infrastructure layer generates IDs from string seed
- Domain layer receives ID as input (immutable)
- Application layer orchestrates ID generation

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

**References**:
- Constitution II. External Identity Model
- data-model.md (Identity Generation Rules section)
- spec.md (FR-032 to FR-036)

---

### 2. Database Migration Strategy

**Decision**: SQLx forward-only migrations with timestamp ordering, soft delete support

**Rationale**:
- Forward-only execution matches SQLx best practices
- Timestamp-based ordering prevents dependency conflicts
- Soft delete pattern supports data recovery
- No rollback dependency requirements simplify maintenance

**Alternatives Considered**:
- Down migrations: Add complexity, not needed for this use case
- Sequential numbers: Risk of gaps, less flexible
- Git-based: Hard to track state, requires database versioning

**Implementation**:
```sql
-- migrations/001_create_schema.sql
CREATE SCHEMA IF NOT EXISTS ev;

-- migrations/002_create_partners.sql
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

-- migrations/003_create_stations.sql
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

-- migrations/004_create_chargers.sql
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
```

**Rules**:
- Migrations are forward-only
- Migrations are timestamp-ordered (001_create_schema.sql, 002_create_partners.sql, 003_create_stations.sql, 004_create_chargers.sql)
- No rollback dependency allowed
- Each migration can be applied independently

**References**:
- Constitution II. External Identity Model
- data-model.md (Database Schema section)
- spec.md (FR-057)

---

### 3. API Pagination Strategy

**Decision**: Offset-based pagination with query parameters

**Rationale**:
- Simple to implement and understand
- Well-suited for dashboard/list views
- Allows efficient skip-ahead for filtering/sorting
- Industry standard for REST APIs

**Alternatives Considered**:
- Cursor-based: Better for complex filters/sorting, but more complex to implement
- Keyset pagination: Best performance for sorted lists, but doesn't allow random access
- Page-based (page/size): Less efficient for large datasets (multiple OFFSET operations)

**Implementation**:
```sql
-- Get paginated list (only active records)
SELECT * FROM ev.partners
WHERE deleted_at IS NULL
ORDER BY created_at DESC
LIMIT $1 OFFSET $2;

-- Response schema
{
  "success": true,
  "data": {
    "items": [
      {
        "id": "PRT-abc123456789",
        "name": "Example EV Network",
        "status": "ACTIVE",
        "is_valid": true,
        "created_by": "admin-user-id",
        "updated_by": "admin-user-id",
        "created_at": "2026-06-23T10:00:00Z",
        "updated_at": "2026-06-23T10:00:00Z"
      }
    ],
    "pagination": {
      "page": 1,
      "limit": 50,
      "total": 10,
      "pages": 1
    }
  },
  "error": null
}
```

**Parameters**:
- `page`: Page number (1-indexed, default 1)
- `limit`: Items per page (default 50, max 100)

**References**:
- Constitution III. API Contract Compliance
- data-model.md (Query Patterns section)
- spec.md (FR-062 to FR-064)

---

### 4. Frontend State Management

**Decision**: React Query for server state, React Router v6 for routing

**Rationale**:
- React Query is perfect for server state caching, synchronization, and background updates
- React Router v6 provides declarative routing with hooks
- Fits Clean Architecture: React Query wraps apiClient, not directly fetching from components
- Excellent TypeScript support
- Well-tested and production-ready

**Alternatives Considered**:
- Redux Toolkit: Overkill for this use case, requires more boilerplate
- Zustand: Good for client state, but React Query is more suited for server state
- Context API: Too basic, no caching or background updates

**Implementation**:
```typescript
// api/partners.ts
import { apiClient } from './client';

export const partnersApi = {
  list: (page: number = 1, limit: number = 50) =>
    apiClient.get('/partners', { params: { page, limit } }),
  create: (data: { name: string, status?: string, is_valid?: boolean }) =>
    apiClient.post('/partners', data),
  get: (id: string) =>
    apiClient.get(`/partners/${id}`),
  delete: (id: string) =>
    apiClient.delete(`/partners/${id}`),
  soft_delete: (id: string) =>
    apiClient.put(`/partners/${id}`, { deleted_at: new Date().toISOString() }),
  undelete: (id: string) =>
    apiClient.put(`/partners/${id}`, { deleted_at: null }),
};

// hooks/usePartners.ts
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { partnersApi } from '../api/partners';

export const usePartners = (page: number = 1, limit: number = 50) => {
  return useQuery({
    queryKey: ['partners', page, limit],
    queryFn: () => partnersApi.list(page, limit),
  });
};

export const useCreatePartner = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (data: { name: string, status?: string, is_valid?: boolean }) =>
      partnersApi.create(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['partners'] });
    },
  });
};
```

**References**:
- Constitution IV. Domain Purity (UI must not contain transport logic)
- Constitution V. Test-Driven Development (React Query behavior tests)
- data-model.md (Repository Interfaces section)
- spec.md (FR-050 to FR-052)

---

### 5. Cascade Delete Strategy (Hard Delete Only)

**Decision**: Hard delete with database CASCADE, soft delete with no cascade

**Rationale**:
- Hard delete CASCADE: Database-level enforce (ON DELETE CASCADE)
- Soft delete no cascade: Application-level control
- Separation of concerns: Database enforces hard delete, application enforces soft delete
- Consistent with Clean Architecture simplicity
- Avoids complex application-level CASCADE logic

**Rules**:
1. **Hard Delete**: When entity is hard-deleted (DELETE statement), related entities are automatically deleted via database CASCADE
2. **Soft Delete**: When entity is soft-deleted (UPDATE deleted_at), related entities are NOT automatically deleted
3. **Cascade Delete Rule**: CASCADE applies ONLY to hard delete operations
4. **Soft Delete Rule**: Queries filter by `deleted_at IS NULL` for active records only

**Implementation**:
```sql
-- Partners table (CASCADE on hard delete of partner)
CREATE TABLE ev.partners (
    id TEXT PRIMARY KEY,
    partner_id TEXT NOT NULL REFERENCES ev.partners(id) ON DELETE CASCADE,
    -- ...
);

-- Hard delete partner (stations automatically deleted by database CASCADE)
DELETE FROM ev.partners WHERE id = 'PRT-abc123456789';

-- Soft delete partner (stations NOT automatically deleted)
UPDATE ev.partners SET deleted_at = NOW() WHERE id = 'PRT-abc123456789';

-- Stations table (CASCADE on hard delete of station)
CREATE TABLE ev.stations (
    id TEXT PRIMARY KEY,
    station_id TEXT NOT NULL REFERENCES ev.stations(id) ON DELETE CASCADE,
    -- ...
);

-- Hard delete station (chargers automatically deleted by database CASCADE)
DELETE FROM ev.stations WHERE id = 'STA-xyz987654321';

-- Soft delete station (chargers NOT automatically deleted)
UPDATE ev.stations SET deleted_at = NOW() WHERE id = 'STA-xyz987654321';

-- Chargers table (CASCADE on hard delete of charger - no cascade to children)
CREATE TABLE ev.chargers (
    id TEXT PRIMARY KEY,
    station_id TEXT NOT NULL REFERENCES ev.chargers(id) ON DELETE CASCADE,
    -- ...
);

-- Hard delete charger (no cascade to children)
DELETE FROM ev.chargers WHERE id = 'CHR-fee987654321';

-- Soft delete charger (no cascade to children)
UPDATE ev.chargers SET deleted_at = NOW() WHERE id = 'CHR-fee987654321';
```

**Rust Repository Interface**:
```rust
// domain/repositories/partner_repository.rs
#[async_trait]
pub trait PartnerRepository: Send + Sync {
    async fn create(&self, name: String, created_by: String, updated_by: String) -> Result<Partner, AppError>;
    async fn get_by_id(&self, id: String) -> Result<Option<Partner>, AppError>;
    async fn list(&self, page: u32, limit: u32) -> Result<(Vec<Partner>, u64), AppError>;
    async fn hard_delete(&self, id: String) -> Result<(), AppError>;  // CASCADE to stations
    async fn soft_delete(&self, id: String, deleted_by: String) -> Result<(), AppError>;  // No cascade
    async fn undelete(&self, id: String, updated_by: String) -> Result<Partner, AppError>;
}
```

**Rust Service Layer**:
```rust
// application/services/partner_service.rs
impl PartnerService {
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
}
```

**References**:
- data-model.md (Soft Delete Strategy section)
- spec.md (FR-028 to FR-031, FR-068)

---

### 6. Repository Interface Contracts

**Decision**: Explicit repository traits defined in domain layer, implemented in infrastructure layer

**Rationale**:
- Defines clear contracts for Clean Architecture
- Domain layer defines interfaces (abstractions)
- Infrastructure layer implements interfaces (concreteness)
- Enables dependency injection
- Enables testing (mock implementations)

**Implementation**:
```rust
// domain/repositories/partner_repository.rs
#[async_trait]
pub trait PartnerRepository: Send + Sync {
    // Domain layer defines traits only
    async fn create(&self, name: String, created_by: String, updated_by: String) -> Result<Partner, AppError>;
    async fn get_by_id(&self, id: String) -> Result<Option<Partner>, AppError>;
    async fn list(&self, page: u32, limit: u32) -> Result<(Vec<Partner>, u64), AppError>;
    async fn hard_delete(&self, id: String) -> Result<(), AppError>;
    async fn soft_delete(&self, id: String, deleted_by: String) -> Result<(), AppError>;
    async fn undelete(&self, id: String, updated_by: String) -> Result<Partner, AppError>;
}

// domain/repositories/station_repository.rs
#[async_trait]
pub trait StationRepository: Send + Sync {
    async fn create(&self, name: String, location: Option<String>, partner_id: String, created_by: String, updated_by: String) -> Result<Station, AppError>;
    async fn get_by_id(&self, id: String) -> Result<Option<Station>, AppError>;
    async fn list(&self, page: u32, limit: u32, partner_id: Option<String>) -> Result<(Vec<Station>, u64), AppError>;
    async fn hard_delete(&self, id: String) -> Result<(), AppError>;
    async fn soft_delete(&self, id: String, deleted_by: String) -> Result<(), AppError>;
    async fn undelete(&self, id: String, updated_by: String) -> Result<Station, AppError>;
}

// domain/repositories/charger_repository.rs
#[async_trait]
pub trait ChargerRepository: Send + Sync {
    async fn create(&self, station_id: String, status: String, power_rating: i32, created_by: String, updated_by: String) -> Result<Charger, AppError>;
    async fn get_by_id(&self, id: String) -> Result<Option<Charger>, AppError>;
    async fn list(&self, page: u32, limit: u32, station_id: Option<String>) -> Result<(Vec<Charger>, u64), AppError>;
    async fn update_status(&self, id: String, status: String, updated_by: String) -> Result<Charger, AppError>;
    async fn hard_delete(&self, id: String) -> Result<(), AppError>;
    async fn soft_delete(&self, id: String, deleted_by: String) -> Result<(), AppError>;
    async fn undelete(&self, id: String, updated_by: String) -> Result<Charger, AppError>;
}

// infrastructure/repositories/partner_repository_impl.rs
pub struct PartnerRepositoryImpl {
    pool: PgPool,
}

impl PartnerRepository for PartnerRepositoryImpl {
    async fn create(&self, name: String, created_by: String, updated_by: String) -> Result<Partner, AppError> {
        // Implementation using SQLx
        // ...
    }

    async fn hard_delete(&self, id: String) -> Result<(), AppError> {
        // Hard delete with CASCADE
        // Database CASCADE will automatically delete associated stations
        // ...
    }

    async fn soft_delete(&self, id: String, deleted_by: String) -> Result<(), AppError> {
        // Soft delete (no CASCADE)
        // ...
    }
    // ...
}
```

**References**:
- data-model.md (Repository Interfaces section)
- spec.md (FR-047 to FR-049)

---

### 7. Shared Crates Boundaries

**Decision**: Clear separation between platform-core and platform-db

**Rationale**:
- platform-core: Pure utilities only
- platform-db: SQLx operations and repositories
- No business logic in either
- No framework usage in either

**platform-core Scope**:
- Error system (AppError enum)
- Result types (AppResult<T>)
- Configuration management
- ID generation utilities
- Validation helpers
- NO IO operations

**platform-db Scope**:
- SQLx pool management
- Repository implementations
- Migrations
- Database abstractions
- NO business logic

**References**:
- Constitution III. API Contract Compliance (application layer orchestration)
- data-model.md (Repository Interfaces section)
- spec.md (FR-047 to FR-049)

---

### 8. Status Enum Consistency

**Decision**: Unified status enum across all entities (ACTIVE, INACTIVE, MAINTENANCE, DISABLED)

**Rationale**:
- Consistent terminology across all entities
- Easier to understand and maintain
- Easier to filter and query by status
- Reduces cognitive load

**Implementation**:
```rust
// domain/entities/status.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EntityStatus {
    Active,
    Inactive,
    Maintenance,
    Disabled,
}

impl EntityStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            EntityStatus::Active => "ACTIVE",
            EntityStatus::Inactive => "INACTIVE",
            EntityStatus::Maintenance => "MAINTENANCE",
            EntityStatus::Disabled => "DISABLED",
        }
    }

    pub fn from_str(s: &str) -> Result<Self, AppError> {
        match s.to_uppercase().as_str() {
            "ACTIVE" => Ok(EntityStatus::Active),
            "INACTIVE" => Ok(EntityStatus::Inactive),
            "MAINTENANCE" => Ok(EntityStatus::Maintenance),
            "DISABLED" => Ok(EntityStatus::Disabled),
            _ => Err(AppError::Validation(format!("Invalid entity status: {}", s))),
        }
    }
}
```

**Database Implementation**:
```sql
-- All entities include status column
ALTER TABLE ev.partners ADD COLUMN status TEXT NOT NULL DEFAULT 'ACTIVE';
ALTER TABLE ev.stations ADD COLUMN status TEXT NOT NULL DEFAULT 'ACTIVE';
ALTER TABLE ev.chargers ADD COLUMN status TEXT NOT NULL DEFAULT 'ACTIVE';

-- Index for status filtering
CREATE INDEX idx_partners_status ON ev.partners(status);
CREATE INDEX idx_stations_status ON ev.stations(status);
CREATE INDEX idx_chargers_status ON ev.chargers(status);

-- View for active records by status
CREATE VIEW ev.active_partners_status AS
SELECT * FROM ev.partners WHERE deleted_at IS NULL;

CREATE VIEW ev.active_stations_status AS
SELECT * FROM ev.stations WHERE deleted_at IS NULL;

CREATE VIEW ev.active_chargers_status AS
SELECT * FROM ev.chargers WHERE deleted_at IS NULL;
```

**References**:
- data-model.md (Status field definition)
- spec.md (FR-072 to FR-074)

---

## Integration Patterns

### Backend Layer Integration

**Domain Layer (Rust structs)**:
```rust
// domain/entities/partner.rs
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
        // Validation logic
        // ...
        Ok(())
    }

    pub fn is_active(&self) -> bool {
        self.deleted_at.is_none()
    }
}
```

**Infrastructure Layer (SQLx repositories)**:
```rust
// infrastructure/repositories/partner_repository_impl.rs
use sqlx::PgPool;
use domain::entities::partner::Partner;

pub async fn create_partner(
    pool: &PgPool,
    name: String,
    created_by: String,
    updated_by: String,
) -> Result<Partner, AppError> {
    let id = id::generate_partner_id("partner");  // Infrastructure only
    let partner = Partner {
        id,
        name,
        status: "ACTIVE".to_string(),
        is_valid: true,
        created_by,
        updated_by,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        deleted_at: None,
    };

    sqlx::query(
        "INSERT INTO ev.partners (id, name, status, is_valid, created_by, updated_by, created_at, updated_at, deleted_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         RETURNING *"
    )
    .bind(&partner.id)
    .bind(&partner.name)
    .bind(&partner.status)
    .bind(partner.is_valid)
    .bind(&partner.created_by)
    .bind(&partner.updated_by)
    .bind(&partner.created_at)
    .bind(&partner.updated_at)
    .bind(&partner.deleted_at)
    .fetch_one(pool)
    .await?
    .map(|row| Partner {
        id: row.get("id"),
        name: row.get("name"),
        status: row.get("status"),
        is_valid: row.get("is_valid"),
        created_by: row.get("created_by"),
        updated_by: row.get("updated_by"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        deleted_at: row.get::<_, Option<String>>("deleted_at").map(|s| {
            chrono::DateTime::parse_from_rfc3339(&s).ok().unwrap()
        }),
    })
}
```

**Application Layer (use-cases)**:
```rust
// application/use_cases/create_partner.rs
pub struct CreatePartner {
    pub name: String,
    pub created_by: String,
    pub updated_by: String,
}

pub async fn execute(pool: &PgPool, cmd: CreatePartner) -> Result<Partner, AppError> {
    // Domain invariants
    if cmd.name.trim().is_empty() {
        return Err(AppError::Validation("Name cannot be empty".into()));
    }

    // Call infrastructure (ID generation in infrastructure layer)
    let id = id::generate_partner_id("partner");

    // Call repository
    partner_repository::create(pool, cmd.name, cmd.created_by, cmd.updated_by).await
}
```

**Presentation Layer (HTTP handlers)**:
```rust
// presentation/handlers/partners.rs
#[get("/api/v1/partners")]
pub async fn list_partners(
    query: Query<PartnerListParams>,
    get_partners: Arc<GetPartners>,
) -> impl Responder {
    let partners = get_partners.execute(query.into_inner()).await?;
    web::Json(JsonResponse {
        success: true,
        data: partners,
        error: None,
    })
}
```

### Frontend Layer Integration

**Transport Layer (apiClient)**:
```typescript
// api/client.ts
import axios from 'axios';

export const apiClient = axios.create({
  baseURL: import.meta.env.VITE_API_URL || 'http://localhost:8080',
  headers: {
    'Content-Type': 'application/json',
  },
});

// api/partners.ts
import { apiClient } from './client';

export const partnersApi = {
  list: (page: number = 1, limit: number = 50) =>
    apiClient.get('/partners', { params: { page, limit } }),
  create: (data: { name: string, status?: string, is_valid?: boolean }) =>
    apiClient.post('/partners', data),
  get: (id: string) =>
    apiClient.get(`/partners/${id}`),
  delete: (id: string) =>
    apiClient.delete(`/partners/${id}`),
  soft_delete: (id: string) =>
    apiClient.put(`/partners/${id}`, { deleted_at: new Date().toISOString() }),
  undelete: (id: string) =>
    apiClient.put(`/partners/${id}`, { deleted_at: null }),
};
```

**Hooks (React Query)**:
```typescript
// hooks/usePartners.ts
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { partnersApi } from '../api/partners';

export const usePartners = (page: number = 1, limit: number = 50) => {
  return useQuery({
    queryKey: ['partners', page, limit],
    queryFn: () => partnersApi.list(page, limit),
  });
};

export const useCreatePartner = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (data: { name: string, status?: string, is_valid?: boolean }) =>
      partnersApi.create(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['partners'] });
    },
  });
};
```

**Components (UI only)**:
```typescript
// features/partners/list/partner-list.tsx
import { usePartners, useCreatePartner } from '../../../hooks/usePartners';

export const PartnerList = () => {
  const { data, isLoading } = usePartners(1, 50);
  const createPartner = useCreatePartner();

  const handleSubmit = (name: string) => {
    createPartner.mutate({ name });
  };

  return (
    <div>
      {/* Pure UI - no API calls here */}
      <PartnerForm onSubmit={handleSubmit} />
      {isLoading ? <Spinner /> : <PartnerTable partners={data?.items || []} />}
    </div>
  );
};
```

---

## Performance Considerations

### Database Performance

**Query Optimization**:
- Indexes on foreign keys (`partner_id`, `station_id`)
- Indexes on `deleted_at` for soft delete filtering
- Indexes on `status` for status-based queries
- Unique indexes on names
- Simple COUNT queries for KPIs
- Use EXPLAIN ANALYZE for slow queries
- Connection pooling via SQLx

**Caching Strategy**:
- Dashboard KPIs: Cache for 30 seconds (user can tolerate brief staleness)
- Partner list: Client-side caching via React Query
- Station list: Client-side caching via React Query
- Charger list: Client-side caching via React Query

**Database Configuration**:
```yaml
# docker-compose.yml
services:
  postgres:
    command:
      - postgres
      - -c
      - shared_buffers=256MB
      - -c
      - max_connections=100
      - -c
      - work_mem=4MB
```

**References**:
- Success Criteria: SC-003, SC-004
- data-model.md (Database Schema section)

---

## Security Considerations

### Input Validation

**Backend Validation**:
```rust
// application/use_cases/create_partner.rs
pub struct CreatePartner {
    pub name: String,
    pub created_by: String,
    pub updated_by: String,
}

pub async fn execute(pool: &PgPool, cmd: CreatePartner) -> Result<Partner, AppError> {
    // Validate name
    if cmd.name.trim().is_empty() {
        return Err(AppError::Validation("Name cannot be empty".into()));
    }

    if cmd.name.len() > 200 {
        return Err(AppError::Validation("Name cannot exceed 200 characters".into()));
    }

    if !cmd.name.chars().all(|c| c.is_alphanumeric() || c == ' ' || c == '-') {
        return Err(AppError::Validation("Name can only contain letters, numbers, spaces, and hyphens".into()));
    }

    // Domain invariants are enforced in domain layer

    // Call infrastructure
    partner_repository::create(pool, cmd.name, cmd.created_by, cmd.updated_by).await
}
```

**Frontend Validation**:
```typescript
// features/partners/create/partner-form.tsx
const handleSubmit = (e: React.FormEvent) => {
  e.preventDefault();
  const form = e.currentTarget as HTMLFormElement;
  const formData = new FormData(form);
  const name = formData.get('name') as string;

  if (!name.trim()) {
    setError('Name cannot be empty');
    return;
  }

  if (name.length > 200) {
    setError('Name cannot exceed 200 characters');
    return;
  }

  createPartner.mutate({ name });
};
```

### SQL Injection Prevention

**Always use parameterized queries via SQLx**:
```rust
// BAD - never do this
let query = format!("SELECT * FROM ev.partners WHERE name = '{}'", name);
sqlx::query(&query).fetch_one(pool).await?;

// GOOD - use parameterized queries
sqlx::query("SELECT * FROM ev.partners WHERE name = $1")
  .bind(&name)
  .fetch_one(pool)
  .await?;
```

**References**:
- Constitution III. API Contract Compliance (validation on all endpoints)
- spec.md (FR-059 to FR-061, FR-068)

---

## Testing Strategy

### Backend Tests

**Unit Tests (Domain)**:
```rust
// tests/unit/partner_test.rs
#[test]
fn test_create_partner_validates_name() {
    let pool = setup_test_pool().await;
    let create_partner = application::use_cases::create_partner::CreatePartner {
        name: String::new(),  // Empty name
        created_by: "admin-user-id".into(),
        updated_by: "admin-user-id".into(),
    };

    let result = application::use_cases::create_partner::execute(&pool, create_partner).await;
    assert!(matches!(result, Err(AppError::Validation(_))));
}
```

**Integration Tests (API)**:
```rust
// tests/integration/partners_api_test.rs
#[actix_web::test]
async fn test_create_partner_success() {
    let app = test::init_service(app().await).await;

    let payload = serde_json::to_value(json!({
        "name": "Test Partner"
    })).unwrap();

    let req = test::TestRequest::post()
        .uri("/api/v1/partners")
        .set_json(&payload)
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body = test::read_body_json::<JsonResponse<Partner>>(resp).await;
    assert_eq!(body.success, true);
    assert_eq!(body.data.id, /* expected ID format PRT-xxx */);
}
```

**Repository Tests**:
```rust
// tests/repositories/partner_repository_test.rs
#[actix_web::test]
async fn test_partner_repository_create() {
    let pool = setup_test_pool().await;

    let partner = partner_repository::create_partner(&pool, "Test Partner".into(), "admin".into(), "admin".into()).await.unwrap();

    assert_eq!(partner.name, "Test Partner");
    assert!(partner.id.starts_with("PRT-"));
}
```

### Frontend Tests

**Component Tests**:
```typescript
// tests/partners/partner-list.test.tsx
import { render, screen } from '@testing-library/react';
import { PartnerList } from '../partner-list';

describe('PartnerList', () => {
  it('renders loading state', () => {
    render(<PartnerList />);
    expect(screen.getByRole('status', { name: /loading/i })).toBeInTheDocument();
  });

  it('renders partner table when data loaded', async () => {
    const mockData = { items: [{ id: "PRT-xxx", name: "Test" }] };
    render(<PartnerList />);
    // ... test table rendering
  });
});
```

**API Mock Tests**:
```typescript
// tests/api/partners.test.ts
import { partnersApi } from '../partners';

vi.mock('../client', () => ({
  apiClient: {
    get: vi.fn(),
    post: vi.fn(),
    get: vi.fn(),
  },
}));

describe('partnersApi', () => {
  it('list calls correct endpoint with pagination', {
    partnersApi.list(1, 50);
    expect(apiClient.get).toHaveBeenCalledWith('/partners', { params: { page: 1, limit: 50 } });
  });
});
```

**React Query Behavior Tests**:
```typescript
// tests/hooks/usePartners.test.ts
import { renderHook, waitFor } from '@testing-library/react';
import { usePartners } from '../usePartners';

describe('usePartners', () => {
  it('fetches partners on mount', async () => {
    renderHook(() => usePartners(1, 50));
    await waitFor(() => expect(result.current.data).toBeDefined());
  });
});
```

**References**:
- Constitution V. Test-Driven Development (NON-NEGOTIABLE)
- spec.md (FR-060 to FR-067)

---

## Future-Proofing Considerations

### Extensibility

**Easy to add more entities** (e.g., Users, Billing):
- Follow existing patterns
- Add to domain layer first
- Create repository in infrastructure
- Add use-case in application
- Add handler in presentation

**Easy to add more API endpoints**:
- Follow existing API structure
- Use standardized response format
- Apply input validation consistently
- Document in OpenAPI

### Monitoring & Observability

**Structured Logging**:
```rust
// middleware/logging.rs
use tracing::{info, instrument};

#[instrument(skip(req, app))]
pub async fn request_logger(
    req: HttpRequest,
    body: String,
    app: web::Data<App>,
    next: Service<HttpContext>,
) -> impl Responder {
    let request_id = Uuid::new_v4().to_string();
    info!(
        request_id = %request_id,
        method = %req.method(),
        path = %req.path(),
        "Incoming request"
    );

    let resp = next.call(req).await;
    info!(
        request_id = %request_id,
        status = %resp.status(),
        "Request completed"
    );
    resp
}
```

**Tracing with Correlation IDs**:
```rust
// middleware/tracing.rs
use tracing::instrument;

#[instrument(skip(pool, partner_id))]
pub async fn get_partner(
    pool: &PgPool,
    partner_id: String,
) -> Result<Partner, AppError> {
    let partner = partner_repository::get_partner(pool, partner_id).await?;
    Ok(partner)
}
```

**References**:
- Constitution: Observability Law (section 9)
- spec.md (FR-068 to FR-071)

---

## Conclusion

All technical decisions have been made and documented. The implementation can proceed with:

1. **Backend** (Rust + Actix-Web + SQLx):
   - Deterministic ID generation (infrastructure layer)
   - Soft delete with hard delete CASCADE (database-level)
   - Clean Architecture layers enforced
   - Comprehensive testing strategy

2. **Frontend** (React + TypeScript + React Query):
   - React Query for server state management
   - React Router v6 for routing
   - apiClient for transport layer
   - No direct fetch() calls in components

3. **Database** (PostgreSQL 'ev' schema):
   - Three-table schema with external IDs
   - Soft delete with `deleted_at` column
   - Hard delete CASCADE (ON DELETE CASCADE)
   - Proper indexing and constraints
   - Audit fields (created_by, updated_by)

4. **Infrastructure**:
   - Docker containerization
   - Docker Compose orchestration
   - Structured logging
   - Tracing with correlation IDs

**Next Step**: Generate `data-model.md` with detailed entity definitions and `contracts/api.yaml` with OpenAPI specification.
