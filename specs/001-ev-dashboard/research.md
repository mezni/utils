# Research: EV Dashboard Platform Kernel

**Feature**: EV Dashboard Platform Kernel
**Date**: 2026-06-23
**Branch**: 001-ev-dashboard

## Overview

This document consolidates technical research findings for the EV Dashboard Platform Kernel feature. All NEEDS CLARIFICATION items from the implementation plan have been resolved.

---

## Technology Decisions

### 1. External ID Generation (nanoid)

**Decision**: Use `rust-nanoid` crate for generating unique external IDs

**Rationale**:
- Deterministic nanoid generation from provided strings ensures consistent IDs
- Short (12 characters) and URL-safe
- Collision rate extremely low for this use case
- Supported by Rust ecosystem

**Alternatives Considered**:
- UUID v4: Too long, exposes internal state, not recommended per constitution
- Sequential IDs: Leaks ordering information, not externally stable
- Hash-based IDs: More expensive, potential collision issues

**Implementation**:
```rust
// platform-core/src/id/mod.rs
use nanoid::nanoid;

pub fn generate_partner_id() -> String {
    format!("PRT-{}", nanoid!(12))
}

pub fn generate_station_id() -> String {
    format!("STA-{}", nanoid!(12))
}

pub fn generate_charger_id() -> String {
    format!("CHR-{}", nanoid!(12))
}
```

**References**:
- rust-nanoid crate: https://github.com/niklasf/nanoid-rust
- Requirements: Constitution II. External Identity Model

---

### 2. Database Migration Strategy

**Decision**: SQLx forward-only migrations with timestamp ordering

**Rationale**:
- Forward-only execution matches SQLx best practices
- Timestamp-based ordering prevents dependency conflicts
- No rollback dependency requirements simplify maintenance
- Clear, predictable migration history

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
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- migrations/003_create_stations.sql
CREATE TABLE ev.stations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT,
    partner_id TEXT NOT NULL REFERENCES ev.partners(id) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

**Rules**:
- Migrations are forward-only
- Migrations are timestamp-ordered (001_create_schema.sql, 002_create_partners.sql, etc.)
- No rollback dependency allowed
- Each migration can be applied independently

**References**:
- Constitution IV. Domain Purity (infrastructure handles DB operations)
- Epic specification: Section 5.4 Migration Rules

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
```
GET /api/v1/partners?page=1&limit=50
GET /api/v1/stations?page=2&limit=100&partner_id=PRT-xxx

Response:
{
  "success": true,
  "data": {
    "items": [...],
    "pagination": {
      "page": 1,
      "limit": 50,
      "total": 100,
      "pages": 2
    }
  },
  "error": null
}
```

**Parameters**:
- `page`: Page number (1-indexed, default 1)
- `limit`: Items per page (default 50, max 100)
- `offset`: For advanced filtering, not commonly used

**References**:
- Constitution III. API Contract Compliance (standardized format)
- Epic specification: FR-022, Success Criteria SC-003

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
  create: (data: { name: string }) =>
    apiClient.post('/partners', data),
  get: (id: string) =>
    apiClient.get(`/partners/${id}`),
};

// hooks/usePartners.ts
import { useQuery } from '@tanstack/react-query';
import { partnersApi } from '../api/partners';

export const usePartners = (page: number = 1, limit: number = 50) => {
  return useQuery({
    queryKey: ['partners', page, limit],
    queryFn: () => partnersApi.list(page, limit),
  });
};
```

**References**:
- Constitution IV. Domain Purity (UI must not contain transport logic)
- Constitution V. Test-Driven Development (React Query behavior tests)
- Epic specification: Section 6.3 Frontend Rules

---

### 5. Database Schema Design

**Decision**: Three-table schema with cascading deletes, external IDs as primary keys

**Rationale**:
- Simple, normalized schema
- External IDs meet constitution requirements
- Cascading deletes enforce referential integrity (Partners → Stations → Chargers)
- Created timestamps for audit trail

**Schema**:

```sql
-- partners table
CREATE TABLE ev.partners (
    id TEXT PRIMARY KEY,                    -- PRT-xxx (nanoid 12 chars)
    name TEXT NOT NULL,                     -- Partner name
    created_at TIMESTAMP NOT NULL DEFAULT NOW()  -- Audit field
);

-- stations table
CREATE TABLE ev.stations (
    id TEXT PRIMARY KEY,                    -- STA-xxx (nanoid 12 chars)
    name TEXT NOT NULL,                     -- Station name
    location TEXT,                          -- Station location (optional)
    partner_id TEXT NOT NULL,               -- FK to partners.id
    created_at TIMESTAMP NOT NULL DEFAULT NOW()  -- Audit field
);

-- chargers table
CREATE TABLE ev.chargers (
    id TEXT PRIMARY KEY,                    -- CHR-xxx (nanoid 12 chars)
    station_id TEXT NOT NULL,               -- FK to stations.id
    status TEXT NOT NULL DEFAULT 'active',  -- Charger status
    power_rating INTEGER NOT NULL,          -- Power rating in kW
    created_at TIMESTAMP NOT NULL DEFAULT NOW()  -- Audit field
);
```

**Constraints**:
- All `id` fields are TEXT PRIMARY KEY (external IDs)
- NO surrogate keys (auto-increment integers)
- `partner_id` references `ev.partners(id)` with CASCADE delete
- `station_id` references `ev.stations(id)` with CASCADE delete
- `created_at` for all tables (audit trail)

**Indexes**:
```sql
CREATE INDEX idx_stations_partner_id ON ev.stations(partner_id);
CREATE INDEX idx_chargers_station_id ON ev.chargers(station_id);
```

**References**:
- Constitution II. External Identity Model
- Constitution IV. Domain Purity (domain entities in Rust, not in SQL)
- Epic specification: Section 5.2 Tables

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
    pub created_at: chrono::DateTime<chrono::Utc>,
}

// domain/entities/station.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Station {
    pub id: String,
    pub name: String,
    pub location: Option<String>,
    pub partner_id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}
```

**Infrastructure Layer (SQLx repositories)**:
```rust
// infrastructure/repositories/partner_repository.rs
use sqlx::PgPool;
use domain::entities::partner::Partner;

pub async fn create_partner(
    pool: &PgPool,
    name: String,
) -> Result<Partner, AppError> {
    let id = id::generate_partner_id();
    let partner = Partner { id, name, created_at: Utc::now() };

    sqlx::query(
        "INSERT INTO ev.partners (id, name, created_at)
         VALUES ($1, $2, $3)
         RETURNING *"
    )
    .bind(&partner.id)
    .bind(&partner.name)
    .bind(&partner.created_at)
    .fetch_one(pool)
    .await?
    .map(|row| Partner {
        id: row.get("id"),
        name: row.get("name"),
        created_at: row.get("created_at"),
    })
}
```

**Application Layer (use-cases)**:
```rust
// application/use_cases/create_partner.rs
pub struct CreatePartner {
    pub name: String,
}

pub async fn execute(pool: &PgPool, cmd: CreatePartner) -> Result<Partner, AppError> {
    // Domain invariants
    if cmd.name.is_empty() {
        return Err(AppError::Validation("Name cannot be empty".into()));
    }

    // Call infrastructure
    partner_repository::create_partner(pool, cmd.name).await
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
  list: (params?: { page?: number; limit?: number }) =>
    apiClient.get('/partners', { params }),
  create: (data: { name: string }) =>
    apiClient.post('/partners', data),
  get: (id: string) =>
    apiClient.get(`/partners/${id}`),
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
    queryFn: () => partnersApi.list({ page, limit }),
  });
};

export const useCreatePartner = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (data: { name: string }) => partnersApi.create(data),
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

**References**:
- Constitution II. External Identity Model
- Constitution III. API Contract Compliance
- Constitution IV. Domain Purity
- Constitution V. Test-Driven Development

---

## Performance Considerations

### Database Performance

**Query Optimization**:
- Indexes on foreign keys (`partner_id`, `station_id`)
- Simple COUNT queries for KPIs (no complex aggregation)
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

---

## Security Considerations

### Input Validation

**Backend Validation**:
```rust
// application/use_cases/create_partner.rs
pub struct CreatePartner {
    pub name: String,
}

pub async fn execute(pool: &PgPool, cmd: CreatePartner) -> Result<Partner, AppError> {
    // Validate name
    if cmd.name.trim().is_empty() {
        return Err(AppError::Validation("Name cannot be empty or whitespace".into()));
    }

    if cmd.name.len() > 200 {
        return Err(AppError::Validation("Name cannot exceed 200 characters".into()));
    }

    // Validate character set (alphanumeric, spaces, hyphens)
    if !cmd.name.chars().all(|c| c.is_alphanumeric() || c == ' ' || c == '-') {
        return Err(AppError::Validation("Name can only contain letters, numbers, spaces, and hyphens".into()));
    }

    partner_repository::create_partner(pool, cmd.name).await
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
- Security requirements per constitution

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

    let partner = partner_repository::create_partner(&pool, "Test Partner".into()).await.unwrap();

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
    const mockData = { items: [{ id: 'PRT-xxx', name: 'Test' }] };
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
  it('list calls correct endpoint with pagination', () => {
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
- Epic specification: Section 9 Observability

---

## Conclusion

All technical decisions have been made and documented. The implementation can proceed with:

1. **Backend** (Rust + Actix-Web + SQLx):
   - External ID generation using rust-nanoid
   - Forward-only SQLx migrations with timestamp ordering
   - Offset-based pagination for list endpoints
   - Clean Architecture layers enforced
   - Comprehensive testing strategy

2. **Frontend** (React + TypeScript + React Query):
   - React Query for server state management
   - React Router v6 for routing
   - apiClient for transport layer
   - No direct fetch() calls in components

3. **Database** (PostgreSQL 'ev' schema):
   - Three-table schema with external IDs
   - Cascading deletes (Partners → Stations → Chargers)
   - Proper indexing
   - Audit timestamps

4. **Infrastructure**:
   - Docker containerization
   - Docker Compose orchestration
   - Structured logging
   - Tracing with correlation IDs

**Next Step**: Generate `data-model.md` with detailed entity definitions and `contracts/api.yaml` with OpenAPI specification.
