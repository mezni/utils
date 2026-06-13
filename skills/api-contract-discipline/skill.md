# API Contract Discipline Skill — BorneMap

## Purpose
Prevent API rot through inconsistent DTOs, frontend/backend mismatches, and silent breaking changes.

---

## Core Philosophy

**API = Contract, Not Implementation Detail**

Every API endpoint must be versioned, typed, and documented before implementation.

---

## 🔒 Core Rules

### 1. /api/v1/* Strictness

**ALL endpoints MUST follow the versioned path:**

```rust
// ✅ CORRECT
GET /api/v1/stations
GET /api/v1/stations/nearby
GET /api/v1/stations/{id}

// ❌ WRONG
GET /stations
GET /api/v1/stations?lat=...  // Optional params in path
GET /api/v1/stations/{id}/details  // Additional nesting
```

**Enforcement:**
- No unversioned routes allowed
- Version must be in path, not query parameter
- Version changes require migration plan

---

### 2. Typed Responses

**Every endpoint must reference a TypeScript type:**

```rust
// ✅ CORRECT
// @bm/api-client/getStations.ts
export async function getStations(): Promise<GetStationsResponse> {
  const response = await fetch('/api/v1/stations');
  return response.json();  // TypeScript validates
}

// ❌ WRONG
export async function getStations() {
  const response = await fetch('/api/v1/stations');
  return response.json();  // No type safety!
}
```

**Required Type Definition:**
```typescript
// @bm/types/api.ts
export interface GetStationsResponse {
  stations: Station[];
  pagination: {
    limit: number;
    offset: number;
    total: number;
  };
}
```

---

### 3. No Untyped Responses

**All API responses must be strictly typed:**

```rust
// ✅ CORRECT
#[derive(Serialize)]
pub struct GetStationsResponse {
    pub stations: Vec<StationDto>,
    pub pagination: PaginationDto,
}

// ❌ WRONG
#[derive(Serialize)]
pub struct GetStationsResponse {
    // Missing required fields
    pub stations: Vec<StationDto>,
    // Missing pagination field
}
```

**Requirements:**
- All fields must be typed
- No `Option<T>` without clear documentation
- No ad-hoc JSON shapes
- No "temporary fields" without deprecation plan

---

### 4. Response Shapes Must Match @bm/types

**Single source of truth:**

```rust
// backend: driver-service
// @bm/types/api.ts
export interface StationDto {
  id: string;
  name: string;
  latitude: number;
  longitude: number;
  status: 'active' | 'inactive' | 'maintenance';
}

// ✅ CORRECT - Backend matches TypeScript
// Rust: All fields serialized exactly as defined
// TypeScript: All fields expected exactly as defined

// ❌ WRONG - Mismatch
// Rust: { id: "STA-001", name: "Station", lat: 36.8 }
// TypeScript: expects { id: string, name: string, latitude: number }
```

**Verification Steps:**
1. TypeScript interface defined first
2. Rust struct matches interface
3. All fields present and typed
4. All types match exactly

---

### 5. No Optional Fields Without Versioning

**Versioning rule for optional fields:**

```rust
// MVP-1: Only required fields
export interface GetStationsResponse {
  stations: StationDto[];  // All fields required
}

// MVP-2: Add optional fields via new version
// API v1.1:
export interface GetStationsResponse {
  stations: StationDto[];  // Required
  has_more?: boolean;      // Optional (new)
}

// ❌ WRONG - Optional fields in MVP-1
export interface GetStationsResponse {
  stations: StationDto[];
  has_more?: boolean;  // Should be MVP-2 only
}
```

**Versioning Pattern:**
- MVP-1: Only required fields
- New features: New version
- Deprecate old fields via ADR
- Never change existing fields

---

### 6. No Breaking Changes Without ADR

**Every breaking change requires:**

1. **Architecture Decision Record (ADR)**
2. **Migration plan**
3. **Version bump**
4. **Deprecation timeline**

**ADR Requirements:**
```markdown
# ADR-001: Add optional fields to GetStationsResponse

## Status
Proposed | Accepted | Deprecated | Superseded

## Context
MVP-1 needs optional pagination fields

## Decision
Add optional fields in API v1.1

## Rationale
Better UX for paging

## Consequences
Breaking change for MVP-1 consumers
Requires migration

## Alternatives Considered
❌ Add to v1 (rejected: breaking change)
❌ Remove field instead (rejected: backward incompatible)

## Migration Plan
1. Release v1.1
2. Support both v1 and v1.1 for 2 weeks
3. Deprecate v1
4. Remove v1
```

---

## 📋 Endpoint Verification Checklist

**Before implementing ANY endpoint:**

- [ ] TypeScript interface defined in @bm/types
- [ ] TypeScript interface matches API contract spec
- [ ] Rust struct matches TypeScript interface
- [ ] All fields are typed (no ad-hoc shapes)
- [ ] Versioned path (/api/v1/*) used
- [ ] No unversioned routes
- [ ] No optional fields in MVP-1
- [ ] Breaking changes require ADR
- [ ] Migration plan documented
- [ ] API documentation updated

---

## 🔒 API Contract Discipline Rules

### Request Validation

**All inputs must be validated:**
```rust
// ✅ CORRECT - Handler validates
pub async fn handle_get_stations(
    Query(query_params): Query<GetStationsQuery>,
) -> Result<ApiResponse<GetStationsResponse>, ApiError> {
    // Validate limits
    if query_params.limit > 100 {
        return Err(ApiError::BadRequest("Limit too high".into()));
    }

    // Proceed with validated query
    let result = stations_service::get_all(query_params).await?;
    Ok(ApiResponse::new(result))
}

// ❌ WRONG - No validation
pub async fn handle_get_stations(
    Query(query_params): Query<GetStationsQuery>,
) -> Result<ApiResponse<GetStationsResponse>, ApiError> {
    // ❌ No validation of limits!
    stations_service::get_all(query_params).await
}
```

### Response Validation

**Responses must be strictly shaped:**
```rust
// ✅ CORRECT - Response matches type
#[derive(Serialize)]
pub struct GetStationsResponse {
    pub stations: Vec<StationDto>,
    pub pagination: PaginationDto,
}

// All fields required, all typed

// ❌ WRONG - Inconsistent response
pub struct GetStationsResponse {
    pub stations: Vec<StationDto>,
    pub pagination: Option<PaginationDto>,  // ❌ Optional without versioning
    pub limit: u32,  // ❌ Duplicate with pagination.limit
    pub offset: u32,  // ❌ Duplicate with pagination.offset
}
```

---

## 📊 API Contract Consistency Rules

### Domain Model Consistency

```rust
// ✅ CORRECT - Single domain model
// @bm/types/api.ts
export interface StationDto {
  id: string;
  name: string;
  latitude: number;
  longitude: number;
  status: 'active' | 'inactive' | 'maintenance';
}

// backend: driver-service
#[derive(Serialize)]
pub struct StationDto {
    pub id: String,
    pub name: String,
    pub latitude: f64,
    pub longitude: f64,
    pub status: String,  // Must match enum values
}
```

### Versioning Strategy

```rust
// v1: MVP-1
GET /api/v1/stations
GET /api/v1/stations/nearby
GET /api/v1/stations/{id}

// v1.1: MVP-2 additions
GET /api/v1/stations (extended with optional fields)
GET /api/v1/stations/filtered
POST /api/v1/stations

// Never change v1
```

---

## 🚫 Forbidden Patterns

### 1. Untyped Responses
```rust
// ❌ WRONG
pub async fn handle_get_stations() -> Result<serde_json::Value, Error> {
    // Returns raw JSON, no type safety
}
```

### 2. Ad-hoc JSON Shapes
```rust
// ❌ WRONG
pub async fn handle_get_stations() -> Result<serde_json::Value, Error> {
    // Creates ad-hoc JSON structure
    let json = json!({
        "data": stations,  // ❌ No type safety
        "meta": pagination,  // ❌ No type safety
    });
    Ok(json)
}
```

### 3. "Temporary Fields"
```rust
// ❌ WRONG
pub struct GetStationsResponse {
    pub stations: Vec<StationDto>,
    pub _temp_field: bool,  // ❌ Temporary without deprecation plan
}
```

### 4. Breaking Changes Without ADR
```rust
// ❌ WRONG - Breaking change without ADR
pub struct GetStationsResponse {
    pub stations: Vec<StationDto>,
    pub total: u64,  // ❌ Changed field type
}
```

---

## 🎯 API Contract Discipline Checklist

**Before Implementing API Endpoint:**

- [ ] TypeScript interface defined in @bm/types
- [ ] Interface matches API contract specification
- [ ] Rust struct matches TypeScript interface exactly
- [ ] All fields are typed (no ad-hoc shapes)
- [ ] Versioned path (/api/v1/*) used
- [ ] No unversioned routes
- [ ] No optional fields in MVP-1
- [ ] Breaking changes documented in ADR
- [ ] Migration plan created
- [ ] API documentation updated
- [ ] @bm/api-client implementation created
- [ ] TypeScript types updated
- [ ] Tests written for contract validation

---

## 🔄 API Contract Enforcement

### Pre-Implementation Check

**Before writing ANY API code:**

1. **Define TypeScript Interface:**
   ```typescript
   // First, define the contract
   export interface GetStationsResponse {
     stations: StationDto[];
     pagination: PaginationDto;
   }
   ```

2. **Verify Interface Completeness:**
   - All fields required?
   - All types correct?
   - All enums properly defined?

3. **Create Rust Struct:**
   ```rust
   // Then, create matching struct
   #[derive(Serialize)]
   pub struct GetStationsResponse {
       pub stations: Vec<StationDto>,
       pub pagination: PaginationDto,
   }
   ```

4. **Verify Type Matching:**
   - Field names match?
   - Types match?
   - Enums match?

5. **Create API Implementation:**
   ```rust
   pub async fn handle_get_stations(
       Query(params): Query<GetStationsQuery>,
   ) -> Result<ApiResponse<GetStationsResponse>, ApiError> {
       // Handler maps to service, which uses repository
       stations_service::get_all(params).await
           .map_err(|e| map_error(e))
   }
   ```

6. **Create API Client:**
   ```typescript
   export async function getStations(): Promise<GetStationsResponse> {
     const response = await fetch('/api/v1/stations');
     return response.json();
   }
   ```

---

## 📊 API Contract Consistency Metrics

### Type Safety Score
- 100% = All responses typed
- 80% = Most responses typed
- <80% = Type safety issues

### Contract Compliance Score
- 100% = All endpoints follow rules
- 80% = Most endpoints follow rules
- <80% = Contract violations

### Breaking Change Frequency
- 0 = No breaking changes
- 1-2 = Acceptable
- >2 = Architecture drift risk

---

## 🚦 Enforcement Rules

**When violations detected:**

1. **Untyped Response:** Require TypeScript interface
2. **Breaking Change:** Require ADR + migration plan
3. **Missing Validation:** Require input validation
4. **Contract Mismatch:** Require refactoring

**Stop Execution If:**
- Untyped responses exist
- Breaking changes without ADR
- Contract drift detected
- Versioning violations

---

*This skill prevents API rot and ensures frontend/backend alignment through strict contract discipline.*