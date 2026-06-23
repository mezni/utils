# E001 — DASHBOARD CORE
## Full System Specification (FINAL CONSOLIDATED)
## Version: 1.0

---

# 0. EPIC PURPOSE

E001 defines the foundational platform kernel for an EV infrastructure dashboard system.

It establishes:

- backend service (admin-service)
- frontend application (admin-dashboard)
- database (platform_db)
- shared crates (platform-core, platform-db)
- strict architectural and identity governance rules

This epic is non-decomposable and foundational. All future epics depend on it.

---

# 1. SYSTEM SCOPE

## 1.1 Included Domains

- Partners (EV network operators)
- Stations (charging locations)
- Chargers (charging units)

## 1.2 Explicit Exclusions

- Authentication / RBAC
- Billing / payments
- IoT / telemetry
- Event streaming (Kafka/MQTT)
- Microservices architecture
- Mobile applications

---

# 2. SYSTEM ARCHITECTURE

## 2.1 High-Level Architecture

```
admin-dashboard (React)
        ↓
admin-service (Rust / Actix-Web)
        ↓
platform_db (PostgreSQL)
```

## 2.2 Clean Architecture Model

```
presentation → application → domain → infrastructure
```

### Layer Responsibilities

| Layer | Responsibility |
|---|---|
| presentation | HTTP / UI interface |
| application | use-case orchestration |
| domain | business rules + invariants |
| infrastructure | persistence + external IO |

## 2.3 Dependency Rule

- Dependencies MUST flow inward only
- Domain MUST NOT depend on any framework
- Infrastructure MUST NOT contain business logic

---

# 3. IDENTITY SYSTEM (CRITICAL)

## 3.1 External Identity Model

The system uses ONLY external identifiers:

| Entity | id format |
|---|---|
| Partners | PRT-\<hash-nanoid(12)> |
| Stations | STA-\<hash-nanoid(12)> |
| Chargers | CHR-\<hash-nanoid(12)> |

## 3.2 Identity Rules

- `id` is the ONLY system-wide identifier
- `id` is immutable
- `id` is used in API, DB, and frontend
- No UUID or surrogate keys exist
- IDs are deterministic (hash-based nanoid from seed, infrastructure layer only)
- Format: ENTITY-{12 chars} (e.g., PRT-abc123456789)

## 3.3 Status Enum

Unified across all entities:

- ACTIVE - Record is active and visible
- INACTIVE - Record is inactive
- MAINTENANCE - Record is under maintenance
- DISABLED - Record is disabled

Default: ACTIVE

## 3.3 Referential Integrity

Hierarchy:

```
Partners → Stations → Chargers
```

Rules:

- Stations MUST belong to Partners
- Chargers MUST belong to Stations
- Cascading deletes enforced for HARD deletes only
- Soft deletes do NOT cascade

---

# 4. API SPECIFICATION

## 4.1 Base Path

```
/api/v1
```

## 4.2 Standard Response Contract

### Success
```json
{
  "success": true,
  "data": {},
  "error": null
}
```

### Error
```json
{
  "success": false,
  "data": null,
  "error": {
    "code": "ERROR_CODE",
    "message": "Human readable message"
  }
}
```

## 4.3 API Rules

- All endpoints MUST use standard response format
- No raw framework responses allowed
- Only `id` is exposed externally
- Versioning is mandatory (/api/v1)

## 4.4 Core Endpoints

### Dashboard
- GET /api/v1/dashboard/kpis

### Partners
- GET /api/v1/partners
- POST /api/v1/partners
- GET /api/v1/partners/{id}
- DELETE /api/v1/partners/{id} (hard delete)
- PUT /api/v1/partners/{id} (soft delete/undelete)

### Stations
- GET /api/v1/stations
- POST /api/v1/stations
- GET /api/v1/stations/{id}
- DELETE /api/v1/stations/{id} (hard delete)
- PUT /api/v1/stations/{id} (soft delete/undelete)

### Chargers
- GET /api/v1/chargers
- POST /api/v1/chargers
- GET /api/v1/chargers/{id}
- DELETE /api/v1/chargers/{id} (hard delete)
- PUT /api/v1/chargers/{id} (soft delete/undelete)

---

# 5. DATABASE SPECIFICATION

## 5.1 Schema

```
ev
```

## 5.2 Tables

### partners
- id (PRT-xxx PRIMARY KEY)
- name
- status (enum: ACTIVE, INACTIVE, MAINTENANCE, DISABLED)
- is_valid (boolean)
- created_by (FK → admins.id)
- created_at
- updated_by (FK → admins.id)
- updated_at
- deleted_at

### stations
- id (STA-xxx PRIMARY KEY)
- partner_id (FK → partners.id)
- name
- location
- status (enum: ACTIVE, INACTIVE, MAINTENANCE, DISABLED)
- created_by (FK → admins.id)
- created_at
- updated_by (FK → admins.id)
- updated_at
- deleted_at

### chargers
- id (CHR-xxx PRIMARY KEY)
- station_id (FK → stations.id)
- status (enum: ACTIVE, INACTIVE, MAINTENANCE, DISABLED)
- power_rating (kW)
- created_by (FK → admins.id)
- created_at
- updated_by (FK → admins.id)
- updated_at
- deleted_at

## 5.3 Constraints

- id is PRIMARY KEY everywhere
- Foreign keys use id only
- No surrogate keys exist
- Cascading deletes enforced for HARD DELETE only (ON DELETE CASCADE)
- Soft deletes do NOT cascade (children remain active)
- All queries MUST filter by deleted_at IS NULL for active records
- Audit fields (created_by, updated_by) reference admins table (external dependency)
- Status enum consistent across all entities (ACTIVE, INACTIVE, MAINTENANCE, DISABLED)

## 5.4 Migration Rules

- SQLx migrations
- forward-only execution
- deterministic ordering
- timestamp-based

---

# 6. FRONTEND SPECIFICATION

## 6.1 Stack

- React
- TypeScript
- TailwindCSS
- React Router
- React Query

## 6.2 Route Structure

- /dashboard
- /data/partners
- /data/stations
- /data/chargers
- /users (scaffold)
- /settings (scaffold)

## 6.3 Frontend Rules

- No direct HTTP calls in components
- All API calls via apiClient
- React Query is mandatory for server state
- UI must not contain transport logic

## 6.4 KPI Metrics

Dashboard displays:

- Total Partners
- Total Stations
- Total Chargers

All derived from API (only active records where deleted_at IS NULL).

---

# 7. BACKEND SPECIFICATION

## 7.1 Service

- Rust
- Actix-Web
- SQLx

## 7.2 Architecture Enforcement

```
presentation → application → domain → infrastructure
```

## 7.3 Layer Rules

- presentation: HTTP only
- application: use-cases only
- domain: business logic only
- infrastructure: DB/IO only

---

# 8. SHARED CRATES

## 8.1 platform-core

- error system
- result types
- config utilities
- ID utilities

NO IO allowed.

## 8.2 platform-db

- SQLx pool
- migrations
- repository implementations

NO business logic allowed.

---

# 9. OBSERVABILITY

## 9.1 Logging

- structured logging required
- request_id mandatory
- tracing enabled

## 9.2 Tracing

- correlation ID required
- propagation across all layers

---

# 10. ERROR MODEL

## 10.1 Format

```json
{
  "code": "ERROR_CODE",
  "message": "message"
}
```

## 10.2 Standard Codes

- VALIDATION_ERROR
- NOT_FOUND
- CONFLICT
- INTERNAL_ERROR
- DB_ERROR

---

# 11. GOVERNANCE COMPLIANCE

E001 MUST comply with:

- docs/core/constitution.md
- docs/core/architecture.md
- docs/core/api-standards.md
- docs/core/data-modeling.md

---

# 12. AUDIT & ADMIN DEPENDENCY

## 12.1 Admin Dependency

- **admins table exists in separate system module** (no auth system in scope for E001)
- Audit fields reference admins table: `created_by`, `updated_by`
- Responsibility tracking enabled for data lineage

## 12.2 Delete Operations

### Hard Delete (CASCADE)

- DELETE endpoints remove records from database
- CASCADE delete removes all related children (e.g., deleting partner removes all stations)
- Permanently removed, cannot be recovered

### Soft Delete (NO CASCADE)

- PUT endpoints set `deleted_at` timestamp
- Children NOT automatically deleted (stations remain, chargers remain)
- Records preserved in database for auditing/recovery
- All queries filter by `deleted_at IS NULL`

### Undelete

- PUT endpoint removes `deleted_at` timestamp
- Records become active again
- No CASCADE applied on undelete

---

# 12. SYSTEM GUARANTEES

If implemented correctly, E001 guarantees:

- strict architectural boundaries
- deterministic API behavior
- stable identity model (id-only system)
- scalable epic foundation
- zero cross-layer contamination
- LLM-predictable code generation

---

END OF EPIC E001
