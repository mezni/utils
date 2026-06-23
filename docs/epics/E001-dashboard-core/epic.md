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

- Operators (EV network operators)
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
| Operators | PRT-\<nanoid(12)> |
| Stations | STA-\<nanoid(12)> |
| Chargers | CHR-\<nanoid(12)> |

## 3.2 Identity Rules

- `id` is the ONLY system-wide identifier
- `id` is immutable
- `id` is used in API, DB, and frontend
- No UUID or surrogate keys exist

## 3.3 Referential Integrity

Hierarchy:

```
Operators → Stations → Chargers
```

Rules:

- Stations MUST belong to Operators
- Chargers MUST belong to Stations
- Cascading deletes enforced

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

### Operators
- GET /api/v1/operators
- POST /api/v1/operators
- GET /api/v1/operators/{id}

### Stations
- GET /api/v1/stations
- POST /api/v1/stations
- GET /api/v1/stations/{id}

### Chargers
- GET /api/v1/chargers
- POST /api/v1/chargers
- GET /api/v1/chargers/{id}

---

# 5. DATABASE SPECIFICATION

## 5.1 Schema

```
ev
```

## 5.2 Tables

### operators
- id (PRT-xxx PRIMARY KEY)
- name
- created_at

### stations
- id (STA-xxx PRIMARY KEY)
- operator_id (FK → operators.id)
- name
- location
- created_at

### chargers
- id (CHR-xxx PRIMARY KEY)
- station_id (FK → stations.id)
- status
- power_rating
- created_at

## 5.3 Constraints

- id is PRIMARY KEY everywhere
- Foreign keys use id only
- No surrogate keys exist
- Cascading deletes enforced

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

- Total Operators
- Total Stations
- Total Chargers

All derived from API.

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
