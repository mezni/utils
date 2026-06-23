# SYSTEM OVERVIEW
## Platform Core Architecture Summary
## Version: 1.0

---

# 0. PURPOSE

This document provides a high-level overview of the platform system defined under Epic E001.

It describes:
- system components
- architectural layout
- data flow model
- module responsibilities
- runtime structure

It does NOT define implementation details or business logic.

---

# 1. SYSTEM DEFINITION

The system is a **modular monolithic dashboard platform kernel** designed for EV infrastructure management.

It is composed of:

- Backend service (`admin-service`)
- Database (`platform_db`)
- Frontend application (`admin-dashboard`)
- Shared system libraries (`platform-core`, `platform-db`)

---

# 2. HIGH-LEVEL ARCHITECTURE

```text id="arch-001"
                ┌──────────────────────────┐
                │   admin-dashboard        │
                │   (React + TS)           │
                └──────────┬───────────────┘
                           │ API (/api/v1)
                           ▼
                ┌──────────────────────────┐
                │   admin-service          │
                │   (Actix-Web / Rust)     │
                └──────────┬───────────────┘
                           │ SQLx
                           ▼
                ┌──────────────────────────┐
                │   platform_db            │
                │   PostgreSQL             │
                └──────────────────────────┘
```

# 3. CORE COMPONENTS

## 3.1 Backend (admin-service)

Responsible for:

- REST API exposure
- business orchestration
- application use-case execution
- enforcing architecture rules

Stack:

- Rust
- Actix-Web
- SQLx

## 3.2 Database (platform_db)

Responsible for:

- persistent storage
- relational integrity
- migration management

Core schema namespace:

```
ev
```

Entities:

- partners
- stations
- chargers

## 3.3 Frontend (admin-dashboard)

Responsible for:

- user interface rendering
- data presentation
- user interactions

Stack:

- React
- TypeScript
- TailwindCSS
- React Query

## 3.4 Shared Crates

### platform-core

Provides:

- error system
- result types
- configuration primitives
- ID utilities

### platform-db

Provides:

- database pool management
- SQLx integration
- repository implementations

---

# 4. DATA MODEL OVERVIEW

## 4.1 Entity Hierarchy

```
Partners → Stations → Chargers
```

## 4.2 Identity Model

The system uses a single external identifier model:

| Entity | id format |
|---|---|
| Partners | PRT-\<nanoid(12)> |
| Stations | STA-\<nanoid(12)> |
| Chargers | CHR-\<nanoid(12)> |

Rules:

- `id` is the only external identifier
- `id` is immutable
- relationships use `id`

---

# 5. API OVERVIEW

## 5.1 Base Path

```
/api/v1
```

## 5.2 Response Model

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

## 5.3 Core Endpoints

- `/dashboard/kpis`
- `/partners`
- `/stations`
- `/chargers`

All endpoints:

- use standardized response format
- use `id` as identifier
- follow `/api/v1` versioning

---

# 6. ARCHITECTURE MODEL

## 6.1 Clean Architecture Layers

```
presentation → application → domain → infrastructure
```

## 6.2 Layer Responsibilities

### Presentation
- HTTP handling
- request validation
- response mapping

### Application
- use-case orchestration
- workflow coordination

### Domain
- business rules
- invariants
- entity logic

### Infrastructure
- database access
- external IO
- persistence implementation

---

# 7. FRONTEND STRUCTURE OVERVIEW

Frontend is organized into:

- pages (routing layer)
- features (business UI logic)
- api (HTTP client layer)
- components (pure UI)

Rules:

- no direct API calls in UI components
- React Query manages server state

---

# 8. DATA FLOW MODEL

```
User Action
   ↓
React Component
   ↓
React Query
   ↓
API Client
   ↓
admin-service
   ↓
application layer
   ↓
domain logic
   ↓
infrastructure (SQLx)
   ↓
PostgreSQL
   ↓
Response returns upward
```

---

# 9. SYSTEM CHARACTERISTICS

The system is designed as:

- modular monolith
- contract-driven backend
- strict layered architecture
- external-ID based identity model
- epic-isolated domain design

---

# 10. OUT OF SCOPE (SYSTEM-WIDE)

The system explicitly does NOT include:

- authentication / RBAC
- billing or payments
- IoT or telemetry systems
- event streaming (Kafka/MQTT)
- microservices architecture
- mobile applications

---

# 11. GUARANTEES

If correctly implemented, the system guarantees:

- predictable architecture boundaries
- stable API contracts
- scalable epic-based extension
- no internal identity leakage
- deterministic data flow
- LLM-consistent code generation structure

---

END OF SYSTEM OVERVIEW
