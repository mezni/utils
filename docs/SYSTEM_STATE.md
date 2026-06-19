# SYSTEM_STATE.md
## BorneMap System Architecture State

**Last Updated:** June 2026  
**Status:** Pre-Sprint (Foundation Phase)  
**Version:** 1.0

---

## 1. System Overview

BorneMap is an EV charging station discovery and management platform for the Tunisian market.

### Mission
Fast product validation through iterative delivery under strict architectural constraints.

### Current Phase
**Foundation Phase** — Base infrastructure, governance, and API contracts are defined. No sprints executed yet.

---

## 2. Architecture State

### 2.1 Service Topology (IMMUTABLE)

| Service | Port | Responsibility | Schema Owner | Status |
|---------|------|-----------------|--------------|--------|
| **Auth Service** | :3000 | Keycloak proxy, user sync, token management | `users` | 🔴 Not Started |
| **Driver Service** | :3001 | Station discovery, geospatial queries, caching | `inventory` (read) | 🔴 Not Started |
| **Admin Service** | :3002 | Partner CRUD, station management, analytics | `inventory` (write) | 🔴 Not Started |

### 2.2 Frontend Applications (FIXED)

| App | Framework | Purpose | Status |
|-----|-----------|---------|--------|
| **mobile-driver** | Expo SDK 54 + React Native | Mobile station discovery | 🔴 Not Started |
| **web-driver** | React + Leaflet | Web station discovery | 🔴 Not Started |
| **dashboard** | React + shadcn/ui | Partner/Admin portal | 🔴 Not Started |

### 2.3 Gateway & Identity

| Component | Technology | Responsibility | Status |
|-----------|-----------|-----------------|--------|
| **Traefik** | API Gateway | TLS termination, JWT validation, routing | 🔴 Not Started |
| **Keycloak** | Identity Provider | Single realm (bornemap), 3 clients | 🔴 Not Started |
| **Redis** | Cache Layer | Spatial tile snapshots (Driver Service only) | 🔴 Not Started |

### 2.4 Database State

#### platform_db (PostgreSQL 16 + PostGIS)

| Schema | Purpose | Owner | Status |
|--------|---------|-------|--------|
| **gis** | Raw OSM import tier | Background pipeline | 🔴 Not Started |
| **inventory** | Operational tier (stations, chargers, partners) | Admin Service (write), Driver Service (read) | 🔴 Not Started |
| **users** | User profiles | Auth Service (exclusive) | 🔴 Not Started |

#### Isolated Databases

| Database | Purpose | Status |
|----------|---------|--------|
| **keycloak_db** | Identity metadata | 🔴 Not Started |
| **analytics_db** | Event logging (write-only) | 🔴 Not Started |

---

## 3. API Contract State

### 3.1 OpenAPI Endpoints (Planned)

```
Traefik Gateway Routes:
├── /api/v1/auth/*    → Auth Service (:3000)
├── /api/v1/driver/*  → Driver Service (:3001)
└── /api/v1/admin/*   → Admin Service (:3002)
```

### 3.2 OpenAPI Spec Files

| File | Service | Status |
|------|---------|--------|
| `api/openapi/auth.yaml` | Auth Service | 📋 Pending |
| `api/openapi/driver.yaml` | Driver Service | 📋 Pending |
| `api/openapi/admin.yaml` | Admin Service | 📋 Pending |
| `api/openapi/shared.yaml` | Common DTOs | 📋 Pending |

### 3.3 Data Model Status

- **Entity IDs:** Defined (USR-, OPR-, STA-, CHG- with nanoid(12))
- **DTOs:** 📋 Pending (after OpenAPI design)
- **Validation:** 📋 Pending (Zod/serde)

---

## 4. Technology Stack Approval

### Backend
- ✅ Rust (Actix-web)
- ✅ SQLx (compile-time queries)
- ✅ Cargo workspace (shared crates)

### Frontend
- ✅ React 18+
- ✅ TypeScript strict mode
- ✅ Expo SDK 54 (mobile)
- ✅ Leaflet (web mapping)
- ✅ shadcn/ui (dashboard)
- ✅ Tailwind CSS

### Data & Cache
- ✅ PostgreSQL 16 + PostGIS
- ✅ Redis (spatial cache)
- ✅ SQLx migrations

### Identity
- ✅ Keycloak (single realm: bornemap)
- ✅ Traefik (JWT validation via JWKS)

### Orchestration
- ✅ Docker Compose (only allowed system)

---

## 5. Governance & Enforcement

### 5.1 Authority Hierarchy

1. **Architecture Flowchart** (absolute highest authority)
2. **Constitution** (v1.3)
3. **Guardrails** (v3.0)
4. **OpenAPI Contracts**
5. **Code / Config**

### 5.2 SpecKit CI Enforcement

The following are HARD BUILD FAILURES if violated:
- ❌ Service topology violations
- ❌ Schema isolation breaches
- ❌ OpenAPI-first violations
- ❌ SQLx compile-time validation failures
- ❌ nanoid(12) format violations
- ❌ Frontend API boundary violations

### 5.3 Code Review Gates (6-Category Audit)

Every sprint MUST pass:
1. **Architecture Violations** → No service boundary leaks
2. **OpenAPI Drift** → Contract ↔ Implementation alignment
3. **Database Safety** → No raw SQL, SQLx compliance
4. **Identity Violations** → Correct nanoid format, no hardcoded IDs
5. **Frontend Violations** → No direct API calls, proper routing
6. **Security Violations** → Input validation, trust boundaries

---

## 6. Identity System State

### 6.1 Keycloak Configuration

| Item | Value | Status |
|------|-------|--------|
| **Realm** | bornemap | 📋 Pending Setup |
| **Mobile Client** | mobile-driver-app | 📋 Pending Setup |
| **Web Client** | web-driver-app | 📋 Pending Setup |
| **Dashboard Client** | admin-dashboard | 📋 Pending Setup |

### 6.2 Roles

| Role | Scope | Status |
|------|-------|--------|
| **role:driver** | Public + Registered users | 📋 Pending |
| **role:partner** | Partner/Operator access | 📋 Pending |
| **role:admin** | System administration | 📋 Pending |

### 6.3 Entity Identity System

```
Format: <PREFIX>-nanoid(12)

Examples:
USR-k8F3aZ91LmQx    (User)
OPR-9xQa2Lp0VmZk    (Operator/Partner)
STA-pL91xZk8Qa2m    (Station)
CHG-a1B2c3D4e5F6    (Charger)
```

---

## 7. Shared Code Structure

### 7.1 Backend Rust Crates (source/shared/)

| Crate | Purpose | Status |
|-------|---------|--------|
| **auth-core** | Auth primitives | 📋 Pending |
| **db-models** | Shared SQL models | 📋 Pending |
| **validation** | Input validation rules | 📋 Pending |
| **geo** | GIS utilities | 📋 Pending |
| **error** | Error types & handling | 📋 Pending |
| **utils** | Common utilities | 📋 Pending |

### 7.2 Frontend Packages (packages/)

| Package | Purpose | Status |
|---------|---------|--------|
| **shared-types** | TypeScript types | 📋 Pending |
| **shared-ui** | Reusable UI components | 📋 Pending |
| **shared-hooks** | React hooks | 📋 Pending |
| **api-client** | Generated OpenAPI client | 📋 Pending |
| **auth-client** | Keycloak client wrapper | 📋 Pending |
| **config** | Shared config | 📋 Pending |
| **utils** | Frontend utilities | 📋 Pending |

---

## 8. Dependency Direction (ENFORCED)

```
✅ ALLOWED:
apps → packages → api-client → OpenAPI
services → crates (shared/)
infrastructure → (no dependencies)

❌ FORBIDDEN:
services → apps (HARD BLOCK)
apps → services (HARD BLOCK)
packages → services (HARD BLOCK)
crates → frontend (HARD BLOCK)
```

---

## 9. Sprint Execution State

### 9.1 Completed Sprints
- None (Pre-Sprint Phase)

### 9.2 Active Sprint
- None

### 9.3 Planned Sprints
- SPRINT-001: Auth Service Core Implementation
- SPRINT-002: Driver Service Spatial API
- SPRINT-003: Admin Service Partner Management
- SPRINT-004: Frontend Mobile App
- SPRINT-005: Frontend Web App
- SPRINT-006: Dashboard Portal
- SPRINT-007: Integration & E2E Testing

---

## 10. Known Issues & Blockers

| ID | Issue | Priority | Status |
|----|-------|----------|--------|
| None | System ready for sprint execution | - | ✅ |

---

## 11. Documentation Index

| Document | Purpose |
|----------|---------|
| `architecture.md` | Detailed system architecture |
| `auth-flow.md` | Authentication flow documentation |
| `api-contracts.md` | OpenAPI contract overview |
| `guardrails.md` | Execution standards & rules |
| `roadmap_status.md` | Sprint progress tracking |
| `sprint_backlog.md` | Deferred work & backlog |
| `adr/` | Architecture Decision Records |

---

## 12. Governance Commands

### SpecKit CI Validation
```bash
npm run speckit:validate
```

### Database Migrations
```bash
cargo sqlx migrate run --database-url postgres://...
```

### Generate OpenAPI Client
```bash
npm run generate:api-client
```

---

**Last Review:** June 2026  
**Next Review:** Post-SPRINT-001  
**Reviewer:** System State Initialization
