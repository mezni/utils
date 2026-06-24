# Architecture Rules — BorneMap

**Status**: System of Record
**Version**: 1.0.0
**Date**: 2026-06-24

---

## 1. Repository Topology

```
bornemap/
│
├── source/           # 🚀 ALL EXECUTABLE SYSTEM CODE
│   ├── apps/         # Frontend applications
│   ├── services/     # Backend services (exactly 3)
│   ├── shared/       # Domain kernel (shared-domain, shared-infra)
│   └── packages/     # Frontend ecosystem (ui-kit, domain-types, client-core)
│
├── docs/             # 📚 SYSTEM INTELLIGENCE LAYER
│   ├── governance/   # Constitution + execution rules
│   ├── security/     # Identity + RBAC + trust model
│   ├── contracts/    # API contracts, events, DTOs
│   ├── speckit/      # Sprint execution system
│   ├── architecture/ # Architecture decisions
│   ├── product/      # Product requirements
│   ├── domain/       # Domain models
│   ├── process/      # Process definitions
│   └── adr/          # Architecture Decision Records
│
├── infra/            # 🐳 EXTERNAL SYSTEMS ONLY
│   ├── postgres/
│   ├── keycloak/
│   ├── redis/
│   └── docker/
│
├── .github/workflows/
├── scripts/
├── Cargo.toml        # Rust workspace root
└── package.json      # Frontend workspace root
```

---

## 2. Service Topology

**Invariant**: Exactly 3 services. No more. No fewer.

| Service | Port | Domain | Schema Ownership |
|---------|------|--------|-----------------|
| `auth-service` | 3000 | Authentication + user profiles | `platform_db.users` |
| `driver-service` | 3001 | GIS + telemetry + analytics | `platform_db.gis`, `analytics_db` |
| `admin-service` | 3002 | Inventory + analytics | `platform_db.inventory` |

---

## 3. Clean Architecture (Rust)

### Layer Rules

```
┌──────────────────────────────────────────────┐
│              presentation/                   │
│  HTTP handlers, validation, response mapping │
├──────────────────────────────────────────────┤
│              application/                    │
│  Use-case orchestration, DTO mapping         │
├──────────────────────────────────────────────┤
│              infrastructure/                 │
│  SQLx, Redis, external integrations          │
├──────────────────────────────────────────────┤
│              domain/                         │
│  Pure logic, entities, value objects, errors │
└──────────────────────────────────────────────┘
```

**Dependency direction**: Outer layers depend on inner layers. Never inward.

### Layer Constraints

| Layer | DB Access | HTTP | Framework Deps | Business Logic |
|-------|-----------|------|----------------|----------------|
| domain | ❌ | ❌ | ❌ | ✅ |
| application | ❌ | ❌ | ❌ | ✅ (orchestration) |
| infrastructure | ✅ | ❌ | ✅ (SQLx, Redis) | ❌ |
| presentation | ❌ | ✅ | ✅ (axum/actix) | ❌ |

---

## 4. Frontend Architecture

### Package Dependency Chain

```
ui-kit ──→ domain-types ──→ client-core
```

### Package Responsibilities

| Package | Purpose | Forbidden |
|---------|---------|-----------|
| `ui-kit` | Components, layouts, tokens, accessibility | Business logic, API calls |
| `domain-types` | DTOs, API contracts, event schemas, entity IDs | Runtime logic |
| `client-core` | API clients, React Query, auth/session | Business logic |

### UI State Requirements

Every interaction MUST include 4 states:

1. **Loading** — skeleton/spinner while data loads
2. **Success** — data displayed with confirmation
3. **Error** — clear error message with retry option
4. **Empty** — helpful message when no data exists

No silent failures allowed.

---

## 5. Database Architecture

### platform_db

| Schema | Tables | Owner |
|--------|--------|-------|
| `users` | `user_profiles` | auth-service |
| `gis` | `osm_charging_stations_temp`, `osm_charging_stations` | driver-service |
| `inventory` | (API boundary) | admin-service |

### analytics_db

| Service | Access Level |
|---------|-------------|
| driver-service | READ/WRITE |
| admin-service | READ ONLY |
| auth-service | NO ACCESS |
| Frontend | NEVER ACCESS |

### keycloak_db

- Identity provider storage only
- No application logic
- Owned exclusively by Keycloak

---

## 6. Entity ID Standard

**Format**: `PREFIX-nanoid(12)`

| Entity Type | Prefix | Example |
|-------------|--------|---------|
| Charging Station | `STA` | `STA-k9x2lm8q1v7z` |
| Charger | `CHG` | `CHG-x91kd82m4p0a` |
| Operator | `OPR` | `OPR-abc123def456` |
| Event | `EVT` | `EVT-q8m1z7p3x6n2` |

**Users use Keycloak UUID only.** Never mix.

---

## 7. API Ownership

| Service | Owned Endpoints |
|---------|----------------|
| auth-service | `/api/v1/auth/*`, `/api/v1/users/*` |
| driver-service | `/api/v1/gis/*`, `/api/v1/telemetry/*` |
| admin-service | `/api/v1/inventory/*`, `/api/v1/analytics/*` |

**Shared (allowed everywhere):** `/health`, `/ready`, `/live`, `/metrics`

---

## 8. OSM Importer Architecture

```
/infra/docker/osm-importer/

Dockerfile
requirements.txt
scripts/
├── import.sh              # Entrypoint: download + orchestrate
└── parse_and_import.py    # ETL: parse PBF → staging → curated
```

**Pipeline:**
```
OSM PBF → osmium tags-filter → GeoJSON → staging INSERT → curated INSERT
```

**Constraints:**
- Standalone batch container (no daemon)
- No dependency on backend services
- Idempotent (ON CONFLICT DO NOTHING)
- Ephemeral: exits after completion

---

## 9. Dependency Graph

### Frontend
```
ui-kit (no deps)
   ↓
domain-types (no deps)
   ↓
client-core (depends on domain-types)
```

### Backend
```
shared-domain (no deps)
   ↓
shared-infra (depends on shared-domain)

services depend on both shared-domain and shared-infra
```

### Forbidden Edges
```
service-A → service-B (direct import)
frontend → backend (direct import)
circular dependencies (any direction)
```

---

## 10. Trust Boundaries

```
 Client ──→ Traefik ──→ Service ──→ Database
   │                      │
   │                      ├── Keycloak (JWT validation)
   │                      └── External APIs
```

**Trust decay**: Every boundary crossing reduces trust.
- Client input: ZERO trust
- Service-to-service: LOW trust
- Database: TRUSTED (SQLx validated)
- Keycloak JWT: TRUSTED

---

## 11. Migration Strategy

### Schema → Owner Mapping

| Schema | Owner Service | Migration Path |
|--------|--------------|----------------|
| `users` | auth-service | `migrations/platform_db/users/` |
| `gis` | driver-service | `migrations/platform_db/gis/` |
| `inventory` | admin-service | `migrations/platform_db/inventory/` |

### Rules
- Forward-only migrations
- No destructive rollback
- SQLx compile validation required
- CI must verify migration integrity
- `IF NOT EXISTS` / `OR REPLACE` for idempotency

---

## 12. Sprint Execution Flow

```
PHASE 0: Documentation First (write spec to /docs/speckit/sprints/<id>/spec.md)
PHASE 1: Spec (refine requirements, validate constitution)
PHASE 2: Plan (architecture, service impact, DB changes)
PHASE 3: Tasks (atomic tasks with deps, validation rules)
         ↓
         [GIT BRANCH: sprint/<id>-<short-title>]
         ↓
PHASE 4: Implementation (task-by-task, no scope deviation)
PHASE 5: Validation (SQLx, tests, security)
PHASE 6: Delivery (SYSTEM_STATE.md, sprint_review.md, follow_up.md)
```

---

## 13. Hard Stop Conditions

Execution MUST stop immediately if:

| Condition | Rule Source |
|-----------|-------------|
| Spec not documented first | §PHASE 0 |
| Branch not created before implementation | §BRANCH RULE |
| New service introduced | §2.1 Service Count |
| SQLx validation fails | §11 SQLx Rule |
| Architecture boundary violated | §3 Clean Architecture |
| UX/UI PRO MAX rules violated | §4 Frontend Rules |
| Identity separation broken | §2.3 Identity Dual-System |
| Cross-service DB write | §4 Data Ownership |
| Scope expansion without approval | §Speckit System |
