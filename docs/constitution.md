# BorneMap — Constitution (v1.0 RESET)

**Version:** 1.0
**Status:** Active
**Last Updated:** 2026-06-10

---

## 1. PURPOSE

BorneMap is a UX-first, map-centric EV charging platform designed for scalable discovery, operational control, and future analytics intelligence.

It enables:

- **Drivers** → discover charging stations via high-performance maps
- **Partners** → manage charging infrastructure (MVP-2+)
- **Admins** → govern system operations (MVP-2+)
- **Analytics systems** → evolve from event capture to intelligence (MVP-4+)

---

## 2. TOOLING & EXECUTION MODEL

### 2.1 Tool-Agnostic Execution

BorneMap is not dependent on any specific AI tool.

Any approved coding agent (e.g. OpenCode or equivalent CLI-based automation system) MAY:
- generate code
- refactor code
- run scaffolding tasks
- modify files under source/

### 2.2 Execution Constraints

All agents MUST obey:
- This Constitution
- ADR decisions
- Repository boundaries

No tool has authority over architecture.

### 2.3 Forbidden Actions (ALL TOOLS)

- Modifying system architecture without ADR
- Changing database structure without migrations
- Breaking service boundaries
- Accessing external systems directly (Keycloak, DBs)
- Writing code outside source/

---

## 3. REPOSITORY ARCHITECTURE

### 3.1 Monorepo Root

```
bornemap/
├── source/                  ← ALL RUNTIME CODE
│   ├── services/            ← Backend microservices
│   │   ├── libs/            ← Shared libraries
│   │   ├── driver-service/  ← Discovery API (8080)
│   │   ├── admin-service/   ← Management API (8081)
│   │   └── clickstream-service/ ← Events API (8082)
│   │
│   ├── frontend/            ← Frontend applications
│   │   ├── mobile-driver/   ← Expo app (primary UX)
│   │   ├── web-driver/      ← React web app
│   │   └── dashboard/       ← Admin UI
│   │
│   └── packages/            ← Shared workspace packages
│       ├── ui/              ← UI primitives
│       ├── design-tokens/   ← Colors, spacing, typography
│       ├── api-contracts/   ← API endpoint contracts
│       ├── types/           ← Shared TypeScript types
│       ├── event-taxonomy/  ← Clickstream event definitions
│       ├── config/          ← Shared env/API config
│       └── utils/           ← Geo, validation, formatting
│
├── docs/                    ← Architecture, ADRs, specs
├── infra/                   ← Docker, Traefik, deployment
├── scripts/                 ← Dev tooling
└── README.md
```

### 3.2 Non-Negotiable Rule

ALL executable code MUST live under `/source`.

Nothing outside /source is runtime.

---

## 4. FRONTEND ARCHITECTURE

### 4.1 Driver Mobile (PRIMARY PRODUCT)

- Expo SDK 54
- Map-first UX
- Gesture-driven interaction model
- UX Rules:
  - skeletons over spinners
  - optimistic UI everywhere
  - haptics for primary actions
  - Reanimated-only animations

### 4.2 Web Driver

- Simplified discovery experience
- Leaflet-based maps
- No feature parity requirement with mobile

### 4.3 Dashboard

- Partner + admin control plane
- CRUD + analytics views
- MVP-2+ only

---

## 5. BACKEND ARCHITECTURE

### 5.1 Services

| Service | Port | Responsibility |
|---|---|---|
| driver-service | 8080 | station discovery |
| admin-service | 8081 | management |
| clickstream-service | 8082 | event ingestion |
| auth-gateway | MVP-3 | identity layer |

### 5.2 API RULE

ALL endpoints MUST follow: `/api/v1/*`

No exceptions.

### 5.3 Architecture Style

- No API gateway
- No BFF
- No orchestration layer
- Direct service access from clients

### 5.4 Backend Stack

- **Language:** Rust (Actix-web)
- **Database:** sqlx (PostgreSQL)
- **Async:** tokio
- **Serialization:** serde
- **IDs:** nanoid prefix format (e.g. `STA-a1b2c3d4e5`)
- **Logging:** tracing (structured JSON)

### 5.5 Repository Layout

All runtime code under `/source`:

```
source/
├── services/    ← backend microservices + shared libs
├── frontend/    ← frontend applications
└── packages/    ← shared workspace packages
```

---

## 6. DATA ARCHITECTURE

### 6.1 Databases

| DB | Purpose |
|---|---|
| platform_db | system of record |
| analytics_db | event stream |
| keycloak_db | identity (managed only) |

### 6.2 platform_db Schemas

- **inventory** (core domain) — partner, station, charger
- **users** (MVP-3) — user_account, profile, favorites, reviews
- **gis** (read-only) — OpenStreetMap derived data

### 6.3 Data Rules

- platform_db is source of truth
- gis is read-only
- analytics_db is append-only
- keycloak_db is never accessed directly

---

## 7. IDENTITY & RBAC

### 7.1 Realms

| Realm | Users |
|---|---|
| bm-drivers | public + registered drivers |
| bm-control | partners + admins |

### 7.2 Roles

- public_driver
- registered_driver
- partner
- admin

### 7.3 JWT Model

```json
{
  "sub": "USR-abc123def456",
  "realm": "bm-drivers",
  "role": "registered_driver",
  "partner_id": "PRT-abc123def456"
}
```

### 7.4 Access Rules

- No frontend → Keycloak direct access
- Authentication always via backend layer
- Partner scoping enforced server-side: `WHERE partner_id = JWT.partner_id`

---

## 8. NETWORK MODEL

### 8.1 MVP-1

```
Clients → Services → Databases
```

### 8.2 Production (MVP-6)

```
Internet → Traefik → Services → Databases
```

### 8.3 Zones

- Edge (Traefik)
- Application (services)
- Data (databases)
- Identity (Keycloak internal)

---

## 9. DATA FLOWS

### 9.1 Discovery Flow

```
Driver App → Driver Service → platform_db → Response
```

### 9.2 Admin Flow

```
Dashboard → Admin Service → platform_db
```

### 9.3 Analytics Flow

```
App → Clickstream Service → analytics_db
```

### 9.4 Authentication Flow

```
Client → Auth Gateway → Keycloak → JWT
```

---

## 10. TESTING STRATEGY

- Backend: unit + integration tests
- Frontend: UX flow validation
- System: contract + E2E discovery flows

---

## 11. MVP MODEL

| MVP | Focus |
|---|---|
| MVP-1 | UX Discovery |
| MVP-2 | Admin + Dashboard |
| MVP-3 | Identity + RBAC |
| MVP-4 | Analytics |
| MVP-5 | Performance |
| MVP-6 | Production infra |

---

## 12. NON-NEGOTIABLE RULES

- all code under /source
- /api/v1/* mandatory
- no API gateway
- platform_db = source of truth
- gis is read-only
- analytics is append-only
- Keycloak is internal only
- services must not overlap domains
- Reanimated-only animations (mobile)
- no hardcoded design tokens in components
- no external DB access from clients
- no infra changes without ADR

---

## 13. DESIGN PRINCIPLES

- UX > complexity
- simplicity before optimization
- domain separation over shared logic
- deterministic MVP evolution
- system evolves from UX pressure, not backend speculation

---

## 14. EXECUTION STRATEGY

### 14.1 Delivery Model

- MVP-driven incremental delivery
- Strict vertical slices (not layer-first)
- Each MVP ends with stabilization sprint
- No cross-MVP feature leakage

### 14.2 Vertical Slice Rule

Each MVP MUST include:
- Backend (service + DB)
- Frontend (app / UI)
- Integration (end-to-end wiring)
- Stabilization (testing + polish)

### 14.3 No Early Implementation

No feature may be implemented before its assigned MVP stage without explicit ADR approval.

### 14.4 Global Execution Flow (per MVP)

```
Design → Backend → Frontend → Integration → Testing → Stabilization
```

Applied per MVP, not globally parallel.

---

## FINAL STATEMENT

BorneMap is a UX-first, domain-isolated EV platform where frontend map experience drives backend evolution, system complexity is introduced only through MVP progression, and execution tools are interchangeable but always constitution-constrained.

---

## ONE-LINE MASTER VIEW

```
Driver App → Driver Service → platform_db (PostGIS)
Dashboard → Admin Service → platform_db
All Apps → Clickstream → analytics_db
Auth → Auth Gateway → Keycloak → JWT
```
