# BorneMap Constitution
**Version:** 1.0  
**Status:** Active  
**Last Updated:** 2026-06-10

---

## 1. PURPOSE

BorneMap is a **UX-first, map-centric EV charging platform for the Tunisian market** enabling:

- **Drivers** to discover charging stations through a high-performance map experience
- **Partners** to manage charging infrastructure
- **Admins** to govern the platform
- **Analytics** to capture events and evolve toward intelligence

---

## 2. TOOLING MANDATE

| Tool | Role | Scope |
|------|------|-------|
| Claude Code | Primary implementation agent | All code generation, refactoring, file operations |
| Claude (chat) | Architecture, planning, documentation | ADRs, specs, constitutions |
| Impeccable UX/UI | Design | Screens, flows, design tokens |

**Claude Code is the sole code execution tool.** No other code-generation tools are used for runtime artifacts.

---

## 3. CORE ARCHITECTURAL PRINCIPLES

### 3.1 UX-FIRST
UX quality supersedes system complexity. Perceived speed supersedes backend sophistication. **Map interaction latency is a primary KPI.**

### 3.2 DOMAIN-DRIVEN SERVICES
Each service owns a bounded context. Services never query outside their domain.

| Domain | Owner | Responsibility |
|--------|-------|-----------------|
| Discovery | Driver service | Station search, geospatial queries |
| Management + Events | Admin service | Station CRUD, partner management, event ingestion |
| Identity | Keycloak | Authentication, realm management |

### 3.3 TRAEFIK AS API GATEWAY
**All client traffic routes through Traefik.** Clients never call services directly.  
Traefik handles:
- TLS termination
- Domain routing
- Auth middleware
- Rate limiting (future)

### 3.4 SINGLE SOURCE OF TRUTH
- `platform_db.inventory.station` is the authoritative record for all station data
- GIS is derived and read-only
- Analytics is append-only (immutable event log)

### 3.5 VERSIONED API CONTRACT
**All endpoints must be prefixed `/api/v1/`.** No exceptions.

### 3.6 FRONTEND-FIRST PRODUCT MODEL
- Driver mobile app is the primary product surface
- Backend evolves to support UX needs
- Web driver app is secondary
- Dashboard is operational (ops only)

### 3.7 SOURCE-ROOTED CODEBASE
**All runtime code lives under `source/`.** Everything outside `source/` is non-runtime:
- `docs/` — architecture, specs, constitution
- `infra/` — Docker Compose, migrations, env files
- `scripts/` — dev tooling, seed runners

---

## 4. SYSTEM ARCHITECTURE

### 4.1 Traffic Flow
```
Clients → Traefik → Services → Databases
```

### 4.2 Services

| Service | Port | Language | Responsibility |
|---------|------|----------|-----------------|
| Driver service | :8080 | Rust / Actix-web | Station discovery (geospatial) |
| Admin service | :8081 | Rust / Actix-web | Management + event ingestion |
| Auth gateway | — | Keycloak | Identity abstraction |

**Critical:** Clickstream service does not exist. Event ingestion (`/api/v1/events`, `/api/v1/events/batch`) lives in **admin-service**.

### 4.3 Databases

| Database | Engine | Rule |
|----------|--------|------|
| `platform_db` | PostgreSQL 16 + PostGIS | System of record. Never drop/truncate. |
| `analytics_db` | PostgreSQL 16 | Append-only. No UPDATE, no DELETE. |
| `keycloak_db` | PostgreSQL 16 | Never accessed directly. Ever. |

### 4.4 `platform_db` Schemas

| Schema | Owner | Rule |
|--------|-------|------|
| `inventory` | Admin service | Writable by admin-service only |
| `gis` | Driver service | READ-ONLY. Never INSERT/UPDATE/DELETE |
| `users` | Auth gateway | Auth scope only |

---

## 5. MONOREPO STRUCTURE

```
bornemaps/
├── AGENTS.md                      ← Coding agent context
├── source/                        ← ALL runtime code lives here. No exceptions.
│   ├── services/                  ← Rust microservices
│   │   ├── shared/                ← Shared Rust crates (ev-core, ev-auth, ev-db)
│   │   ├── driver-service/        ← Rust / Actix-web :8080
│   │   └── admin-service/         ← Rust / Actix-web :8081
│       └── front/                     ← Mobile and web apps
│       ├── packages/              ← Shared design system, UI kit
│       ├── mobile-driver/         ← Expo SDK 54 app
│       ├── web-driver/            ← React + Leaflet
│       └── dashboard/             ← React + shadcn/ui
├── docs/
│   ├── constitution-v1.0.md       ← This file
│   ├── api-contract.md
│   ├── architecture/
│   │   ├── bornemaps-architecture.mermaid
│   │   └── adr/
│   │       ├── ADR-001-traefik-as-gateway.md
│   │       ├── ADR-002-rust-actix-services.md
│   │       ├── ADR-003-expo-sdk-54-lock.md
│   │       ├── ADR-004-clickstream-in-admin-service.md
│   │       ├── ADR-005-postgis-spatial-index.md
│   │       └── ADR-006-pnpm-only.md
│   ├── database/
│   │   ├── platform-db-schema.md
│   │   └── analytics-db-schema.md
│   └── mvp/
│       ├── mvp-1-discovery-core.md
│       ├── mvp-2-operational.md
│       ├── mvp-3-identity.md
│       ├── mvp-4-analytics.md
│       ├── mvp-5-performance.md
│       └── mvp-6-production.md
├── infra/
│   ├── docker-compose.yml
│   ├── .env.example
│   └── migrations/
│       ├── 001-platform-db-init.sql
│       ├── 002-gis-schema.sql
│       ├── 003-inventory-schema.sql
│       └── 004-analytics-db-init.sql
└── scripts/
    ├── seed-tunisia.ts
    └── dev.sh
```

**Rule:** Runtime code → `source/`. Config, SQL, docs → outside `source/`. Never mix.

---

## 6. MOBILE STACK

### 6.1 SDK Lock
- **Expo SDK: 54** — locked. No upgrades without an ADR.

### 6.2 Package Manager
- **pnpm** — only. Never `npm` or `yarn`.
- If `ERR_PNPM_MINIMUM_RELEASE_AGE_VIOLATION` occurs: `pnpm install --no-frozen-lockfile`

### 6.3 Dependencies

| Package | Version | Notes |
|---------|---------|-------|
| `expo` | — | SDK 54 |
| `react-native-maps` | — | Native map on mobile — only inside `MapContainer.tsx` |
| `leaflet` | — | Web map — only inside `MapContainer.tsx` |
| `react-native-reanimated` | v3 | Only animation library allowed |
| `expo-router` | v3 | File-based routing |
| `react-query` | v5 | All server state |
| `zustand` | v4 | Client UI state only |
| `expo-haptics` | — | Haptic feedback on CTAs |
| `expo-location` | — | GPS |

### 6.4 Map Strategy
- **Mobile** → `react-native-maps`
- **Web** → `Leaflet`
- **Abstraction:** `MapContainer.tsx` is the single platform abstraction
- **Rule:** No `Platform.OS` checks outside `MapContainer.tsx`

### 6.5 Network
- All client traffic routes through **Traefik**
- Local dev: direct IP or Cloudflare Tunnel
- **ngrok is prohibited**

---

## 7. UX MANDATE — PRO MAX STANDARD

The mobile driver app is held to the **highest achievable UX quality.** Every interaction is deliberate, every transition is smooth, every state is designed.

### 7.1 Non-negotiable Rules

1. **Skeleton screens over spinners** — everywhere, no exceptions
2. **Optimistic UI** on all user actions that touch the backend
3. **Haptic feedback** on all primary CTAs (`expo-haptics`)
4. **Gesture-first** — bottom sheets, swipe-to-dismiss, pull-to-refresh
5. **Empty states fully designed** — never a blank screen
6. **Error states with recovery actions** — never raw error strings
7. **Dark mode works on every screen** from day one
8. **Map interaction must never cause marker jitter** or unnecessary re-renders
9. **Animations use `react-native-reanimated` v3 only** — core `Animated` API is prohibited
10. **Route transitions via `expo-router` layout animations only**

### 7.2 Design Token Discipline

- All tokens defined in `source/mobile-driver/design/tokens.ts`
- **No hardcoded colors, spacing, or typography** in any component file
- Dark mode handled via token variants — **no inline `colorScheme` conditionals** in components

---

## 8. API CONTRACT

All endpoints: `/api/v1/*`  
All responses: `Content-Type: application/json`  
All timestamps: ISO 8601 UTC (`2026-06-10T14:30:00Z`)  
All IDs: entity-prefixed nanoid (e.g., `STA-abc123`)

### Driver Service (:8080)
```
GET  /api/v1/stations
GET  /api/v1/stations/nearby?lat={f64}&lng={f64}&radius={f64}
GET  /api/v1/stations/{id}
```

### Admin Service (:8081)
```
GET    /api/v1/stations
POST   /api/v1/stations
PUT    /api/v1/stations/{id}
DELETE /api/v1/stations/{id}

GET    /api/v1/partners
POST   /api/v1/partners
PUT    /api/v1/partners/{id}

POST   /api/v1/events
POST   /api/v1/events/batch
```

**Full request/response shapes:** See `docs/api-contract.md`

---

## 9. AUTHENTICATION & RBAC

Managed by **Keycloak** — internal only.

### 9.1 Realms

| Realm | Users |
|-------|-------|
| `bm-drivers` | Public + registered drivers |
| `bm-control` | Partners + admins |

### 9.2 Roles

- `public_driver` — unauthenticated driver
- `registered_driver` — authenticated driver
- `partner` — infrastructure partner
- `admin` — platform admin

### 9.3 JWT Shape

```json
{
  "sub": "USR-abc123",
  "realm": "bm-drivers",
  "role": "registered_driver",
  "partner_id": "PRT-abc123"
}
```

### 9.4 Rules

- **No frontend → Keycloak direct access**
- **Partner scoping enforced server-side:** `WHERE partner_id = JWT.partner_id`
- **Services validate all JWTs** — no implicit trust

---

## 10. DATA ARCHITECTURE

### 10.1 Inventory Model
```
partner → station → charger
```

### 10.2 Analytics Model
- Append-only events in `analytics_db.raw_events`
- No business joins
- **No UPDATE or DELETE ever**

### 10.3 GIS Model
- OSM-derived data loaded by migration scripts only
- **Read-only** — no service writes to the `gis` schema
- **SRID 4326** (WGS 84) for all geometry storage

---

## 11. ENTITY ID CONVENTION

All IDs use entity-prefixed nanoids:

| Entity | Prefix | Example |
|--------|--------|---------|
| User | USR | `USR-abc123` |
| Partner | PRT | `PRT-abc123` |
| Station | STA | `STA-abc123` |
| Charger | CHR | `CHR-abc123` |
| Operator | OPR | `OPR-abc123` |

**Soft delete** on infrastructure entities only (station, charger, partner).  
**Hard delete** on user-generated content.

---

## 12. NETWORK MODEL

### Local (MVP-1)
```
Clients → Services → Databases
```
Docker Compose, local only.

### Production
```
Internet → Traefik → Services → Databases
```

### Zones

| Zone | Components |
|------|------------|
| Edge | Traefik |
| App | Services |
| Data | Databases |
| Identity | Keycloak (internal) |

---

## 13. SECURITY PRINCIPLES

1. **Zero trust** — all requests validated, no implicit internal trust
2. **Least privilege** — each service accesses only its own schema
3. **No cross-domain DB access** — services never query outside their bounded context
4. **GIS is read-only** — no service writes to `gis` schema
5. **Analytics is append-only** — immutable event log
6. **Keycloak is internal** — never exposed directly to clients

---

## 14. TESTING PRINCIPLES

| Layer | Scope |
|-------|-------|
| Backend | Unit tests, integration tests per service |
| Mobile | UX flow tests, map interaction validation |
| System | Contract tests, E2E discovery flows |

---

## 15. MVP EVOLUTION

| MVP | Focus |
|-----|-------|
| MVP-1 | **Discovery UX** — map, nearby search, station detail, events |
| MVP-2 | **Operational control** — admin dashboard, partner management |
| MVP-3 | **Identity & RBAC** — Keycloak, JWT, realm routing |
| MVP-4 | **Analytics intelligence** |
| MVP-5 | **Performance hardening** |
| MVP-6 | **Production infra** — Traefik, TLS, public exposure |

**Each MVP ends with a mandatory stabilization sprint.** No cross-MVP feature bleed.

---

## 16. NON-NEGOTIABLE RULES

1. ✗ All runtime code lives under `source/`
2. ✗ Expo SDK locked at 54
3. ✗ `/api/v1/*` on every endpoint
4. ✗ All client traffic routes through Traefik
5. ✗ `platform_db` is the single source of truth
6. ✗ `gis` schema is read-only — never written to by services
7. ✗ `analytics_db` is append-only — no UPDATE, no DELETE
8. ✗ `keycloak_db` is never accessed directly
9. ✗ Services must not overlap domain boundaries
10. ✗ `react-native-reanimated` v3 only — core `Animated` API prohibited
11. ✗ No hardcoded design tokens in component files
12. ✗ Claude Code is the sole code execution tool
13. ✗ pnpm only — no npm, no yarn
14. ✗ ngrok is prohibited
15. ✗ Dark mode support from day one
16. ✗ Skeleton screens over spinners — no exceptions
17. ✗ Clickstream service does not exist — events live in admin-service
18. ✗ No `Platform.OS` checks outside `MapContainer.tsx`
19. ✗ Partners cannot self-register — admin creates all partners
20. ✗ Soft delete on infrastructure entities only

---

## FINAL STATEMENT

**BorneMap is a UX-first, domain-isolated EV platform where:**

- Frontend map experience drives backend evolution
- System complexity is introduced through controlled MVP progression
- Claude Code operates as the implementation agent within strict constitutional boundaries
- Every decision is documented as an ADR
- Every session is tracked in `EXECUTION-LOG.md`

### One-line master view:
```
Clients → Traefik → [Driver service | Admin service] → [platform_db | analytics_db | keycloak_db]
```
