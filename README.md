# BorneMap — UX-First EV Charging Platform for Tunisia

**Status:** Pre-MVP-1 (Documentation Complete)  
**Repository:** `/home/dali/WORK/BorneMap`  
**Constitution:** [v1.0](docs/constitution-v1.0.md)  
**Last Updated:** 2026-06-10

---

## Quick Start

This project is in documentation/planning phase. Implementation starts with Session 1 (Infrastructure).

### Key Documents

**Governance & Rules:**
- [Constitution v1.0](docs/constitution-v1.0.md) — Core rules, non-negotiable
- [CLAUDE.md](CLAUDE.md) — Claude Code session contract (for implementation agent)
- [EXECUTION-LOG.md](EXECUTION-LOG.md) — Session tracking, bug tracker, blockers

**Architecture & Design:**
- [API Contract v1.0](docs/api/api-contract.md) — All endpoints, shapes, status codes
- [Architecture Diagram](docs/architecture/bornemaps-architecture.mermaid) — C4 model
- [ADRs](docs/architecture/adr/) — 6 Architecture Decision Records

**Data Models:**
- [platform_db Schema](docs/database/platform-db-schema.md) — Inventory, GIS, Users
- [analytics_db Schema](docs/database/analytics-db-schema.md) — Append-only events

**Implementation Planning:**
- [MVP-1: Discovery Core](docs/mvp/mvp-1-discovery-core.md) — 6-week plan (40+ tasks)
- [MVP-2 through MVP-6](docs/mvp/) — Operational, Identity, Analytics, Performance, Production

---

## Project Overview

**BorneMap** is a map-centric EV charging discovery platform for Tunisia.

### Core Features
- **Mobile-first**: High-performance map experience for drivers
- **Station discovery**: Find nearby chargers (geospatial search, <100ms latency)
- **Partner management**: Charging networks manage infrastructure
- **Admin control**: Platform governance and operations
- **Analytics**: Clickstream events for business intelligence

### Tech Stack

**Backend:**
- Rust + Actix-web (:8080 driver-service, :8081 admin-service)
- PostgreSQL 16 + PostGIS (spatial queries)
- Traefik (API gateway)
- Keycloak (identity management)

**Frontend:**
- Expo SDK 54 (mobile) — React Native + react-native-maps
- React + Leaflet (web driver)
- React + shadcn/ui (admin dashboard)

**Data:**
- platform_db (system of record)
- analytics_db (append-only events)
- keycloak_db (identity, never accessed by services)

---

## Constitutional Rules (Non-Negotiable)

### Architecture
- ✅ All code under `source/` (no runtime code elsewhere)
- ✅ Traefik as API gateway (clients never touch services directly)
- ✅ Two services only: driver-service, admin-service
- ✅ `/api/v1/*` prefix on all endpoints

### Frontend
- ✅ Expo SDK 54 locked (no upgrades without ADR)
- ✅ pnpm only (no npm, no yarn)
- ✅ react-native-reanimated v3 (no core Animated API)
- ✅ Skeleton screens, never spinners
- ✅ Dark mode on every screen from day one

### Data
- ✅ platform_db = source of truth
- ✅ gis schema READ-ONLY (driver-service only)
- ✅ analytics_db APPEND-ONLY (no UPDATE/DELETE)
- ✅ Soft-delete on infrastructure, hard-delete on user data

### UX Pro Max Rules
- ✅ Skeleton screens over spinners (everywhere)
- ✅ Optimistic UI on all backend actions
- ✅ Haptic feedback on primary CTAs
- ✅ Gesture-first (bottom sheets, swipe-to-dismiss)
- ✅ Empty states fully designed (never blank)
- ✅ Error states with recovery actions
- ✅ No map jitter or marker flashing

---

## File Structure

```
bornemaps/
├── CLAUDE.md                      ← Claude Code session contract
├── EXECUTION-LOG.md               ← Session tracking
├── README.md                       ← This file
├── docs/                          ← Non-runtime documentation
│   ├── constitution-v1.0.md
│   ├── architecture/
│   │   ├── bornemaps-architecture.mermaid
│   │   └── adr/
│   │       ├── ADR-001-traefik-as-gateway.md
│   │       ├── ADR-002-rust-actix-services.md
│   │       ├── ADR-003-expo-sdk-54-lock.md
│   │       ├── ADR-004-clickstream-in-admin-service.md
│   │       ├── ADR-005-postgis-spatial-index.md
│   │       └── ADR-006-pnpm-only.md
│   ├── api/
│   │   └── api-contract.md
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
├── source/                        ← ALL RUNTIME CODE GOES HERE
│   ├── mobile-driver/             ← Expo SDK 54 (pending)
│   ├── web-driver/                ← React + Leaflet (pending)
│   ├── dashboard/                 ← React + shadcn/ui (pending)
│   ├── driver-service/            ← Rust/Actix :8080 (pending)
│   └── admin-service/             ← Rust/Actix :8081 (pending)
├── infra/                         ← Infrastructure, non-runtime
│   ├── docker-compose.yml         ← (pending)
│   ├── .env.example               ← (pending)
│   └── migrations/                ← SQL migration files (pending)
│       ├── 001-platform-db-init.sql
│       ├── 002-gis-schema.sql
│       ├── 003-inventory-schema.sql
│       └── 004-analytics-db-init.sql
└── scripts/                       ← Dev tooling (pending)
    ├── seed-tunisia.ts
    └── dev.sh
```

---

## API Endpoints

### Driver Service (:8080)
```
GET  /api/v1/stations                                     # Paginated list
GET  /api/v1/stations/nearby?lat={lat}&lng={lng}&radius={km}  # Radius search
GET  /api/v1/stations/{id}                                # Station detail + chargers
```

### Admin Service (:8081)
```
GET    /api/v1/stations                                   # Partner-scoped list
POST   /api/v1/stations                                   # Create station
PUT    /api/v1/stations/{id}                              # Update station
DELETE /api/v1/stations/{id}                              # Soft-delete

GET    /api/v1/partners                                   # List partners (admin)
POST   /api/v1/partners                                   # Create partner (admin)
PUT    /api/v1/partners/{id}                              # Update partner (admin)

POST   /api/v1/events                                     # Single event
POST   /api/v1/events/batch                               # Batch events (max 100)
```

Full specification: [API Contract v1.0](docs/api/api-contract.md)

---

## Session Progress

**Session 000 (2026-06-10) — Pre-Implementation Planning**
- ✅ Constitution v1.0 finalized
- ✅ API Contract v1.0 documented
- ✅ Architecture diagrams created
- ✅ 6 ADRs written
- ✅ Database schemas designed
- ✅ MVP-1 work plan created (6 weeks, 40+ tasks)
- ✅ Execution log initialized

**Session 001 (Pending) — Infrastructure & Database**
- Docker Compose scaffold (postgres, postgis, traefik)
- platform_db migrations (inventory, gis, users schemas)
- analytics_db setup (raw_events table, append-only rules)
- Seed data (Tunisia test stations)

**Session 002+ (Pending) — Backend, Frontend, Mobile**

See [EXECUTION-LOG.md](EXECUTION-LOG.md) for detailed progress tracking.

---

## Key Concepts

### UX-First Philosophy
Backend exists to serve frontend UX. Every architectural decision prioritizes:
1. Perceived speed (skeleton screens, optimistic UI)
2. Map interaction latency (<100ms nearby search)
3. Mobile-first experience
4. Dark mode quality

### Domain-Driven Design
- **Driver service** owns discovery (read platform_db.inventory, read-only gis)
- **Admin service** owns management (write platform_db.inventory, write analytics)
- Services never cross domain boundaries
- Keycloak owns identity (internal only)

### Immutable Analytics
- Events are append-only (never UPDATE/DELETE)
- Historical data is sacred (enables cohort analysis)
- Raw events feed aggregated tables (future)

### Mobile Stack Lock
- Expo SDK 54 frozen (no upgrades without ADR + stabilization)
- pnpm locked version (frozen-lockfile in CI)
- All dependencies pinned to known-good versions

---

## Development Workflow

### Phase 1: Infrastructure (Week 1)
```bash
cd /home/dali/WORK/BorneMap
docker-compose up -d              # Start postgres, traefik
psql -U borneuser -d platform_db  # Verify connection
# Run migrations: 001, 002, 003, 004
```

### Phase 2: Backend Services (Weeks 2-3)
```bash
cd source/services/driver-service
cargo run

cd source/services/admin-service
cargo run

# Verify: curl http://localhost:8080/api/v1/stations
```

### Phase 3: Frontend & Mobile (Weeks 3-4)
```bash
cd source/front/mobile-driver
pnpm install
pnpm run ios        # or android

cd source/front/dashboard
pnpm install
pnpm run dev
```

### Phase 4: Integration & Testing (Weeks 4-5)
```bash
# E2E tests, performance benchmarks, stabilization
# Map jitter elimination, query optimization, UX polish
```

---

## Key Metrics & SLOs

**Performance Targets:**
- Nearby search: <100ms p95
- Station detail: <50ms p95
- Map rendering (1000+ markers): zero jitter
- Battery impact: <5% per hour

**Quality Targets:**
- 80%+ unit test coverage (backend)
- 100% contract test coverage (API)
- 99.9% uptime (production, MVP-6)
- Zero blank empty states (UX audit)

---

## Success Criteria (MVP-1 Launch)

- [ ] `docker-compose up` starts all services
- [ ] Mobile app installs and runs without crashes
- [ ] Nearby search returns results in <100ms
- [ ] Dark mode works perfectly on all screens
- [ ] All skeleton screens animate smoothly
- [ ] Network errors show contextual recovery
- [ ] All haptic feedback triggers
- [ ] No `Platform.OS` outside MapContainer.tsx
- [ ] All colors from tokens.ts (no hardcoding)
- [ ] Postman collection with 10+ requests
- [ ] README with complete setup instructions

---

## Important Links

- **Constitution:** [docs/constitution-v1.0.md](docs/constitution-v1.0.md)
- **API Specification:** [docs/api/api-contract.md](docs/api/api-contract.md)
- **Database Design:** [docs/database/](docs/database/)
- **Implementation Plan:** [docs/mvp/mvp-1-discovery-core.md](docs/mvp/mvp-1-discovery-core.md)
- **Session Log:** [EXECUTION-LOG.md](EXECUTION-LOG.md)

---

## Questions?

Refer to the **Constitution v1.0** for all platform rules.  
Refer to **ADRs** in `docs/architecture/adr/` for design decisions.  
Refer to **EXECUTION-LOG.md** for session progress and blockers.

---

**Project Lead:** Claude (chat) — Architecture & Planning  
**Implementation Agent:** Claude Code — All runtime code  
**Status:** Documentation Complete, Ready for Development  
**Next Checkpoint:** Session 1 — Infra & Database Live
