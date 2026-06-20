# BorneMap Project Constitution
**Version:** 1.4
**Date:** June 2026
**Status:** Approved / Core Reference
**Supersedes:** v1.3

---

## 1. Project Identity & Mission

**Name:** BorneMap

**Mission:** EV charging station discovery and management platform for the Tunisian market.

**Optimization Objective:** Fast product validation through iterative delivery under strict architectural constraints.

**Monorepo Root:** `bornemap/` (repository root — no `source/` prefix)

### System Access Profiles

| Actor | Capability |
|---|---|
| Public Users | Discover and browse charging stations on map (unauthenticated) |
| Drivers (Registered) | Save favorites, personalize experience |
| Partners | Manage their stations and infrastructure entries |
| Administrators | Maintain infrastructure integrity and spatial accuracy |

### Validation-Phase Exclusions

The platform SHALL NOT include during the validation phase:

- OCPP integration, charger hardware communication, or session signaling
- Payment processing, billing, or invoicing systems
- Smart charging optimization or grid load balancing
- Real-time telemetry pipelines or hardware streaming
- Distributed event-driven systems (Kafka, RabbitMQ, NATS)
- Service meshes or distributed tracing stacks
- Custom native mobile modules beyond Expo Go constraints
- Autoscaling or advanced infrastructure orchestration systems
- Kubernetes manifests (deferred post-validation)

---

## 2. Tech Stack & Architectural Constraints

### Mobile
- Expo SDK 54 (locked)
- React Native
- AsyncStorage for offline caching

### Web (Driver)
- React + Leaflet

### Dashboard (Partner / Admin)
- React + Tailwind CSS + shadcn/ui
- React Router v6
- Framer Motion (route transitions only — no decorative use)
- React Query (transport/cache layer only — not state management)

### Shared Frontend Logic
- Shared hooks, types, and business logic via `packages/` (TypeScript)
- Platform-specific rendering only (`Leaflet` vs `react-native-maps`)
- Design system consistency enforced via `packages/shared-ui`
- **`packages/` = TypeScript shared packages only**
- **`shared/` = Rust Cargo workspace crates only**
- These directories are distinct. Never conflate them.

### Backend
- Rust (Actix-web)
- SQLx (compile-time queries only — no runtime string construction)

### Database
- PostgreSQL 16 + PostGIS
- Single `platform_db` with schema isolation:
  - `gis` — owned by driver-service (raw OSM import)
  - `inventory` — owned by admin-service (stations, partners, chargers, materialized views)
  - `users` — owned by auth-service (user profiles)
- Separate: `keycloak_db`, `analytics_db`

### Identity
- Keycloak, single realm: `bornemap`
- Clients: `mobile-driver-app`, `web-driver-app`, `admin-partner-dashboard`
- Roles: `role:driver`, `role:partner`, `role:admin`

### Cache
- Redis — spatial query acceleration (driver-service responsibility only)

### Gateway
- Traefik (TLS termination + JWT forward auth + routing)

---

## 3. Service Topology (Frozen)

| Service | Port | Schema Owned | Responsibility |
|---|---|---|---|
| auth-service | :3000 | `users` | Sole Keycloak admin API caller. JWT issuance and sync. |
| driver-service | :3001 | `gis` | PostGIS spatial read API. Redis cache owner. |
| admin-service | :3002 | `inventory`, `analytics` | Partner CRUD. Audit pipeline. Cache bust trigger. |

**No fourth service may ever be added.**

---

## 4. Entity Identity System (Canonical)

All entity identifiers MUST follow:

```
<ENTITY_PREFIX>-<nanoid(12)>
```

| Entity | Prefix | Example |
|---|---|---|
| Users | `USR` | `USR-k8F3aZ91LmQx` |
| Operators/Partners | `OPR` | `OPR-9xQa2Lp0VmZk` |
| Stations | `STA` | `STA-pL91xZk8Qa2m` |
| Chargers | `CHG` | `CHG-mZ3kLx09PqRt` |

**Rules:**
- IDs generated only via shared nanoid(12) utility
- No manual ID assignment
- Prefixes are classification labels only — no runtime business logic may parse them
- CI validates format on every commit

---

## 5. System Governance Protocol (SpecKit SDD)

The LLM operating on this project must:

- Follow the SDEC v3.0 master prompt as the highest authority
- Never exceed current sprint scope
- Apply OpenAPI-first design (contract before any implementation code)
- Maintain strict service topology — no additions, no removals
- Surface all constitutional conflicts immediately rather than resolving them silently

### Source of Truth Hierarchy (canonical order)

1. SDEC Master Prompt (v3.0) — highest authority
2. This constitution (v1.4)
3. `constitution/guardrails.md`
4. `docs/architecture.md`
5. `api/openapi/*.yaml`
6. `sprints/<id>/state/sprint_state.json`
7. LLM output — lowest authority

### Sprint End Mandatory Outputs

At the end of every sprint the system MUST produce:

- `docs/SYSTEM_STATE.md` (updated)
- `docs/roadmap_status.md` (updated)
- `state/sprint_state.json` (updated)
- `sprints/<id>/review/sprint_review.md`
- `sprints/<id>/review/validation_report.md`
- `sprints/<id>/backlog/follow_up.md`

---

## 6. CI + SpecKit Enforcement

All rules enforced via `tools/ci_guard.sh` + `tools/sprint_engine.sh` + `tools/validate.sh`:

- Service topology validation
- Schema isolation enforcement (no cross-schema writes without service mediation)
- OpenAPI-first enforcement (API spec must precede implementation)
- SQLx compile-time safety enforcement
- nanoid(12) format validation
- Frontend API boundary enforcement
- Test coverage threshold enforcement
- Doc drift detection

**Any violation = HARD BUILD FAILURE.**

---

## 7. Known Inherited Bugs (Watch List)

These bugs are permanently tracked across all sprints:

| ID | Description | Rule |
|---|---|---|
| KNOWN-001 | Test stations leaking into production results | Always: `WHERE s.is_test = FALSE` |
| KNOWN-002 | `partner_profiles` missing `deleted_at` | Add to schema, verify in migrations |
| KNOWN-003 | Duplicate `/api/v1/nearby` across services | Single endpoint in driver-service only |
| KNOWN-004 | `ci_guard.sh` grep missing `-E` flag | Fix: `grep -E "SELECT .* FROM.*(users\|inventory\|gis)"` |

Every sprint review must confirm these are not reintroduced.
