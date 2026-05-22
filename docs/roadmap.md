# BorneMap Roadmap

Phased delivery plan for BorneMap v3. Each phase lists its **Goal**,
**Deliverables**, and the **Constitution principle(s)** it satisfies.

Every phase MUST end with the Constitution's Definition of Done (see
Principle VII) before the next phase begins:

1. Applicable tests pass in CI.
2. OpenAPI specs updated for any REST surface change.
3. Security review confirms Principle V is upheld.
4. Logging / metrics / health for the changed path comply with Principle VI.
5. An ADR is filed if a constitutional boundary is affected.

Methodology (Kanban + sprint cadence + GitHub Projects schema + GH Actions)
lives in [methodology.md](./methodology.md).

---

## Phase 0 — Foundation & Governance

**Goal**: Prepare the repository and governance model.

**Deliverables**

- Monorepo setup (top-level layout per Constitution §"Repository structure")
- Constitution committed (`.specify/memory/constitution.md`)
- ADRs 0001–0005 committed (`docs/adr/`)
- Branch strategy (trunk-based, short-lived feature branches)
- PR templates (`.github/PULL_REQUEST_TEMPLATE.md`)
- `CODEOWNERS`
- `CONTRIBUTING.md`

**Principles satisfied**: Governance (ADR rule), Principle VII (DoD).

---

## Phase 1 — CI/CD & Dev Environment

**Goal**: Automated validation and local orchestration.

**Deliverables**

- GitHub Actions pipeline (lint → unit → integration → openapi-bundle →
  docker-build)
- `docker-compose.yml` with required containers: `nginx`, `keycloak`,
  `auth-service`, `core-service`, `geo-service`, `analytics-service`,
  `postgres`, `mongodb`, `rabbitmq`
- `Makefile` (up / down / logs / test / lint / openapi)
- NGINX routing (path → service map, per
  [operations/deployment.md](./operations/deployment.md))
- Environment strategy (`.env.example`, no secrets committed)
- `/health` endpoints (liveness + readiness) on every service
- `/metrics` endpoints (Prometheus-compatible) on every service
- **OpenAPI bundle + publish job in CI** so every later phase has a
  publishing target for Principle VII DoD item 2

**Principles satisfied**: I (modular services), V (TLS at gateway, secrets
via env), VI (health + metrics), VII (CI gates).

---

## Phase 2 — Service Scaffolding

**Goal**: Create all backend services and frontend foundations.

**Deliverables**

- `core-service` scaffold (NestJS)
- `auth-service` scaffold (NestJS)
- `geo-service` scaffold (Rust + Actix-Web)
- `analytics-service` scaffold (NestJS)
- React + Vite frontend scaffold under `frontend/` with route groups
  `/`, `/driver/*`, `/operator/*`, `/admin/*` reserved (single app, role-based
  guards added later)
- OpenAPI setup per NestJS service
- React Query setup
- Leaflet setup
- **JWT validation middleware (dev-mode issuer)** wired into every NestJS
  service. Phase 6 swaps the dev issuer for Keycloak with no endpoint
  rewrites.

**Principles satisfied**: I (service boundaries), V (JWT validated per
service from day one), VI (structured logs + correlation ID middleware).

---

## Phase 3 — Database & Tunisia Map

**Goal**: Enable PostGIS and spatial infrastructure.

**Deliverables**

- PostgreSQL + PostGIS provisioned via docker-compose
- Migrations framework
- **`deleted_at TIMESTAMPTZ` columns on `companies`, `stations`, `chargers`**
  with default index `WHERE deleted_at IS NULL`
- Cascade scaffolding (triggers or service-level cascade utilities) for
  company → stations → chargers
- `outbox` table (id, aggregate_type, aggregate_id, event_type, payload,
  created_at, published_at)
- Tunisia OSM import (full country)
- Nearby search stored procedure / SQL using PostGIS
- Seed data (sample companies, stations, chargers)

**Principles satisfied**: I (Postgres + PostGIS as source of truth), III
(outbox table created before any producer), IV (soft-delete columns).

---

## Phase 4 — Geo-Service

**Goal**: High-performance geospatial APIs.

**Deliverables**

- Nearby search API (`GET /nearby?lat&lon&radius`)
- Bounding-box API (`GET /bbox?minLat&minLon&maxLat&maxLon`)
- Route API (origin → destination, returns polyline)
- ETA API (origin → destination given speed profile)
- DashMap cache for hot reads
- Benchmarking harness
- GeoJSON responses

**Performance target**: p99 < 200 ms.

**Benchmark assumptions** (so the SLA is grounded):

- Dataset: full Tunisia OSM import from Phase 3.
- Station count: as seeded in Phase 3 (~MVP scale, target ≥ 1k stations).
- Query mix: nearby / bbox / route ≈ 70 / 20 / 10.
- Cache state: warm DashMap.
- Topology: single-node baseline behind NGINX.

**Principles satisfied**: I (geo isolated in Rust per ADR-002), VI
(metrics on every endpoint), VII (spatial correctness tests).

---

## Phase 5 — Core-Service

**Goal**: Business APIs with transactions, outbox, audit logging, and
soft delete.

> Phase 5 is split into a **gating sub-phase 5a** and a delivery sub-phase
> **5b**. No CRUD endpoint MAY merge until 5a is green. This enforces
> Principle III (NON-NEGOTIABLE outbox).

### Phase 5a — Outbox Foundation (gating)

**Deliverables**

- Outbox write helper (`insertOutbox(tx, event)`) callable inside any
  business transaction
- Outbox relay worker (polls `outbox` rows, publishes to RabbitMQ, marks
  `published_at`)
- Transaction rollback tests: business mutation rollback MUST also roll
  back the outbox row
- Idempotency contract for consumers (event id as dedupe key) documented

**Exit gate**: Phase 5b cannot start until 5a tests are green in CI.

### Phase 5b — Business APIs

**Deliverables**

- Company CRUD (Admin-only creation, per Constitution Principle II)
- Station CRUD (owner = company OR private individual)
- Charger CRUD (belongs to exactly one station)
- Favorites
- Reviews
- Moderation
- Audit log events emitted via the outbox for every mutation
- Soft-delete cascade tests:
  - Deleting a company soft-deletes its stations and chargers.
  - Deleting a station soft-deletes its chargers.
- Read-path queries verified to include `WHERE deleted_at IS NULL`

**Principles satisfied**: II (domain model), III (every event via outbox),
IV (soft delete + cascade), VI (correlation IDs through mutation +
outbox + relay), VII (transaction, outbox, audit, soft-delete tests).

---

## Phase 5.5 — Analytics Consumer

**Goal**: Close the event-integrity loop by consuming outbox events into
MongoDB.

**Deliverables**

- `analytics-service` RabbitMQ consumer with at-least-once semantics
- Idempotency layer (dedupe on event id)
- MongoDB persistence for analytics aggregates AND audit logs
- Minimal read endpoints for Phase 10 admin/analytics UI
- End-to-end tests: HTTP write in `core-service` → outbox → RabbitMQ →
  Mongo document visible to consumer

**Principles satisfied**: I (analytics + audit on MongoDB only), III
(at-least-once + idempotency), VI (metrics on consumer lag), VII
(end-to-end audit-log tests).

---

## Phase 6 — Auth & Keycloak

**Goal**: Replace the Phase 2 dev issuer with Keycloak and add full
authorization features.

**Deliverables**

- Keycloak realm configuration (committed as JSON / scripted import)
- PKCE authentication flow for the SPA
- Google OAuth identity broker
- JWT validation switched to Keycloak public keys at the gateway AND in
  every service
- Role guards (admin, operator, driver, public) wired into NestJS
- Operator invitation flow
- Silent token refresh

**Why no endpoint rework**: Phase 2 already shipped JWT-validation
middleware behind a dev issuer; Phase 6 swaps the issuer config and adds
guards. No business endpoint changes.

**Principles satisfied**: V (Keycloak sole IdP per ADR-0003, PKCE, JWT
validated at gateway and service).

---

## Phase 7 — Public Frontend (route group `/`)

**Goal**: Public map and station discovery.

**Deliverables**

- Tunisia map (Leaflet)
- Bounding-box queries to `geo-service` as the map moves
- Nearby search UI
- Station detail drawer
- Filtering (charger type, power, availability stub)
- Mobile-responsive UI
- Route transitions (Framer Motion)

**Principles satisfied**: I (REST-only client), VI (correlation ID
propagated in client requests).

---

## Phase 8 — Driver Frontend (route group `/driver/*`)

**Goal**: Authenticated driver experience.

**Deliverables**

- Registration / login via Keycloak (Phase 6)
- Favorites
- Reviews submission
- Route visualization (uses Phase 4 Route API)
- ETA display (uses Phase 4 ETA API)
- Protected routes (driver-role guard)

**Principles satisfied**: II (favorites/reviews belong to the user), V
(role-guarded routes), VII (route + ETA correctness tests).

---

## Phase 9 — Operator Portal (route group `/operator/*`)

**Goal**: Operator management tools.

**Deliverables**

- Station management UI (CRUD scoped to the operator's company)
- Charger management UI
- Company profile editor
- Sub-operator management
- Company-scope guards (operator cannot read/write outside their company)
- Flag / moderation notifications

**Principles satisfied**: II (company-owned stations), IV (soft-delete
respected in UI lists), V (scope guards enforced in service AND in UI).

---

## Phase 10 — Admin Dashboard & Analytics (route group `/admin/*`)

**Goal**: Administration and analytics tooling.

**Deliverables**

- Moderation queue
- Company management (Admin-only creation per Principle II)
- Audit log viewer (reads from `analytics-service`)
- Analytics charts (reads from `analytics-service`)
- User management (Keycloak admin proxy)
- Date filtering across audit + analytics views

**Principles satisfied**: I (analytics reads MongoDB via
analytics-service only), II (admin creates companies), VI (audit
visibility).

---

## Phase 11 — Hardening & Production Deploy

**Goal**: Production deployment and operational hardening.

**Deliverables**

- HTTPS (Let's Encrypt at NGINX)
- Security headers (HSTS, CSP, X-Frame-Options, Referrer-Policy)
- Backups (Postgres + Mongo)
- Monitoring (Prometheus scrape config; Grafana/Loki deferred to
  Phase 12)
- Load testing against Phase 4 SLA
- SSL Labs verification (≥ A grade)
- Production deployment runbook (`docs/operations/deployment.md`
  finalized)

**Principles satisfied**: V (TLS, rate limiting, no public Keycloak),
VI (metrics scraped), VII (load tests, security validated).

---

## Phase 12 — Post-MVP Backlog

Not started until Phase 11 ships. Each item is a candidate for its own
ADR if it affects a constitutional boundary.

- Redis (caching layer)
- Kubernetes (replacing docker-compose) — **requires ADR; affects
  Constitution Deployment section**
- Prometheus stack (full deployment)
- Grafana
- Loki (log aggregation)
- OpenTelemetry (distributed tracing) — supersedes Phase 1 correlation-ID
  approach
- Multi-language support (i18n)
- Mobile app
- Real-time charger availability — **explicitly a Constitution non-goal
  for MVP; lifting requires ADR**

---

## Estimated Timeline (Solo)

| Phase | Description | Duration |
|---|---|---|
| 0 | Foundation & Governance | 2–3 days |
| 1 | CI/CD & Dev Environment | 1 week |
| 2 | Service Scaffolding (+ JWT middleware) | 1 week |
| 3 | Database & Tunisia Map | 1–2 weeks |
| 4 | Geo-Service | 2–3 weeks |
| 5 | Core-Service (5a + 5b) | 3–4 weeks |
| 5.5 | Analytics Consumer | 1 week |
| 6 | Auth & Keycloak | 1 week |
| 7 | Public Frontend | 2 weeks |
| 8 | Driver Frontend | 1–2 weeks |
| 9 | Operator Portal | 1–2 weeks |
| 10 | Admin Dashboard & Analytics | 2 weeks |
| 11 | Hardening & Production Deploy | 1 week |

**Total**: ~21–26 weeks solo development.

Phase 12 (post-MVP) is open-ended and not included in the MVP timeline.
