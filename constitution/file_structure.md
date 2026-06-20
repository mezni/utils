# BorneMap — File Structure
**Version:** 1.1
**Date:** June 2026
**Supersedes:** v1.0

> **Monorepo root:** `bornemap/` (no `source/` prefix anywhere)
>
> `shared/` = Rust Cargo workspace crates ONLY
> `packages/` = TypeScript shared packages ONLY

---

```
bornemap/
│
├── constitution/                        # 📜 Governance Layer
│   ├── constitution.md                  # v1.4 — project identity, stack, topology
│   ├── guardrails.md                    # v2.1 — hard constraints + execution rules
│   ├── capabilities.md                  # v2.0 — seven professional skills
│   ├── file_structure.md                # this file
│   ├── delivery_os.md                   # Delivery OS specification
│   └── sdec_master_prompt.md            # SDEC v3.0 master prompt
│
├── docs/                                # 📚 Living System Documentation
│   ├── architecture.md                  # system architecture (frozen for validation phase)
│   ├── auth-flow.md                     # Keycloak + JWT flow detail
│   ├── api-contracts.md                 # index of all OpenAPI specs
│   ├── SYSTEM_STATE.md                  # runtime state snapshot (updated per sprint)
│   ├── roadmap_status.md                # sprint delivery tracking
│   └── adr/                             # Architecture Decision Records
│       ├── 0001-system-overview.md
│       ├── 0002-service-boundaries.md
│       └── 0003-data-isolation.md
│
├── infrastructure/                      # 🐳 Infrastructure Configuration
│   ├── docker/
│   │   ├── docker-compose.yml           # production compose
│   │   ├── docker-compose.dev.yml       # development override
│   │   ├── docker-compose.test.yml      # test environment (testcontainers supplement)
│   │   └── env/
│   │       ├── .env.dev
│   │       └── .env.prod
│   │
│   ├── traefik/
│   │   ├── traefik.yml
│   │   └── dynamic/
│   │       ├── routers.yml
│   │       ├── middlewares.yml
│   │       └── tls.yml
│   │
│   ├── keycloak/
│   │   ├── realm-export.json            # bornemap realm config
│   │   ├── clients/
│   │   │   ├── mobile-driver-app.json
│   │   │   ├── web-driver-app.json
│   │   │   └── admin-partner-dashboard.json
│   │   └── themes/
│   │
│   ├── postgres/
│   │   ├── init/
│   │   │   ├── 01-extensions.sql        # PostGIS, uuid-ossp
│   │   │   ├── 02-schemas.sql           # gis, inventory, users schema creation
│   │   │   ├── 03-roles.sql             # per-service DB roles + read-only role for mv
│   │   │   └── 04-analytics.sql         # analytics_db init
│   │   └── backups/
│   │
│   └── redis/
│       └── redis.conf
│
├── services/                            # 🦀 Rust Microservices
│   ├── auth-service/                    # :3000 — Keycloak owner, users schema
│   │   ├── src/
│   │   │   ├── api/                     # DTOs + Actix-web handlers (no business logic)
│   │   │   ├── domain/                  # pure business logic (no DB, no HTTP)
│   │   │   ├── application/             # use-case orchestration + trait definitions
│   │   │   ├── infrastructure/          # SQLx, Keycloak admin client, HTTP
│   │   │   └── main.rs                  # wiring only
│   │   ├── migrations/                  # append-only SQL migrations
│   │   ├── tests/                       # unit + integration tests
│   │   ├── Cargo.toml
│   │   └── Dockerfile
│   │
│   ├── driver-service/                  # :3001 — spatial API, gis schema, Redis owner
│   │   ├── src/
│   │   │   ├── api/
│   │   │   ├── domain/
│   │   │   ├── application/
│   │   │   ├── infrastructure/
│   │   │   └── main.rs
│   │   ├── migrations/
│   │   ├── tests/
│   │   ├── Cargo.toml
│   │   └── Dockerfile
│   │
│   ├── admin-service/                   # :3002 — inventory schema, analytics_db
│   │   ├── src/
│   │   │   ├── api/
│   │   │   ├── domain/
│   │   │   ├── application/
│   │   │   ├── infrastructure/
│   │   │   └── main.rs
│   │   ├── migrations/
│   │   ├── tests/
│   │   ├── Cargo.toml
│   │   └── Dockerfile
│   │
│   └── shared-services-config/          # shared Actix config (middleware, CORS, etc.)
│
├── shared/                              # 🦀 Rust Shared Crates ONLY
│   ├── auth-core/                       # JWT validation, role types
│   ├── db-models/                       # shared SQLx model types
│   ├── geo/                             # PostGIS geometry helpers
│   ├── validation/                      # input validation logic
│   ├── error/                           # unified error types
│   └── utils/                           # nanoid generator, common utilities
│
├── api/                                 # 📄 OpenAPI Contract Layer
│   ├── openapi/
│   │   ├── identity.yaml                # auth-service contract
│   │   ├── driver.yaml                  # driver-service contract
│   │   ├── admin.yaml                   # admin-service contract
│   │   └── shared.yaml                  # shared schemas / components
│   └── async-events/
│       └── schemas.md                   # future event schema reference (not implemented)
│
├── apps/                                # 📱 Frontend Applications
│   ├── web/                             # React + Leaflet (driver web)
│   │   ├── src/
│   │   ├── public/
│   │   ├── package.json
│   │   └── vite.config.ts
│   │
│   ├── dashboard/                       # React + shadcn/ui (partner/admin)
│   │   ├── src/
│   │   ├── package.json
│   │   └── vite.config.ts
│   │
│   └── mobile/                          # Expo SDK 54 (driver mobile)
│       ├── src/
│       ├── assets/
│       ├── app.json
│       └── package.json
│
├── packages/                            # 📦 TypeScript Shared Packages ONLY
│   ├── shared-types/                    # TypeScript types shared across apps
│   ├── shared-ui/                       # design system components (Skill 1 enforcement)
│   ├── shared-hooks/                    # domain hooks shared across apps
│   ├── api-client/                      # generated API client (from OpenAPI)
│   ├── auth-client/                     # Keycloak auth client wrapper
│   ├── config/                          # shared config (env, constants)
│   └── utils/                           # TypeScript utilities
│
├── scripts/                             # 🔧 Developer Tooling (not CI)
│   ├── dev-up.sh                        # start local dev environment
│   ├── dev-down.sh
│   ├── reset-db.sh
│   ├── migrate.sh
│   ├── seed.sh                          # real fixture data only — no mock/fake data
│   ├── import-osm.sh                    # Overpass API → gis.osm_charging_stations_temp
│   └── generate-client.sh              # OpenAPI → TypeScript client codegen
│
├── tools/                               # ⚙️ Sprint Execution Engine (CI-enforced)
│   ├── sprint_engine.sh                 # phase state machine + transition control
│   ├── validate.sh                      # artifact + checksum validation
│   ├── reconcile.sh                     # GitHub Issues ↔ backlog drift correction
│   ├── sync.sh                          # GitHub label + metadata sync
│   ├── test_runner.sh                   # test execution + coverage reporting
│   └── ci_guard.sh                      # hard validation gate (build breaker)
│
├── state/                               # 🧠 Global Execution State
│   ├── sprint_state.json                # active sprint + current phase
│   ├── mapping.json                     # backlog ID → GitHub Issue # map
│   ├── phase_registry.json              # phase history across all sprints
│   └── transition_log.json             # all phase transitions with timestamps
│
├── logs/                                # 📋 Runtime Logs
│   ├── sprint.log
│   ├── ci.log
│   └── validation.log
│
├── sprints/                             # 🏁 Sprint Execution System
│   └── sprint-NNN/                      # one directory per sprint (NNN = zero-padded)
│       ├── spec/
│       │   ├── spec.md                  # sprint goal + story list
│       │   ├── scope.md                 # explicit in-scope items
│       │   ├── non_scope.md             # explicit exclusions this sprint
│       │   └── assumptions.md           # decisions made without full information
│       ├── design/
│       │   ├── architecture.md          # sprint-level architecture delta
│       │   ├── data_model.md            # DB schema changes this sprint
│       │   ├── service_contracts.md     # service interaction contracts
│       │   └── diagrams.md              # supporting diagrams
│       ├── api/
│       │   └── openapi.yaml             # sprint OpenAPI spec (immutable after IMPLEMENTATION)
│       ├── implementation/
│       │   ├── backend/                 # Rust implementation artifacts
│       │   ├── frontend/                # TypeScript/React implementation artifacts
│       │   └── shared/                  # shared code artifacts
│       ├── testing/
│       │   ├── unit/                    # unit test files
│       │   ├── integration/             # integration test files
│       │   ├── test_results.log         # CI test runner output
│       │   └── coverage.md              # coverage thresholds vs actuals
│       ├── bugs/
│       │   ├── active.md                # open bugs (standard format)
│       │   ├── resolved.md              # closed bugs with regression test refs
│       │   └── regression_log.md        # recurrence tracking
│       ├── backlog/
│       │   ├── sprint_backlog.md        # epics → features → stories
│       │   ├── task_breakdown.md        # story → task decomposition
│       │   └── follow_up.md             # deferred work for future sprints
│       ├── state/
│       │   ├── sprint_state.json        # sprint-local phase + story state
│       │   ├── phase_history.json       # phase entry/exit timestamps
│       │   └── transition_log.json      # transition events for this sprint
│       ├── review/
│       │   ├── sprint_review.md         # delivery summary + security delta
│       │   ├── retro.md                 # what worked / what didn't
│       │   └── validation_report.md     # CI gate results + artifact verification
│       └── artifacts/
│           ├── generated_files_index.md # list of every file generated this sprint
│           └── checksum_manifest.json   # SHA256 hash of every generated file
│
├── .github/
│   └── workflows/
│       ├── backend.yml                  # Rust CI (cargo check, test, sqlx)
│       ├── frontend.yml                 # TypeScript CI (lint, type-check, vitest)
│       ├── tests.yml                    # integration + coverage gate
│       ├── validation.yml               # OpenAPI + doc drift + schema isolation
│       └── deploy.yml                   # deployment (post-validation phase only)
│
├── Cargo.toml                           # Rust workspace root
├── pnpm-workspace.yaml                  # pnpm monorepo config
├── package.json                         # root package (scripts only)
└── README.md
```

---

## Key Rules (Enforced by CI)

1. `shared/` contains Rust crates only. Never add TypeScript here.
2. `packages/` contains TypeScript packages only. Never add Rust here.
3. `packages/utils` and `shared/utils` are distinct — never merge them.
4. `scripts/seed.sh` produces real fixture data only — no fake or mock data.
5. `scripts/import-osm.sh` is the only authorized OSM data ingestion path.
6. All sprint directories follow the `sprint-NNN` naming pattern (e.g. `sprint-001`).
7. `sprints/<id>/api/openapi.yaml` becomes immutable once IMPLEMENTATION phase opens.
8. `services/` has exactly three *service* subdirectories (`auth-service`, `driver-service`, `admin-service`). `shared-services-config` is a shared config utility, not a 4th service.
