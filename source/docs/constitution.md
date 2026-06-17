# BorneMap Project Constitution
**Version:** 1.3 | **Date:** June 2026 | **Status:** Approved / Core Reference

---

## 1. Project Identity & Mission

**Name:** BorneMap

**Mission:** EV charging station discovery and management platform for the Tunisian market.

**Primary Optimization Objective:** Fast product validation through rapid iteration ("Validation before Optimization").

### Validation-Phase Exclusions
The platform SHALL NOT include any of the following features during the validation phase:
- OCPP integration, charging session signaling, or direct hardware communications.
- Native billing, invoicing, or payment processing workflows.
- Smart charging optimization or grid load balancing telemetry.
- Real-time hardware status metrics or continuous charger telemetry tracking.
- Distributed event-driven streaming engines (e.g., Kafka, RabbitMQ) or service meshes.
- Native mobile compilation pipelines or customized native modules before validation.
- Infrastructure autoscaling policies or advanced distributed tracing stacks.

### System Access Profiles
- **Public Users:** Discover charging stations, visualize infrastructure on an interactive map, inspect station metadata.
- **Registered Drivers:** Personalize experience by saving favorites and reviewing operational station locations.
- **Partners:** Manage their own stations (can be business or private entities).
- **Administrators:** Manage physical infrastructure data nodes and maintain strict spatial accuracy.

---

## 2. Tech Stack & Architectural Constraints

- **Mobile:** Expo SDK 54 (locked), React Native + AsyncStorage (for local offline viewport and marker caching).
- **Web (Driver):** React + Leaflet.
- **Unified Experience:** Shared codebase/components between mobile and web driver apps where possible (shared business logic, hooks, types); platform-specific rendering only where required (e.g., Leaflet on web / react-native-maps on mobile).
- **Dashboard:** React + Tailwind CSS + shadcn/ui + React Router v6 + Framer Motion (route transitions only) + React Query.
- **Backend:** Rust / Actix-web from MVP-1.
- **Shared Service Code:** Cargo workspace crates for backend models and validations; separate TypeScript packages for frontend resource sharing.
- **Database:** PostgreSQL + PostGIS. Single instance `platform_db` isolating concerns via schemas: `gis`, `inventory`, `users`. Dedicated `keycloak_db` and separate `analytics_db`.
- **Identity:** Keycloak, single realm (`bornemap`). Access profiles isolated via granular Client Roles (`role:driver`, `role:partner`, `role:admin`) across distinct Keycloak Clients (`mobile-driver-app`, `web-driver-app`, `admin-partner-dashboard`).
- **Cache:** Redis (GIS query and spatial tile cache managed directly by the Driver Service from MVP-6).
- **Gateway:** Traefik (TLS, routing, from MVP-6).
- **Monorepo Root:** `source/`
- **Entity IDs:** Prefixed NanoID string formats (`USR_`, `OPR_`, `STA_`, `CHG_`).
- **Soft Delete:** Enforced on infrastructure entities only (stations, chargers, partners).

---

## 3. Monorepo Structure & Conventions

```
source/
├── apps/
│   ├── mobile-driver/        # Expo SDK 54 (Uses shared-types, shared-hooks)
│   ├── web-driver/           # React + Leaflet (Uses shared-types, shared-hooks, shared-ui)
│   └── dashboard/            # React + shadcn/ui (Uses shared-types, shared-ui)
├── services/                 # Independent Actix-Web Microservices
│   ├── auth-service/         # :3000 (Central Identity & User Schema Gateway)
│   ├── driver-service/       # :3001 (Geospatial Read API + Inventory Writes + Cache Management)
│   └── admin-service/        # :3002 (Partner Infrastructure Management + Analytics Logging)
├── packages/                 # Shared TypeScript Workspace
│   ├── shared-types/         # Unified API Request/Response TS interfaces
│   ├── shared-hooks/         # React Query hooks for auth, locations, and nearby queries
│   └── shared-ui/            # Tailwind configurations, inputs, form components
├── crates/                   # Shared Rust Workspace
│   ├── db-models/            # SQLx Structs for platform_db schemas & NanoID generation
│   └── validation/           # Domain business rules (e.g., plug speeds, geographic limits)
├── infra/
│   ├── docker-compose.yml    # Local multi-service infrastructure orchestrator
│   ├── keycloak/             # Realm provisioning configurations and custom flows
│   └── osm-importer/         # OpenStreetMap Tunisian spatial extraction tooling
└── docs/
    └── adr/                  # Architectural Decision Records
```

### Naming Conventions
- **Services:** kebab-case, suffixed with `-service`.
- **Apps:** kebab-case, descriptive (`mobile-driver`, `web-driver`, `dashboard`).
- **Packages/Crates:** kebab-case, prefixed by domain scope (`shared-`, `db-`).

---

## 4. Domain Model & Entity Conventions

### Entity ID Prefixes (NanoID-based)
- `USR` — User profile pointer
- `OPR` — Operator/Partner
- `STA` — Charging Station
- `CHG` — Individual Charger Hardware Node

### Core Domain Rules
- Admin-only partner creation via invitation workflows, OR partner self-registration via dashboard validated manually by an admin.
- No operator self-registration bypasses admin review.
- Companies (partners) act as the top-level grouping; no independent "networks" layer exists.
- Private home chargers are treated as first-class public map entities, visible alongside commercial stations.
- Private and commercial stations share the exact same database schema constraints and validation structures. Specializations handled via nullable metadata fields.
- Soft delete applies exclusively to infrastructure entities (stations, chargers, partners).

### Schema Ownership
- **`gis` schema** — OpenStreetMap-imported spatial reference data (roads, administrative boundaries, city layouts).
- **`inventory` schema** — Operational infrastructure entities (partner, station, charger) along with user-driven operational interactions (e.g., user favorites or reviews data tables managed by the Driver Service).
- **`users` schema** — Application-level user profile mapping data, owned by the Auth Service and keyed directly to the unique Keycloak String ID (`sub`).

---

## 5. Architectural Principles & Patterns

- **Unit of Work:** All multi-table data modifications within any microservice must be wrapped in a single database transaction block.
- **Cache Invalidation:** Writes or changes hitting `inventory.station` or `inventory.charger` trigger a synchronous cache-bust operation on the Redis spatial cache managed by the Driver Service.
- **Audit Logging:** MongoDB-based independent asynchronous audit logging engine tracks structural layout changes across stations, chargers, and partners.
- **ADR Governance:** Structural adjustments or tooling adaptations are recorded as Architectural Decision Records within `docs/adr/`.
- **Read/Write Separation:** The Driver Service functions as a read-optimized spatial data API via SQL PostGIS functions and internal Redis layers, while handling its own driver transactional updates. No asynchronous outbox patterns are deployed for the validation phase.

---

## 6. API & Service Boundaries

### API Versioning
Endpoints are uniformly prefixed with `/api/v1/`. Major version increments occur exclusively on breaking changes. Additive modifications occur inline.

### Microservice Scopes
- **Auth Service (:3000):** Sole owner of the `users` schema. Integrates directly with the single `bornemap` Keycloak realm. Handles user registration loops, identity profiles, and token issuance workflows. No other microservice interacts with Keycloak directly.
- **Driver Service (:3001):** Owns the `inventory` schema read patterns and user relationship records (e.g., favorites/reviews). Exposes the `/api/v1/nearby` endpoint utilizing PostGIS spatial features. Implements and manages the Redis caching tier.
- **Admin Service (:3002):** Handles partner/station validation dashboards. Executes analytical metric operations by streaming logs directly into the isolated `analytics_db`.

---

## 7. Non-Functional Requirements & Prohibitions

### Code Quality
- **Enforcement:** Code layout checked via automated toolchains (`rustfmt` + `clippy` for Rust; `eslint` + `prettier` for TypeScript).
- **Type Safety:** TypeScript strict mode enabled globally across all user applications. No implicit `any` bindings allowed.

### Security
- Central TLS mapping layer managed via Traefik (starting MVP-6).
- Application layer access tokens processed against Keycloak JWT validation steps.
- Cleartext credentials, API keys, or security vectors are completely barred from git tracking; handled via environmental injection using gitignored `.env` files.

### Explicit Prohibitions for AI Coding Models (SpecKit Guardrails)
> **NO** billing, payment processing, or financial invoicing code modules.
> **NO** OCPP or low-level charger hardware communication protocol logic.
> **NO** Message queue frameworks or broker tools (RabbitMQ, Kafka, MQTT) allowed without an approved ADR.
> **NO** Microservices can be introduced beyond the three defined boundaries (Auth, Driver, Admin).
> **NO** Mocked data layers or fake in-memory repositories in production targets (real database connectivity from MVP-1).
> **NO** Native mobile build dependencies or custom native modules outside default Expo Go bounds before validation phase completion.

---

## 8. MVP Roadmap & Phasing (Reworked Flow)

- **MVP-1: Spatial Core Validation Pipeline** — Create PostGIS `platform_db` Docker profile and define `gis` and `inventory` schemas (`init.sql`). Build the containerized `osm-importer` to load Tunisia PBF data. Author the native `nearby` PostGIS SQL function. Build the Actix-web `driver-service` skeleton with `/health` and `/api/v1/nearby` endpoints. Scaffold the Expo SDK 54 mobile app and React web client with matching Tunisian viewports, markers, and premium loading/error UX. Introduce Traefik for reverse proxy routing. Execute local integration tests.
- **MVP-2: Central Identity Integration** — Provision Keycloak using `kcadm` to establish the single `bornemap` mono-realm. Establish the `auth-service` skeleton to bridge user registrations and token maps. Hook up public driver registration flows (allocating `role:driver`) and configure the secure invitation framework for the administration dashboard to assign `role:partner`.
- **MVP-3: Partner Curation Portals** — Kick off the `admin-service` and layout the partner/admin dashboard using shadcn/ui and Framer Motion. Implement secure CRUD capabilities against stations and chargers. Connect synchronous cache-busting logic across the Driver Service.
- **MVP-4: Real-time Metadata Extensibility** — Incorporate precise domain logic fields within the shared validation engine. Handle private home charger distinctions, accessibility validation models, and custom availability scheduling metrics.
- **MVP-5: Analytical Ingestion & Caching** — Solidify the asynchronous analytical data pipelines within the Admin Service to pipe raw logging structures into the `analytics_db`. Wire up the production Redis instance inside the Driver Service to cache spatial tile query limits and boost read performances.
- **MVP-6: Production Hardening** — Finalize target deployment manifests. Secure Edge structures via Traefik TLS parameters, lock down environmental injection matrices, and hook up overall monitoring/observability runtimes.

---

## 9. Governance

- **Source of Truth:** This document remains the final authority for architectural structure. Direct conflicts between source implementations and configuration files are resolved in favor of this constitution.
- **Amendments:** Modifications to fundamental sections (Sections 1, 2, 5, or 7) strictly require an accompanying ADR recorded inside `docs/adr/`.
- **Evolutionary Sections:** Structural monorepo definitions, entity lists, boundaries, and roadmap tasks (Sections 3, 4, 6, 8) can adapt dynamically without a formalized ADR, provided this master constitution text is updated immediately.
- **AI Coding Model Enforcement:** Coding LLMs must parse this layout alongside `.speckit/rules.md`. Any violation of Section 7 boundaries constitutes a blocking compliance error. Daily tasks must be paired with automatic documentation log adjustments inside the `docs/` workspace path.
