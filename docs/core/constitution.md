# BorneMap Constitution

This constitution defines the non-negotiable principles, rules, and structures that govern the BorneMap platform. No sprint decision, deadline pressure, or convenience argument can override these rules. Violations are Class A issues.

---

## 1. Core Principles

### Principle 1 — Pragmatic Architecture
Use the minimum number of services that correctly separate responsibilities. Do not introduce a new service, worker, or infrastructure component unless no existing component can own the responsibility correctly.

### Principle 2 — Single Source of Truth
Every entity has exactly one authoritative owner. No ambiguity about where data is written or read from. All other representations are derived.

### Principle 3 — Simple Operations
The platform must be operable by one person. Every operational task must have a documented runbook. Complexity that cannot be operated simply is not acceptable.

### Principle 4 — Domain Separation by Schema
Business data, GIS data, user data, and analytics data are separated by PostgreSQL schema and by service responsibility. Cross-schema writes are forbidden except where explicitly permitted by this constitution.

### Principle 5 — Build for Current Scale
Introduce complexity only when current scale justifies it. Premature optimization is a constitution violation. Every non-trivial complexity decision requires an ADR.

### Principle 6 — Public Access is a First-Class Concern
Anonymous public browsing must always work. Authentication must never be required to view stations, markers, or search results. Auth is only triggered at the moment a gated action is attempted.

### Principle 7 — RTL and Arabic are not Afterthoughts
Arabic language support and RTL layout are built from the start in every frontend application. They are not added later. Any screen that does not work correctly in Arabic RTL is a Class A bug.

### Principle 8 — Visual Consistency Across All Surfaces
All four applications share the same design token foundation. Brand identity, color semantics, spacing, and typography are defined once in packages/ui and consumed everywhere. No hardcoded visual values anywhere in application code.

---

## 2. Service Inventory

Exactly these services exist in production:

| Service | Type | Responsibility |
|---------|------|---|
| **Keycloak** | Auth Server | Authentication, token issuance, role management |
| **Driver Service** | Rust/Actix-web | Public discovery, authenticated driver features |
| **Admin Service** | Rust/Actix-web | Partner & admin management, reporting |
| **Clickstream Service** | Rust/Actix-web | Analytics event ingestion & persistence |
| **Traefik** | Edge Router | TLS, routing, public entrypoint |

**No other services may be introduced without an approved ADR.**

Key deferred components:
- **No RabbitMQ** — Analytics events written directly to PostgreSQL
- **No GIS Sync Worker** — Synchronization handled by PostgreSQL trigger

---

## 3. Data Architecture

### Database
Single database: **PostgreSQL 16 + PostGIS**

### Four Schemas
Each schema has exclusive ownership and write permissions:

| Schema | Owns | Written By | Read By |
|--------|------|-----------|---------|
| **inventory** | partner, station, charger, station_availability | Admin Service | Admin Service, Driver Service, GIS trigger |
| **users** | user_account, user_profile, partner_membership, favorite_station, station_review | Driver Service | Driver Service, Admin Service (reporting) |
| **gis** | osm_nodes, osm_ways, roads, boundaries, amenity_points, station_locations | OSM import, trigger function | Driver Service |
| **analytics** | raw_events, event_aggregates | Clickstream Service | Admin Service (reporting) |

### Critical Rules
1. **inventory.station is the source of truth** for all stations. No other table or system is authoritative.
2. **gis is a derived enrichment layer**. It is never the master of any business entity.
3. **gis.station_locations is written exclusively by PostgreSQL trigger function**. No application service writes directly.
4. **Analytics data lives only in the analytics schema**. Never stored in inventory or users.
5. **Cross-schema access follows the explicit table in [section 9 of this constitution](architecture/data.md#cross-schema-access-rules)**.

### Identifier Rules
All business entities use prefixed NanoIDs:

| Prefix | Entity | Format |
|--------|--------|--------|
| USR | user_account | USR-{nanoid} |
| PRT | partner | PRT-{nanoid} |
| STN | station | STN-{nanoid} |
| CHG | charger | CHG-{nanoid} |
| REV | station_review | REV-{nanoid} |
| EVT | analytics event | EVT-{nanoid} |

These are used consistently in APIs, database records, logs, and events. **Sequential integers are never exposed in public APIs.**

---

## 4. Roles and Access Model

### Public Driver
- Anonymous. No login required.
- **Can:** View nearby stations, map markers, search/filter, station detail, public reviews
- **Cannot:** Favorite, write reviews, access profile

### Registered Driver
- Authenticated. Keycloak role: `registered_driver`
- Everything a Public Driver can do, plus:
  - Manage favorites
  - Create, update, delete own reviews
  - View and update own profile

### Partner
- Authenticated. Keycloak role: `partner`
- **Can:**
  - Access Dashboard App (partner view)
  - View and manage own stations only
  - View and manage own chargers only
  - Update own station availability
  - View own reports
- **Rules:**
  - A partner belongs to exactly one partner. Enforced by `users.partner_membership` primary key.
  - Every partner operation is scoped to the partner's own data via JWT `partner_id` claim in Admin Service middleware.
  - A partner can **never** read or write another partner's data.

### Admin
- Authenticated. Keycloak role: `admin`
- **Can:**
  - Access Dashboard App (admin view)
  - Manage all users
  - Manage all partners
  - Manage all stations and chargers
  - Moderate reviews
  - Access global reporting

---

## 5. Authentication and Authorization

### Authentication Owner
**Keycloak owns all authentication.** No service implements its own login, token issuance, or session management.

### JWT Validation
- Every protected endpoint validates the Bearer token against the Keycloak JWKS endpoint
- JWKS response is cached with background refresh
- JWKS is **never** fetched per request

### Role Enforcement
- Roles extracted from JWT `realm_access.roles` claim by `ev-auth` shared crate
- Role enforcement applied in middleware before any handler runs

### Partner Scope Enforcement
- `partner_id` claim injected into JWT by Keycloak mapper at login
- Admin Service middleware extracts claim and applies as **mandatory filter** on all partner-scoped queries
- Enforced in middleware — individual handlers do **not** implement scope checks

### Token Storage Rules
- **Web applications:** tokens stored in memory only — never in localStorage, sessionStorage, or cookies accessible to JavaScript
- **Mobile application:** tokens stored in `expo-secure-store` only — never in AsyncStorage

### Authenticated Upgrade Pattern
- Public browsing **never** triggers an auth prompt
- Authentication triggered only at the moment a gated action is attempted
- User shown upgrade modal with login options
- Original action resumed automatically on completion

---

## 6. Frontend Applications

### Driver Web App
- **Tech:** React + Vite
- **Purpose:** Public and authenticated driver experience
- **Layout:** Full-bleed map with floating UI elements
- **Tokens:** Plus Jakarta Sans, all brand/surface/text/status tokens, map-specific tokens

### Driver Mobile App
- **Tech:** React Native + Expo
- **Purpose:** iOS and Android driver experience
- **Layout:** Full-bleed map with bottom sheet pattern
- **Tokens:** All brand/surface/text/status tokens via native export

### Dashboard App
- **Tech:** React + Vite
- **Purpose:** Single app serving both Partner and Admin roles
- **Role Switching:** Role determined from JWT on login
- **Layout:** Sidebar navigation
- **Tokens:** All brand/surface/text/status tokens, Inter typography, no map-specific tokens

---

## 7. Design System

### Token Foundation
All visual values defined as tokens. **No color, spacing, typography, radius, shadow, or border value may be hardcoded in any component or application code.** Tokens are the only permitted source of visual values.

### Color Tokens
- **brand.primary** (#007943) — Primary actions, CTAs, active states
- **brand.primaryDark** (#005c32) — Gradients, pressed states
- **brand.sageLight** (#EAF0E6) — Selected states, map terrain
- **brand.glow** (#00E676) — Live map pin markers (driver apps only)
- **surface** tokens — Backgrounds, cards, sidebars
- **text** tokens — Main, muted
- **border** tokens — Default, subtle
- **status** tokens — available, inUse, maintenance (color + background)
- **neutral** scale — Full gray palette

### Typography Tokens
- **Driver apps:** Plus Jakarta Sans (high-contrast for maps)
- **Dashboard App:** Inter (readability in data tables)
- **Sizes:** xs(10), sm(12), base(14), lg(16), xl(18), 2xl(20), 3xl(24)
- **Weights:** regular(400), medium(500), semibold(600), bold(700), extrabold(800)

### Spacing, Radius, Shadow
- **Spacing base unit:** 4px. Scale: 4, 8, 12, 16, 20, 24, 32, 40, 48, 64, 80, 96
- **Radius:** sm(4px), md(8px), lg(12px), xl(16px), 2xl(20px), 3xl(24px), full(9999px)
- **Shadows:** card, panel, float, pin (for map markers)

### Component Ownership
- **Shared:** Button, Input, Select, Checkbox, Toggle, Textarea, Modal, Toast, Alert, Badge, Skeleton, EmptyState, ErrorState, Table, StatCard, StatusBadge
- **Driver-specific:** MobileShell, SearchBar, FilterPills, MapPinMarker, BottomStationCard, ZoomControls, BottomTabBar
- **Dashboard-specific:** AppShell, Sidebar, NavigationItem, TopBar, PageContent, DataCard, DataTable

### Accessibility and Language Rules
- All web applications target **WCAG 2.1 AA minimum**
- **Arabic and French** are the two supported languages in all applications
- **RTL layout must be correct for Arabic on every screen** — failures are Class A bugs
- Language switching must work without a page reload

---

## 8. Non-Negotiable Rules

These rules may **never** be overridden by any sprint decision, deadline pressure, or convenience argument. Violating any of these is a **Class A issue**.

### Data Rules
- **inventory.station is the source of truth** for stations
- **gis is never the master** of any business entity
- **Analytics lives in the analytics schema exclusively**
- **No additional schemas** without an approved ADR
- **Cross-schema access follows section 9 exactly**

### Access Rules
- **Public driver access requires no login** at any point
- **Registered-driver features require authentication**
- **Partner users belong to exactly one partner** — enforced at database level
- **Partner operations always scoped** to the partner's own data
- **Admin Service does not write to users schema**

### Infrastructure Rules
- **Only Traefik exposes public ports** (80 and 443)
- **Keycloak owns all authentication**
- **No service implements its own token issuance**
- **Secrets never appear in committed files** or container images
- **No image registry** — images built on the host

### Frontend Rules
- **No visual value hardcoded** in any component — tokens only
- **Tokens never stored in localStorage or AsyncStorage**
- **Arabic RTL must work on every screen** — Class A bug if it doesn't
- **Public browsing never triggers an auth prompt**
- **packages/ui/native must stay synchronized** with packages/ui/src/tokens/colors.ts

### Deferred Rules
- **OCPP is out of scope**
- **Payments are out of scope**
- **Routing is out of scope**
- Introducing any of these requires a new implementation plan phase and an approved ADR

---

## 9. Bug Classification

### Class A — Critical
Blocks correctness, security, or user access. **Must be resolved before phase closes.**

Examples:
- Wrong data returned
- Auth bypass
- RTL layout broken
- Spatial index missing
- Public access broken
- Station is not source of truth

### Class B — Major
Degrades quality but does not block. **Must be resolved before target phase closes.**

Examples:
- Slow query
- Missing error message
- UI misalignment in non-critical screen
- Incomplete error handling

### Class C — Minor
Improvement or nice-to-have. **No mandatory phase target.**

Examples:
- Refactor opportunity
- Minor UX polish
- Documentation gap

---

## 10. Definition of Done

### Sprint Done
- All planned tasks complete or explicitly deferred with written reason in sprint file
- No Class A bugs open
- All tests pass
- Sprint summary written in `docs/project/sprints/sprint-NN.md`

### Phase Done
- All sprints meet Sprint Done criteria
- All phase done criteria checked in `docs/project/phases/phase-NN-status.md`
- Documentation updated to reflect what was actually built
- Zero Class A bugs open
- Hardening sprint completed

---

## 11. Architecture Decisions

All non-trivial architecture decisions are recorded as immutable ADRs in `docs/adr/`. An ADR is required before implementing any decision that:

- Introduces a new service, infrastructure component, or data store
- Changes the source of truth for any entity
- Changes the authentication or authorization model
- Supersedes a previous ADR
- Introduces a pattern not currently in use

**ADRs are never edited.** If a decision changes, a new ADR is written with status "Superseded" referencing the old one.

Current ADRs:
- ADR-001 — PostgreSQL + PostGIS as single database (Accepted)
- ADR-002 — Schema separation over database separation (Accepted)
- ADR-003 — Prefixed NanoIDs over UUIDs (Accepted)
- ADR-004 — Direct analytics insert over RabbitMQ (Accepted)
- ADR-005 — Rust for backend services (Accepted)
- ADR-006 — Bare metal + Docker Compose over Kubernetes (Accepted)
- ADR-007 — Keycloak for authentication (Accepted)
- ADR-008 — PostgreSQL trigger for GIS synchronization (Accepted)
- ADR-009 — Monorepo with Cargo and npm workspaces (Accepted)
- ADR-010 — Traefik as edge router (Accepted)
- ADR-011 — React + Vite for web applications (Accepted)
- ADR-012 — React Native + Expo for mobile app (Accepted)
- ADR-013 — Single Dashboard App over separate Partner and Admin apps (Accepted)

---

## 12. Scope

### Included
- Public and authenticated station discovery
- Map-based interface with markers and filtering
- Favorites and reviews
- Partner station management
- Admin platform control
- GIS data enrichment (OpenStreetMap)
- Clickstream analytics

### Explicitly Deferred
- **OCPP** and charging session management
- **Payment and billing**
- **Routing and navigation**
- **Real-time availability** (OCPP-driven)
- **Push notifications**

These are documented in [scope.md](scope.md).

---

## 13. Change Control

To change this constitution:

1. Create an ADR if the change affects architecture, services, or data models
2. Update this document with the new rule
3. Commit both files together
4. Reference the ADR number in the commit message

Small operational changes that do not affect architecture may be updated in [decisions.md](decisions.md) without an ADR.

---

**Last Updated:** 2026-06-05  
**Status:** Active  
**Next Review:** End of Phase 2
