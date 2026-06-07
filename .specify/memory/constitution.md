# BorneMap Constitution

## Core Principles

### I. Pragmatic Architecture
Use the minimum number of services that correctly separate responsibilities. Do not introduce a new service, worker, or infrastructure component unless no existing component can own the responsibility correctly.

### II. Single Source of Truth
Every entity has exactly one authoritative owner. No ambiguity about where data is written or read from. All other representations are derived.

### III. Simple Operations
The platform must be operable by one person. Every operational task must have a documented runbook. Complexity that cannot be operated simply is not acceptable.

### IV. Domain Separation by Schema
Business data, GIS data, user data, and analytics data are separated by PostgreSQL schema and by service responsibility. Cross-schema writes are forbidden except where explicitly permitted by this constitution.

### V. Build for Current Scale
Introduce complexity only when current scale justifies it. Premature optimization is a constitution violation. Every non-trivial complexity decision requires an ADR.

### VI. Public Access is a First-Class Concern
Anonymous public browsing must always work. Authentication must never be required to view stations, markers, or search results. Auth is only triggered at the moment a gated action is attempted.
### VII. English & French

The platform UI and documentation support English and French. RTL layout and Arabic support are not in scope.

### VIII. Visual Consistency Across All Surfaces
All applications share the same design token foundation. Brand identity, color semantics, spacing, and typography are defined once in packages/ui and consumed everywhere. No hardcoded visual values anywhere in application code.

### IX. API Prefix Consistency
All backend service endpoints are served under the /api/v1 prefix. No endpoint is exposed without this prefix.

## Technology Stack

- **Backend**: Rust + Actix-web, sqlx for database access
- **Frontend Web**: React + Vite + Tailwind CSS + Leaflet
- **Frontend Mobile**: React Native + Expo SDK 54 + react-native-maps
- **Database**: PostgreSQL 16 + PostGIS 3.4
- **Auth**: Keycloak 24
- **Infrastructure**: Bare metal + Docker Compose + Traefik
- **Package Management**: Cargo workspace (Rust), pnpm workspace (JS/TS)
- **CI/CD**: GitHub Actions

## Data Architecture

Four PostgreSQL schemas:
- `inventory` — partners, stations, chargers, availability (owned by Admin Service)
- `users` — user accounts, profiles, favorites, reviews (owned by Driver Service)
- `gis` — OSM data, spatial indexes, station_locations (derived from inventory.station via trigger)
- `analytics` — raw events, aggregates (owned by Clickstream Service)

Cross-schema access rules are enforced by service boundaries. No service writes to a schema it does not own.

## Non-Negotiable Rules

- inventory.station is the source of truth for stations
- gis is never the master of any business entity
- All endpoints served under /api/v1 prefix
- Every service exposes GET /api/v1/health with database check
- Every service runs migrations on startup before accepting requests
- All SQL uses bind parameters — no string interpolation
- Public driver access requires no login at any point
- Only Traefik exposes public ports
- Keycloak owns all authentication
- No visual value hardcoded in any component — tokens only
- Tokens never stored in localStorage or AsyncStorage
- Expo SDK version is 54 — no upgrade without an ADR
- Map tiles use OpenStreetMap — no paid tile provider without an ADR
- Migrations are never edited after commit

## Governance

This constitution supersedes all other practices. Amendments require an ADR with clear rationale and migration plan. All PRs/reviews must verify constitution compliance. Complexity must be justified. Documentation and code comments are written in English. UI text supports English and French.

## Phase 1 — Foundation (Active)

See `docs/planning/planning-bug-tracker.md` for current sprint details.

**Version**: 1.0.0 | **Ratified**: 2026-06-07 | **Last Amended**: 2026-06-07
