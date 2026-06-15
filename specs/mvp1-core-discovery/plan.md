# Implementation Plan: MVP-1 Core Geospatial Discovery

**Branch**: `mvp1-core-discovery` | **Date**: 2026-06-14 | **Spec**: `specs/mvp1-core-discovery/spec.md`

## Summary

Build the end-to-end pipeline for EV charging station discovery in Tunisia: OSM data import → PostGIS storage & query → Rust REST API (driver-service) → Expo mobile map display. Single read-service architecture with mock identity. No auth, no admin.

## Technical Context

**Language/Version**: Rust 1.85+ / TypeScript 5.3

**Primary Dependencies**: actix-web 4, sqlx 0.7 (postgres + chrono), serde, tracing, tokio (Rust) / expo-sdk-54, react-native-maps (Mobile)

**Storage**: PostgreSQL 17 + PostGIS 3.4

**Testing**: cargo test (Rust unit + integration) / manual curl API verification

**Target Platform**: Linux (Docker) / iOS + Android (Expo)

**Project Type**: Web service + Mobile app

**Performance Goals**: API response <150ms, mobile 60fps rendering

**Constraints**: <150ms query latency, no ORM, no auth in MVP-1

**Scale/Scope**: Tunisia single-region, <500 stations expected

## Constitution Check

*GATE: Must pass before implementation. Re-check after design.*

- [X] Database-first: Schema defined in platform-init.sql + functions.sql
- [X] GIS isolation: gis.osm_stations is trigger-synced read cache
- [X] Contract-first: API contracts documented in docs/api-contracts.md
- [X] Mock identity: all records use usr-mvp1-fallback
- [X] No business logic in Rust: geo queries in PostGIS functions
- [X] Single service: driver-service only (ports 3001)
- [X] No microservice sprawl: postgres + driver-service + mobile-app + traefik only

## Project Structure

```
source/
├── apps/
│   ├── shared-mobile/          # TypeScript types & constants
│   │   ├── src/
│   │   │   ├── constants.ts
│   │   │   ├── types.ts
│   │   │   └── index.ts
│   │   ├── package.json
│   │   └── tsconfig.json
│   └── mobile-app/             # Expo app
│       ├── assets/
│       ├── src/
│       │   ├── services/api.ts
│       │   └── presentation/
│       │       ├── screens/MapScreen.tsx
│       │       └── components/StationMarker.tsx
│       ├── App.tsx
│       ├── app.json
│       └── package.json
├── database/
│   └── platform_db/
│       ├── schemas/
│       │   ├── configuration.sql
│       │   ├── inventory.sql
│       │   └── gis.sql
│       ├── functions.sql
│       ├── triggers.sql
│       └── platform-init.sql
├── infra/
│   ├── traefik/
│   │   ├── traefik.yml
│   │   └── dynamic.yml
│   ├── postgres/
│   │   └── init.sql
│   └── docker-compose.yml
├── scripts/
│   ├── import-tunisia-osm.sh
│   └── seed-mvp1-data.sql
└── services/
    ├── Cargo.toml              # Workspace root
    ├── shared/src/             # Domain DTOs, auth, logging
    ├── libs/
    │   ├── db-core/src/        # DB connection pool
    │   └── geo-core/src/       # Tunisia boundary validation
    └── driver-service/src/     # Actix-Web server
        ├── main.rs
        ├── config.rs
        ├── db.rs
        ├── routes/mod.rs
        ├── handlers/mod.rs
        └── models/mod.rs
```

## Implementation Order

### Phase A — Database Layer
1. `platform-init.sql` — extensions, schemas, tables, constraints
2. `functions.sql` — `gis.get_nearby_stations()`
3. `triggers.sql` — `trg_replicate_station_to_gis_cache`

### Phase B — Rust Libraries
1. `libs/geo-core` — Tunisia boundary constants + validation
2. `libs/db-core` — Connection pool setup
3. `services-shared` — DTOs, logging, mock auth

### Phase C — Driver Service
1. Config, DB pool init, route wiring
2. Health endpoint
3. `/api/v1/stations/nearby` endpoint

### Phase D — OSM Import
1. `import-tunisia-osm.sh` — Overpass API fetch + psql insert
2. `seed-mvp1-data.sql` — Demo partners and stations

### Phase E — Shared Mobile Lib
1. Constants, types, index barrel export

### Phase F — Mobile App
1. Expo project init, app.json
2. Map screen with Tunis center
3. API service with 300ms debounce
4. Station markers with `tracksViewChanges = false`

### Phase G — Infrastructure
1. Dockerfiles for each service
2. `docker-compose.yml` — postgis, driver-service, traefik
3. Traefik routing config
4. `.env.example`, `Makefile`
