# BorneMap Onboarding

## Quick Start

1. `cp .env.example .env`
2. `make up` (starts PostGIS)
3. `make dev-api` (starts api-service on :8080)
4. `cd apps/mobile-driver && npx expo start`

## Project Layout

- `backend/` — Rust multi-crate workspace
- `apps/mobile-driver/` — React Native / Expo Go mobile app
- `apps/web-admin/` — React admin portal
- `db/` — Migrations and seed data
- `deployments/` — Production Docker Compose and nginx config
- `docs/` — Architecture docs and runbooks
- `specs/` — Feature specifications

## Make Commands

- `make up` — Start PostGIS
- `make down` — Stop PostGIS
- `make status` — Check container status
- `make test-backend` — Run cargo test
- `make dev-api` — Run api-service
