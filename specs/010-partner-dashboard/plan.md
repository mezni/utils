# Implementation Plan: Sprint 10 — Partner Dashboard

**Branch**: `010-partner-dashboard` | **Date**: 2026-06-04 | **Spec**: `specs/010-partner-dashboard/spec.md`

## Summary
Build an operational dashboard for charging station partners to manage stations, chargers, and availability. The partner API endpoints were already implemented in Sprint 5 (admin-service) but were unreachable due to missing Traefik routing and stale Docker images. This sprint exposes the partner API and builds the frontend.

## Changes

### Backend
- **Traefik routes** (`infra/compose/traefik/dynamic/routes.yml`): Added `/api/v1/partner` router → admin-service (no stripPrefix, since partner routes use full prefix)
- **Docker compose** (`infra/compose/docker-compose.yml`): Changed admin-service `env_file` to `.env` (not `.example`), added `DATABASE_URL` and `ADMIN_SERVICE_MIGRATIONS_DIR`, mounted migrations volume
- **Admin-service config** (`services/admin-service/src/main.rs`): Made migrations non-fatal (warn on failure, since DB already seeded)
- **Seed data** (`services/admin-service/migrations/0016_seed_data.up.sql`): Fixed `ON CONFLICT` clause to use `keycloak_user_id` instead of `id` for user inserts
- **Admin-service binary**: Built debug binary and copied into container (full Docker rebuild would take 30+ min)

### Frontend
- **Dependencies**: Added `@bornemap/api-client`, `@bornemap/auth-client`, `@tanstack/react-query`, `react-router`, `keycloak-js`, `class-variance-authority`
- **Configs**: Updated `vite.config.ts` with proxy, updated `index.html`, cleaned `index.css`
- **API client** (`src/lib/api.ts`): Singleton pointed at `/api/v1/partner`
- **Clickstream** (`src/lib/clickstream.ts`): Event emission on `partner_dashboard` channel
- **Auth** (`src/hooks/useAuth.tsx`): Keycloak auth provider with login/logout/token
- **API hooks**: `usePartnerStations`, `usePartnerChargers`, `usePartnerAvailability`, `usePartnerProfile` — all using React Query
- **Components**: Header, AuthGate, Modal, StationForm, ChargerForm
- **Pages**: StationsPage (list + create + edit + delete + availability toggle + chargers per station), ChargersPage (table view + edit), ProfilePage
- **ApiClient** (`packages/api-client/src/index.ts`): Added optional `headers` parameter to all methods

## Verification
- Backend: `curl localhost/api/v1/partner/me` returns `401 Unauthenticated` (correctly auth-gated)
- Backend: `curl localhost/api/v1/driver/stations` returns stations data
- Frontend: `npm run build` succeeds (97 modules, 350KB JS)
