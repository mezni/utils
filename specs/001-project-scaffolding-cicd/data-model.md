# Data Model: Project Scaffolding & CI/CD

This document defines the structural entities and relationships in the
BorneMap monorepo scaffolding.

## Entity: Backend Crate

| Attribute | Value | Description |
|-----------|-------|-------------|
| `workspace_path` | `sources/backend/` | Location in monorepo |
| `manifest` | `Cargo.toml` | Rust crate definition |
| `runtime` | `main.rs` | Actix-web server entry point |
| `port` | `8080` | HTTP listener |
| `health_path` | `/api/v1/health` | Liveness endpoint |
| `container_image` | `Dockerfile.dev` | Dev Docker build |

**Relationships**:
- Depends on: PostgreSQL 16+ with PostGIS (runtime)
- Depends on: Cargo workspace manifest (`Cargo.toml` at root)
- Used by: All three frontend apps via REST API

## Entity: Frontend Workspace

| Attribute | Value | Description |
|-----------|-------|-------------|
| `workspace_path` | `sources/frontend/` | Location in monorepo |
| `package_manager` | `pnpm 9` | Workspace orchestrator |
| `workspace_config` | `pnpm-workspace.yaml` | Package definitions |

### Sub-Entities

#### Shared UI Package (`packages/ui/`)

| Attribute | Value |
|-----------|-------|
| `config` | `tailwind.config.ts` — design tokens |
| `components` | `<ScrollableTable/>`, `<SettingsCard/>`, `<SelectSetting/>`, `<ConfirmDeleteModal/>` |
| `export` | Named component + token exports |

**Relationships**:
- Consumed by: All three app targets

#### Admin Portal (`apps/admin-portal/`)

| Attribute | Value |
|-----------|-------|
| `framework` | React + Vite + TypeScript |
| `routes` | `/overview`, `/users`, `/data/*`, `/analytics`, `/security`, `/settings/*` |
| `entry` | `src/main.tsx` |

**Relationships**:
- Depends on: `packages/ui/`
- Depends on: Backend API via REST

#### Partner Dashboard (`apps/partner-dashboard/`)

| Attribute | Value |
|-----------|-------|
| `framework` | React + Vite + TypeScript |
| `routes` | Station list, chargers, profile |
| `entry` | `src/main.tsx` |

**Relationships**:
- Depends on: `packages/ui/`
- Depends on: Backend API (partner-scoped)

#### Mobile Driver (`apps/mobile-driver/`)

| Attribute | Value |
|-----------|-------|
| `framework` | Expo SDK 51 + React Native |
| `runtime` | Expo Go (managed) |
| `entry` | File-based routing with `expo-router` |

**Relationships**:
- Depends on: Backend API (`/api/v1/stations/nearby`)
- Locked deps: `react-native-maps`, `expo-location`, `@gorhom/bottom-sheet`, etc.

## Entity: CI Pipeline

| Workflow | Trigger | Jobs | Runner |
|----------|---------|------|--------|
| `backend.yml` | `sources/backend/**`, `Cargo.toml` | fmt, clippy, test, build | ubuntu-latest |
| `frontend.yml` | `sources/frontend/**` | lint, type-check, build | ubuntu-latest |
| `docker.yml` | `docker-compose.dev.yml`, `Dockerfile.dev` | compose up, health curl, compose down | ubuntu-latest |

**Relationships**:
- `backend.yml` depends on: PostgreSQL service container (CI provisioned)
- All workflows depend on: `actions/checkout@v4`

## Entity: Docker Stack

| Service | Image | Port | Healthcheck |
|---------|-------|------|-------------|
| `postgres` | `postgis/postgis:16-3.4-alpine` | 5432 | `pg_isready` |
| `backend-api` | Built from `Dockerfile.dev` | 8080 | N/A (checked via curl) |

**Relationships**:
- `backend-api` depends on: `postgres` (healthy)
- Shared network via `docker-compose.dev.yml`
