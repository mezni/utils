# CI Workflow Contract

## Workflow Registry

| File | Scope | Trigger Paths | Jobs |
|---|---|---|---|
| `ci.yml` | Full workspace | Any push/PR | rust-check (fmt, clippy, test), frontend-check (lint, build) |
| `ci-driver-service.yml` | Driver Service | `services/driver-service/**`, `crates/**` | test (with PostgreSQL container) |
| `ci-admin-service.yml` | Admin Service | `services/admin-service/**`, `crates/**` | test (with PostgreSQL container) |
| `ci-driver-web.yml` | Driver Web | `apps/driver-web/**`, `packages/**` | lint, build |
| `ci-driver-mobile.yml` | Driver Mobile | `apps/driver-mobile/**`, `packages/**` | lint, tsc --noEmit |
| `ci-dashboard.yml` | Dashboard | `apps/dashboard/**`, `packages/**` | lint, build |

## Caching Contract

- All frontend workflows MUST use `actions/cache` with key `${{ runner.os }}-npm-${{ hashFiles('package-lock.json') }}`
- Rust workflows cache via `Swatinem/rust-cache@v2`
- Restore keys: `${{ runner.os }}-npm-`

## PostgreSQL Service Container Contract

- Image: `postgis/postgis:16-3.4`
- Port: `5432`
- Health check: `pg_isready -U postgres`
- Env: `POSTGRES_USER=postgres`, `POSTGRES_PASSWORD=postgres`, `POSTGRES_DB=ev_platform_test`
- Used by: `ci-driver-service.yml`, `ci-admin-service.yml`
