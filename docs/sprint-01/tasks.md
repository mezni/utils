# Sprint 01 — Task Breakdown

## 1. Rust Workspace Setup
- [ ] 1.1 Create workspace `Cargo.toml` with resolver = "2" and all members
- [ ] 1.2 Define workspace-level dependency versions
- [ ] 1.3 Create `Makefile` with dev/build/test/lint targets

## 2. Shared Crates
- [ ] 2.1 `common-auth`: JWT encoding/decoding, Role enum with permission checks
- [ ] 2.2 `common-db`: PgPool creation, migration runner
- [ ] 2.3 `common-errors`: Unified AppError enum with actix-web ResponseError impl
- [ ] 2.4 `common-types`: Shared domain types (Station, Connector, NearbyStation, etc.)
- [ ] 2.5 `common-config`: Environment-based config loader

## 3. Service Skeletons
- [ ] 3.1 auth-service: Cargo.toml + Clean Architecture layers + health endpoint
- [ ] 3.2 admin-service: Cargo.toml + Clean Architecture layers + health endpoint
- [ ] 3.3 driver-service: Cargo.toml + Clean Architecture layers + health endpoint

## 4. Database Migrations
- [ ] 4.1 `0001_enable_extensions.sql`: uuid-ossp, postgis, pgcrypto
- [ ] 4.2 `0002_create_schemas.sql`: users, ev, gis schemas

## 5. Frontend Scaffolds
- [ ] 5.1 admin-dashboard: Vite + React + Tailwind + empty routes/pages
- [ ] 5.2 driver-web: Vite + React + Tailwind + empty routes/pages

## 6. DevOps Tooling
- [ ] 6.1 `docker-compose.yml` with postgis/postgres + 3 services
- [ ] 6.2 `scripts/dev.sh`: Start dev environment
- [ ] 6.3 `scripts/migrate.sh`: Run SQLx migrations
- [ ] 6.4 `scripts/seed.sh`: Placeholder seed script
- [ ] 6.5 `scripts/test.sh`: Run all tests

## 7. CI/CD
- [ ] 7.1 `.github/workflows/rust.yml`: cargo build + test + clippy
- [ ] 7.2 `.github/workflows/frontend.yml`: npm build for both apps
- [ ] 7.3 `.github/workflows/docker.yml`: Docker image build

## 8. Documentation
- [ ] 8.1 `docs/sprint-01/spec.md`
- [ ] 8.2 `docs/sprint-01/plan.md`
- [ ] 8.3 `docs/sprint-01/tasks.md`
- [ ] 8.4 `docs/sprint-01/quickstart.md`
- [ ] 8.5 `docs/architecture.md`
- [ ] 8.6 `docs/database.md`
- [ ] 8.7 `docs/api.md`
- [ ] 8.8 `README.md`

## 9. Verification
- [ ] 9.1 `cargo build --workspace` compiles
- [ ] 9.2 `cargo test --workspace` passes
- [ ] 9.3 `cargo clippy --workspace -- -D warnings` clean
- [ ] 9.4 Frontend builds succeed
