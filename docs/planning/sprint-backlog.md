# Sprint Backlog — Sprint 1.1

**Phase**: 1 — Foundation
**Duration**: 2 weeks
**Goal**: Monorepo compiles, CI runs on every push, all scaffolds in place.
**Status**: 🔴 Not Started

---

## Tasks

### TASK-01 — Initialize monorepo directory structure
**Status**: 🔴 Planned
**Description**: Create the full ev-platform/ directory tree as specified in the constitution.

### TASK-02 — Configure Cargo workspace root
**Status**: 🔴 Planned
**Description**: Root Cargo.toml with workspace members and shared dependency versions.

### TASK-03 — Configure npm workspace
**Status**: 🔴 Planned
**Description**: root package.json with npm workspaces field and scripts, tsconfig.base.json.

### TASK-04 — Create ev-core shared crate
**Status**: 🔴 Planned
**Description**: NanoID generation (new_usr, new_prt, new_stn, new_chg, new_rev, new_evt), shared enums (ConnectorType, ChargerStatus, AvailabilityStatus).

### TASK-05 — Create ev-db shared crate
**Status**: 🔴 Planned
**Description**: PgPool setup (create_pool), pagination structs (OffsetParams, PaginatedResponse).

### TASK-06 — Create ci.yml — full workspace CI
**Status**: 🔴 Planned
**Description**: Rust lint+test (cargo fmt, clippy, test) + Frontend lint+build (npm install, lint, build).

### TASK-07 — Create ci-driver-service.yml
**Status**: 🔴 Planned
**Description**: Path-scoped to services/driver-service and crates/. Includes PostgreSQL test container.

### TASK-08 — Create ci-admin-service.yml
**Status**: 🔴 Planned
**Description**: Path-scoped to services/admin-service and crates/. Includes PostgreSQL test container.

### TASK-09 — Create ci-driver-web.yml
**Status**: 🔴 Planned
**Description**: Path-scoped to apps/driver-web and packages/. npm install, lint, build.

### TASK-10 — Create ci-driver-mobile.yml
**Status**: 🔴 Planned
**Description**: Path-scoped to apps/driver-mobile and packages/. npm install, lint, tsc --noEmit.

### TASK-11 — Create ci-dashboard.yml
**Status**: 🔴 Planned
**Description**: Path-scoped to apps/dashboard and packages/. npm install, lint, build.

### TASK-12 — Create environment file examples
**Status**: 🔴 Planned
**Description**: infra/env/.env.example, driver-service.env.example, admin-service.env.example.

### TASK-13 — Create baseline Docker Compose
**Status**: 🔴 Planned
**Description**: infra/compose/docker-compose.yml with postgres, pgadmin, driver-service, admin-service.

### TASK-14 — Create .gitignore and .dockerignore
**Status**: 🔴 Planned
**Description**: Standard ignores for Rust, Node, IDE files, Docker build artifacts.

---

## Definition of Done

- [ ] `cargo build --all` succeeds
- [ ] `npm install` succeeds
- [ ] All CI workflows pass on a test push
- [ ] Docker Compose starts PostgreSQL cleanly
- [ ] ev-core tests pass (`cargo test -p ev-core`)

---

## Dependencies

| Task | Depends On |
|---|---|
| TASK-06 | TASK-01, TASK-02 |
| TASK-07 | TASK-04, TASK-05 |
| TASK-08 | TASK-04, TASK-05 |
| TASK-13 | TASK-01, TASK-12 |
