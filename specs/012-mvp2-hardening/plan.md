# Implementation Plan: MVP-2 Hardening

**Branch**: `012-mvp2-hardening` | **Date**: 2026-06-09 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/012-mvp2-hardening/spec.md`

## Summary

Hardening sprint for MVP-2: ensure all tests pass, clippy warnings are zero, Docker Compose starts cleanly from zero, spatial queries use index scans, visibility rules are enforced in integration tests, the full product loop works end-to-end, and all CI pipelines are green on main.

## Technical Context

**Language/Version**: Rust 1.85 (stable toolchain, workspace at source/)

**Primary Dependencies**: sqlx 0.8 with `runtime-tokio`, `postgres`, `chrono` features; actix-web 4; serde; tokio

**Storage**: PostgreSQL 17 + PostGIS 3.5 via `postgis/postgis:17-3.5` Docker image

**Testing**: `cargo test --all` (unit + integration), `cargo clippy --all-targets -- -D warnings`, `docker compose up --build -d` verification, `EXPLAIN ANALYZE` for spatial queries

**Target Platform**: Linux (Docker Engine 24+), GitHub Actions ubuntu-latest

**Project Type**: Web services (2 Rust Actix-web binaries + 2 shared crates)

**Performance Goals**: `cargo test --all` under 5 minutes, Docker Compose up under 120 seconds, spatial queries under 100ms at 10k+ stations

**Constraints**: Must match existing project conventions; no new dependencies; no changes to public API surface; integration tests requiring PostgreSQL must skip gracefully when no DATABASE_URL is available

**Scale/Scope**: 4 crates in workspace (ev-core, ev-db, driver-service, admin-service); 2 Rust services; 1 Docker Compose file; 2 GitHub Actions workflows

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Constitution Status**: Template only — not ratified. No binding governance gates.

| Check | Status | Notes |
|-------|--------|-------|
| Documented governance framework? | ⚠️ Template | Constitution still contains placeholder text |
| Architecture constraints apply? | ❌ No | No ratified constraints |
| Design freedom? | ✅ Yes | Follow existing project conventions |

**Decision**: Proceed with no constitution gates. Follow existing patterns from prior sprints.

## Project Structure

### Documentation (this feature)

```text
specs/012-mvp2-hardening/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (created by /speckit.tasks)
```

### Source Code (repository root)

No new code structure for this sprint. Existing layout from prior sprints:

```text
./
├── docker-compose.yml
├── .github/workflows/
│   ├── driver-service.yml
│   └── admin-service.yml
├── source/
│   ├── services/
│   │   ├── driver-service/
│   │   └── admin-service/
│   ├── crates/
│   │   ├── ev-core/
│   │   └── ev-db/
│   └── Cargo.toml
└── database/
    └── migrations/
```

**Structure Decision**: No structural changes. This sprint adds no new directories, services, or crates. All work is within existing files (fixing bugs, adding tests, updating configuration).

## Complexity Tracking

> No Constitution violations to justify. Skip.
