# CI/CD Workflow Contracts

**Date**: 2026-06-09 | **Branch**: `011-docker-compose-ci` | **Directory**: `.github/workflows/`

## Workflow: driver-service.yml

**Purpose**: Build, test, and lint driver-service on relevant changes

| Property | Value |
|----------|-------|
| Trigger | `pull_request`, `push` to `009-driver-service` branch |
| Path filter | `source/services/driver-service/**`, `source/crates/**` |
| Runner | `ubuntu-latest` |
| Timeout | 15 minutes |

**Steps**:
1. Checkout repository
2. Install Rust toolchain (stable) with clippy
3. Cache Cargo registry + target (restore on match, save on miss)
4. Run `cargo build --package driver-service`
5. Run `cargo test --package driver-service`
6. Run `cargo clippy --package driver-service -- -D warnings`
7. (Optional) Build Docker image

## Workflow: admin-service.yml

**Purpose**: Build, test, and lint admin-service on relevant changes

| Property | Value |
|----------|-------|
| Trigger | `pull_request`, `push` to `010-admin-service` branch |
| Path filter | `source/services/admin-service/**`, `source/crates/**` |
| Runner | `ubuntu-latest` |
| Timeout | 15 minutes |

**Steps**:
1. Checkout repository
2. Install Rust toolchain (stable) with clippy
3. Cache Cargo registry + target (restore on match, save on miss)
4. Run `cargo build --package admin-service`
5. Run `cargo test --package admin-service`
6. Run `cargo clippy --package admin-service -- -D warnings`
7. (Optional) Build Docker image

## Notes

- Both workflows are needed because path-scoped triggers are per-workflow, not per-job
- `source/crates/**` is included in both triggers since shared crates affect both services
- Docker build is optional (step 7) — can be gated behind a manual trigger or branch condition
- No database needed for build/test steps (sqlx uses non-compile-time `query_as::<_, T>()`, not `query_as!`)
