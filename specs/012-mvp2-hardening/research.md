# Research: MVP-2 Hardening

**Date**: 2026-06-09 | **Branch**: `012-mvp2-hardening` | **Spec**: [spec.md](./spec.md)

## Architecture Decisions

### Integration Test Strategy for PostgreSQL-Dependent Tests

- **Decision**: Gate integration tests behind `DATABASE_URL` env var. Use `#[ignore]` with a `#[cfg_attr]` or manual skip in test setup. Tests that find `DATABASE_URL` unset print a clear skip message and pass trivially.
- **Rationale**: Workspace must compile and test on machines without PostgreSQL (CI build-only runs, developer machines). Using compile-time `query_as!` would require a live DB at build time — our runtime `query_as::<_, T>()` approach avoids this.
- **Current state**: Both services use `query_as::<_, T>()`. No compile-time macros. Integration tests that exist should already follow this pattern — verify and fix if not.

### Docker Compose Clean-Start Verification

- **Decision**: Use `docker compose down -v` (removes volumes) then `docker compose up --build -d` for zero-state testing. Verify health via `docker compose ps --filter health=healthy` and endpoint curl.
- **Rationale**: `down -v` destroys the pgdata volume, forcing PostgreSQL to initialize from scratch and triggering migrations on first service startup.
- **Verification script**: Write `scripts/verify-zero-state.sh` that automates the clean-start loop and reports pass/fail for each health check.

### Spatial Query Index Verification

- **Decision**: Enable `auto_explain` module in PostgreSQL or manually capture SQL from the nearby endpoint via `log_statement = all`, then run `EXPLAIN ANALYZE` on the captured query.
- **Rationale**: The GIST index on station coordinates was created in migration 0003. Need to confirm the planner uses it (index scan) rather than sequential scan.
- **Alternative**: Add `EXPLAIN (ANALYZE, BUFFERS)` endpoint to Driver Service (dev-only) — rejected as unnecessary complexity. Manual EXPLAIN ANALYZE is sufficient.

### Visibility Rule Integration Tests

- **Decision**: Add integration tests in driver-service that:
  1. Insert a partner with `is_active = false` directly via SQL
  2. Insert stations belonging to that partner
  3. Query the nearby/markers/search endpoints
  4. Assert zero results for that partner's stations
  5. Repeat for `is_verified = false` and `is_live = false`
- **Rationale**: This directly tests the JOIN condition on `inventory.partner` that enforces visibility. These tests must run against a real PostgreSQL instance.
- **Existing coverage**: Driver Service Sprint 2.3 integration tests may already cover this — verify and add tests for any uncovered scenarios.

### Full Product Loop Verification

- **Decision**: Write `scripts/verify-full-loop.sh` that uses `curl` to walk through the complete workflow:
  1. Create partner via Admin Service
  2. Verify partner
  3. Set is_live
  4. Create station
  5. Create chargers
  6. Query Driver Service nearby — confirm station appears
  7. Deactivate partner — confirm station disappears
- **Rationale**: Automatable, reproducible, fast. No UI needed. Can run in CI.

### CI Pipeline Verification

- **Decision**: Verify CI by pushing to the feature branch and checking GitHub Actions status via `gh run list` or the GitHub UI.
- **Rationale**: Workflows are already defined (Sprint 2.5). Need to confirm they trigger correctly on path-scoped changes and all steps pass.
- **Note**: `cargo clippy --all-targets` includes test code coverage (`--all-targets` compiles tests, benches, and examples).

### Existing Patterns

From prior sprints:
- Workspace root: `source/Cargo.toml` with 4 members
- Tests: `#[cfg(test)]` modules in each crate + optional integration tests
- `#[ignore]` convention for tests requiring external services
- Docker Compose v3.8+ at repo root
- GitHub Actions workflows at `.github/workflows/`
- sqlx migrations at `database/migrations/0001-0004`
- Seed data at `database/seeds/001-004`

## Environment Variables Summary

| Variable | Default | Used By |
|----------|---------|---------|
| `DATABASE_URL` | — | Integration tests (skipped if absent) |
| `RUST_LOG` | `info` | Rust services |
| `PORT` | 8080/8081 | Rust services |

No new environment variables needed for this sprint.
