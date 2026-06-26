# Sprint 02 — Follow-Up

**Status:** ✅ Complete
**Branch:** `sprint/02-auth-db-sqlx-migrations`
**Date:** 2026-06-26

## Delivered

- [x] `shared/bornemap-db` — pool.rs, state.rs, migrator.rs
- [x] `shared/bornemap-db/migrations/202406260001_init_auth.sql` — users, oauth_accounts, refresh_sessions
- [x] Workspace Cargo.toml — added bornemap-db member
- [x] `services/auth-service` — config with DATABASE_URL, main with DB lifecycle, health/ready probes DB
- [x] `infra/docker-compose.yml` — DATABASE_URL env var, port 3000, no version attr
- [x] `QUICKSTART.md` — updated ports and env vars

## Verification

| Check | Result |
|---|---|
| `cargo check` | ✅ |
| `cargo fmt --check` | ✅ |
| `cargo clippy -- -D warnings` | ✅ |
| `cargo build -p auth-service` | ✅ |
| `GET /health/live` | ✅ 200 |
| `GET /health/ready` (DB up) | ✅ 200 |
| Migration: users table | ✅ |
| Migration: oauth_accounts table | ✅ |
| Migration: refresh_sessions table | ✅ |

## Decisions

| Decision | Rationale |
|---|---|
| `bornemap-db` controls all DB access | Clean Architecture — infra layer owns SQL |
| `sqlx::query("SELECT 1")` at startup | Validates DB reachable before binding port |
| `run_migrations` returns `MigrateError` | SQLx v0.8 uses `MigrateError`, not `sqlx::Error` |
| `sqlx` as direct dep of auth-service | Needed for `sqlx::query()` calls in health/main |
| Port 3000 (not 8081) | Aligned with MASTER_PROMPT port range rule |

## Issues

None.
