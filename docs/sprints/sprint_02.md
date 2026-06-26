# Sprint 02 — Auth DB + SQLx + Migrations

**ID:** 02
**Name:** auth-db-sqlx-migrations
**Status:** Planned
**Date:** 2026-06-26

---

## Scope

| Area | Extent |
|---|---|
| `shared/bornemap-db` | Create (pool, state, migrator) |
| `services/auth-service` | Update (config, main, health) |
| `infra/docker-compose.yml` | Update (DATABASE_URL) |
| Workspace | Update (add bornemap-db member) |

## Architecture Mapping

```
auth-service (main)
  ├── AppConfig { database_url }
  ├── create_pool() → PgPool
  ├── SELECT 1 validation
  ├── run_migrations()
  ├── AppState { db: PgPool }
  └── HttpServer → health/live, health/ready

bornemap-db crate:
  ├── pool.rs      → create_pool()
  ├── state.rs     → AppState
  ├── migrator.rs  → run_migrations()
  └── migrations/  → SQLx embedded migrations
```

## Dependency Graph

```
auth-service → bornemap-db → sqlx (postgres)
            → bornemap-auth → bornemap-core
```

## Files to Create / Modify

| # | File | Action |
|---|---|---|
| 1 | `docs/sprints/sprint_02.md` | Create |
| 2 | `Cargo.toml` | Modify (add bornemap-db member) |
| 3 | `shared/bornemap-db/Cargo.toml` | Create |
| 4 | `shared/bornemap-db/src/lib.rs` | Create |
| 5 | `shared/bornemap-db/src/pool.rs` | Create |
| 6 | `shared/bornemap-db/src/state.rs` | Create |
| 7 | `shared/bornemap-db/src/migrator.rs` | Create |
| 8 | `shared/bornemap-db/migrations/202406260001_init_auth.sql` | Create |
| 9 | `services/auth-service/Cargo.toml` | Modify (add bornemap-db) |
| 10 | `services/auth-service/src/config.rs` | Modify (add database_url) |
| 11 | `services/auth-service/src/main.rs` | Modify (DB lifecycle) |
| 12 | `services/auth-service/src/http/health.rs` | Modify (ready probes DB) |
| 13 | `infra/docker-compose.yml` | Modify (DATABASE_URL) |
| 14 | `docs/sprints/sprint_02_followup.md` | Create |

## Implementation Order

1. Sprint plan doc
2. `bornemap-db` crate (no deps on other project crates)
3. Migration SQL file
4. Workspace Cargo.toml
5. Auth-service updates (config, main, health)
6. Docker-compose update
7. Build + test

## Testing Strategy

| Check | Command |
|---|---|
| Workspace compiles | `cargo check` |
| Bornemap-db crate | `cargo check -p bornemap-db` |
| Auth-service | `cargo check -p auth-service` |
| DB connectivity | `cargo run -p auth-service` with Postgres running |
| Health live | `curl localhost:3000/health/live` → 200 |
| Health ready (DB up) | `curl localhost:3000/health/ready` → 200 |
| Health ready (DB down) | → 503 |

## Security Checklist

| Item | Status |
|---|---|
| No secrets in code | ✅ Env-based DATABASE_URL |
| SQL injection safe | ✅ SQLx prepared statements |
| Migration safety | ✅ Idempotent via SQLx migrator |

## UX Notes

N/A — infrastructure sprint.
