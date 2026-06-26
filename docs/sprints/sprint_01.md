# Sprint 01 — Repo Bootstrap & Auth-Service Skeleton

**ID:** 01  
**Name:** repo-bootstrap-auth  
**Status:** Planned  
**Date:** 2026-06-26  

---

## Scope

| Area | Extent |
|---|---|
| `shared/bornemap-core` | Create |
| `shared/bornemap-auth` | Create |
| `services/auth-service` | Create |
| `infra/` | Create |
| Workspace root | Create |

## Architecture Mapping

Service layers (auth-service):

```
http/health.rs (GET /health/live, GET /health/ready)
    ↓
main.rs (Actix HttpServer + config)
    ↓
config.rs (AppConfig::from_env)
```

Dependency graph:

```
auth-service → bornemap-auth → bornemap-core
                                   ↓
                             thiserror, serde, uuid
```

## Files to Create

| # | File | Layer |
|---|---|---|
| 1 | `Cargo.toml` | Workspace |
| 2 | `shared/bornemap-core/Cargo.toml` | Shared |
| 3 | `shared/bornemap-core/src/lib.rs` | Shared |
| 4 | `shared/bornemap-auth/Cargo.toml` | Shared |
| 5 | `shared/bornemap-auth/src/lib.rs` | Shared |
| 6 | `services/auth-service/Cargo.toml` | Service |
| 7 | `services/auth-service/src/config.rs` | Service |
| 8 | `services/auth-service/src/http/mod.rs` | Service |
| 9 | `services/auth-service/src/http/health.rs` | Service |
| 10 | `services/auth-service/src/main.rs` | Service |
| 11 | `services/auth-service/Dockerfile` | Infra |
| 12 | `infra/docker-compose.yml` | Infra |
| 13 | `.env.example` | Root |

## Implementation Order

1. Workspace Cargo.toml
2. `bornemap-core` (foundational, no deps on other project crates)
3. `bornemap-auth` (depends on core)
4. `auth-service` (depends on core + auth)
5. Infra files (Dockerfile, compose)
6. `.env.example`

## Testing Strategy

| Check | Command |
|---|---|
| Workspace compiles | `cargo check` |
| Single package | `cargo check -p bornemap-core` |
| Auth crate | `cargo check -p bornemap-auth` |
| Service binary | `cargo run -p auth-service` |
| Health live | `curl localhost:8081/health/live` → 200 |
| Health ready | `curl localhost:8081/health/ready` → 200 |

## Security Checklist

| Item | Status |
|---|---|
| No secrets in code | ✅ `.env.example` only, no real secrets |
| No hardcoded credentials | ✅ Config from env |
| Minimal dependencies | ✅ 5 crates, no unused deps |

## UX Notes

N/A — no frontend changes in this sprint.

## Known Issues

- Dockerfile uses `rust:1.75` but Cargo.toml specifies `edition = "2024"` which requires Rust ≥ 1.85. Build may fail on older hosts.
