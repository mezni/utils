# Sprint 01 — Follow-Up

**Status:** ✅ Complete  
**Branch:** `sprint/01-repo-bootstrap-auth`  
**Date:** 2026-06-26  

## Delivered

- [x] Workspace `Cargo.toml` with resolver 2
- [x] `shared/bornemap-core` — domain types, AppError
- [x] `shared/bornemap-auth` — JWT validator stub
- [x] `services/auth-service` — Actix Web, health endpoints, config
- [x] `infra/docker-compose.yml` — postgres:16 + redis:7 + auth-service
- [x] `services/auth-service/Dockerfile`
- [x] `.env.example`
- [x] `QUICKSTART.md`

## Verification

| Check | Result |
|---|---|
| `cargo check` | ✅ Pass |
| `cargo run -p auth-service` | ✅ Starts on :8081 |
| `GET /health/live` | ✅ 200 |
| `GET /health/ready` | ✅ 200 |

## Decisions

| Decision | Rationale |
|---|---|
| Edition 2024 | Using latest Rust for modern features |
| Port 8081 for auth-service | Avoids conflict with common :3000 usage |
| JwtValidator as stub | Real validation in later sprint when crypto deps are added |
| Single workspace Cargo.toml | Clean dependency management across crates |

## Issues

| Issue | Status |
|---|---|
| Dockerfile rust:1.75 incompatible with edition 2024 | 🔴 Noted — needs update to rust:1.96 |
| First build slow (182 crate downloads) | 🟡 One-time cold start |

## Next

- Sprint 02: Auth-service DB integration + real JWT
