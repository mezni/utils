# Sprint 02 — Atomic Tasks

**Status**: TASKS DEFINED
**Date**: 2026-06-24

---

## Task Dependency Graph

```
TASK-001 (create driver-service Cargo project)
    ↓
TASK-002 (define domain layer: Station entity + errors)
    ↓
TASK-003 (implement infrastructure: DB pool + repository)
    ↓
TASK-004 (implement application: get_nearby_stations use-case)
    ↓
TASK-005 (implement presentation: health endpoint)
    ↓
TASK-006 (implement presentation: nearby endpoint + DTOs + validation)
    ↓
TASK-007 (wire routes + main.rs bootstrap)
    ↓
TASK-008 (unit tests)
    ↓
TASK-009 (SQLx prepare check)
    ↓
TASK-010 (integration tests)
```

---

## TASK-001 — Create driver-service Cargo project

| Field | Value |
|-------|-------|
| **Input** | `/source/services/driver-service/` |
| **Output** | Rust binary crate with dependencies |
| **Module Boundary** | `/source/services/driver-service/` |
| **Validation** | `cargo check` compiles |
| **Test** | N/A — project scaffold |
| **Security** | No sensitive deps |

**Files:**
- `Cargo.toml`
- `src/main.rs` (minimal bootstrap)
- Directory structure: `domain/`, `application/`, `infrastructure/`, `presentation/`

---

## TASK-002 — Define domain layer

| Field | Value |
|-------|-------|
| **Input** | Entity definitions from spec |
| **Output** | `domain::station::Station`, `domain::errors::NearbyError` |
| **Module Boundary** | `/source/services/driver-service/src/domain/` |
| **Validation** | Pure structs/enums, no framework deps |
| **Test** | Station construction, error messages |
| **Security** | No external types in domain |

**Files:**
- `src/domain/mod.rs`
- `src/domain/station.rs`
- `src/domain/errors.rs`

---

## TASK-003 — Implement infrastructure layer

| Field | Value |
|-------|-------|
| **Input** | DB connection config, `find_nearby_stations()` SQL |
| **Output** | `PgStationRepository` with `find_nearby` method |
| **Module Boundary** | `/source/services/driver-service/src/infrastructure/` |
| **Validation** | SQLx compile check on query |
| **Test** | N/A (requires live DB for integration) |
| **Security** | Parameterized queries, pool size limits |

**Files:**
- `src/infrastructure/mod.rs`
- `src/infrastructure/db.rs`
- `src/infrastructure/repository.rs`

---

## TASK-004 — Implement application use-case

| Field | Value |
|-------|-------|
| **Input** | `NearbyQuery`, domain types, repository trait |
| **Output** | `GetNearbyStationsUseCase` |
| **Module Boundary** | `/source/services/driver-service/src/application/` |
| **Validation** | Orchestrates: validate params → repo call → return |
| **Test** | Use-case with mock repository |
| **Security** | No SQL in application layer |

**Files:**
- `src/application/mod.rs`
- `src/application/get_nearby_stations.rs`

---

## TASK-005 — Implement health endpoint

| Field | Value |
|-------|-------|
| **Input** | No input |
| **Output** | `GET /api/v1/health` → 200 JSON |
| **Module Boundary** | `/source/services/driver-service/src/presentation/` |
| **Validation** | Response matches spec shape |
| **Test** | `curl localhost:3001/api/v1/health` returns expected |
| **Security** | No auth required for health |

**Files:**
- `src/presentation/health.rs`

---

## TASK-006 — Implement nearby endpoint + DTOs + validation

| Field | Value |
|-------|-------|
| **Input** | Query params, use-case |
| **Output** | `GET /api/v1/stations/nearby` → 200/400 JSON |
| **Module Boundary** | `/source/services/driver-service/src/presentation/` |
| **Validation** | Strict: lat[-90,90], lon[-180,180], radius>0, limit[1,100] |
| **Test** | Edge cases: missing params, out-of-bounds, zero results |
| **Security** | Assume hostile input; never expose DB errors |

**Files:**
- `src/presentation/nearby.rs`
- `src/presentation/dto.rs`

---

## TASK-007 — Wire routes + main.rs

| Field | Value |
|-------|-------|
| **Input** | All handlers |
| **Output** | Running HTTP server on :3001 |
| **Module Boundary** | `/source/services/driver-service/src/` |
| **Validation** | Axum Router with CORS + tracing middleware |
| **Test** | `cargo run` starts on :3001 |
| **Security** | No secret exposure |

**Files:**
- `src/presentation/routes.rs`
- `src/main.rs` (update with router + startup)

---

## TASK-008 — Unit tests

| Field | Value |
|-------|-------|
| **Input** | All modules |
| **Output** | `cargo test` passes |
| **Module Boundary** | All layers |
| **Validation** | Tests for: health shape, param validation, use-case logic, domain |
| **Test** | `cargo test` |
| **Security** | No live DB needed for unit tests |

---

## TASK-009 — SQLx prepare check

| Field | Value |
|-------|-------|
| **Input** | SQL queries in repository |
| **Output** | `cargo sqlx prepare` generates `.sqlx/` |
| **Module Boundary** | Infrastructure |
| **Validation** | `cargo sqlx prepare --check` succeeds |
| **Test** | CI gate |
| **Security** | Hard stop if fails (§14) |

---

## TASK-010 — Integration tests

| Field | Value |
|-------|-------|
| **Input** | Running service + PostgreSQL |
| **Output** | End-to-end validation |
| **Module Boundary** | Cross-layer |
| **Validation** | Full request → response cycle |
| **Test Cases** | Health returns 200; nearby returns data/empty/error |
| **Security** | Error responses don't leak internals |

---

## Parallel Execution

```
TASK-001 ──→ TASK-002 ──→ TASK-003 ──→ TASK-004
                                                ↓
                    TASK-005 ─────────────────┤
                    TASK-006 ─────────────────┤
                                                ↓
                                          TASK-007
                                                ↓
                                          TASK-008
                                                ↓
                                          TASK-009
                                                ↓
                                          TASK-010
```
