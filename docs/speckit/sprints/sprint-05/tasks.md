# Sprint 05 — Atomic Tasks

**Status**: TASKS DEFINED
**Date**: 2026-06-25

---

## Task Dependency Graph

```
TASK-001 (write spec + plan + tasks)
    ↓
TASK-002 (create branch)
    ↓
TASK-003 (bootstrap admin-service Cargo.toml + main.rs)
    ↓
TASK-004 (implement domain: entities, nanoid, errors)
    ↓
TASK-005 (implement application: CRUD use cases)
    ↓
TASK-006 (implement infrastructure: db pool + SQLx repos)
    ↓
TASK-007 (implement presentation: routes, handlers, DTOs)
    ↓
TASK-008 (Dockerfile + docker-compose update)
    ↓
TASK-009 (SQLx prepare --check)
    ↓
TASK-010 (integration tests)
    ↓
TASK-011 (delivery artifacts + commit + PR)
```

---

## TASK-001 — Write sprint documentation

| Field | Value |
|-------|-------|
| **Input** | Sprint 05 spec definition |
| **Output** | spec.md, plan.md, tasks.md |
| **Module Boundary** | `/docs/speckit/sprints/sprint-05/` |

---

## TASK-002 — Create branch

| Field | Value |
|-------|-------|
| **Input** | Git repo |
| **Output** | Branch `sprint/05-admin-service-crud` |
| **Validation** | `git branch` shows new branch |

---

## TASK-003 — Bootstrap admin-service

| Field | Value |
|-------|-------|
| **Input** | Cargo.toml template + driver-service patterns |
| **Output** | `/source/services/admin-service/` with Cargo.toml, main.rs, module structure |
| **Module Boundary** | `/source/services/admin-service/` |
| **Validation** | `cargo check` compiles |

---

## TASK-004 — Domain layer

| Field | Value |
|-------|-------|
| **Input** | Sprint 04 ev schema entity definitions |
| **Output** | `domain/` module with entities, nanoid generator, error types |
| **Module Boundary** | `src/domain/` |
| **Validation** | Entity field alignment with DB columns |

---

## TASK-005 — Application layer

| Field | Value |
|-------|-------|
| **Input** | Domain entities + repository trait |
| **Output** | `application/` module with CRUD use cases |
| **Module Boundary** | `src/application/` |
| **Validation** | All CRUD operations covered for 3 entities |

---

## TASK-006 — Infrastructure layer

| Field | Value |
|-------|-------|
| **Input** | DB connection config + SQL queries |
| **Output** | `infrastructure/` with db pool + SQLx repositories |
| **Module Boundary** | `src/infrastructure/` |
| **Validation** | SQLx compile-time validation |

---

## TASK-007 — Presentation layer

| Field | Value |
|-------|-------|
| **Input** | Use cases + DTO definitions |
| **Output** | `presentation/` with routes, handlers, DTOs |
| **Module Boundary** | `src/presentation/` |
| **Validation** | All 16 endpoints wired |

---

## TASK-008 — Dockerfile + docker-compose

| Field | Value |
|-------|-------|
| **Input** | Dockerfile template from driver-service |
| **Output** | `admin-service/Dockerfile`, updated `docker-compose.yml` |
| **Validation** | `docker build` succeeds |

---

## TASK-009 — SQLx validation

| Field | Value |
|-------|-------|
| **Input** | All SQL queries in repository |
| **Output** | `cargo sqlx prepare --check` passes |
| **Validation** | No SQLx errors |

---

## TASK-010 — Integration tests

| Field | Value |
|-------|-------|
| **Input** | Running admin-service + DB |
| **Output** | Verified CRUD lifecycle |
| **Test Cases** | See plan §2 |

---

## TASK-011 — Delivery artifacts

| Field | Value |
|-------|-------|
| **Input** | All completed work |
| **Output** | SYSTEM_STATE.md, sprint_state.json, validation_report.md, sprint_review.md, follow_up.md |

---

## Execution Order

```
TASK-001 → TASK-002 → TASK-003 → TASK-004 → TASK-005 → TASK-006 → TASK-007 → TASK-008 → TASK-009 → TASK-010 → TASK-011
```

## Hard Stop Conditions

- [ ] Hard delete in any code path → HALT
- [ ] Direct SQL string in handler → HALT
- [ ] Business logic outside domain/application → HALT
- [ ] SQLx validation fails → HALT (§14)
- [ ] ID format rule violated → HALT (§2.4)
- [ ] Scope expansion → HALT
