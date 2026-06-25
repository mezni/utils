# Sprint 05 — Implementation Plan

**Status**: PLANNED
**Date**: 2026-06-25

---

## 1. Architecture Design

### System Context (Sprint 05 scope)

```
┌──────────────────────────────────────────────┐
│               admin-service :3002             │
│  ┌────────────┐  ┌──────────┐  ┌──────────┐  │
│  │  Partners  │  │ Stations │  │ Chargers │  │
│  │   CRUD     │  │   CRUD   │  │   CRUD   │  │
│  └─────┬──────┘  └────┬─────┘  └────┬─────┘  │
│        │              │             │         │
│        └──────────────┼─────────────┘         │
│                       │                       │
│              SQLx (compile-validated)         │
└───────────────────────┼───────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────┐
│               platform_db                     │
│  ┌──────────────────────────────────┐         │
│  │  ev schema (Sprint 04)           │         │
│  │  ├─ partners                     │         │
│  │  ├─ stations                     │         │
│  │  └─ chargers                     │         │
│  └──────────────────────────────────┘         │
└──────────────────────────────────────────────┘
```

### Service Impact Map

| Service | Port | Impact | Notes |
|---------|------|--------|-------|
| `auth-service` | 3000 | None | Not yet implemented |
| `driver-service` | 3001 | None | No changes |
| `admin-service` | 3002 | ✅ Bootstrapped | New service |

### Dependency Graph

```
Cargo.toml (dependencies)
    ↓
domain/ (entities, validation, nanoid)
    ↓
application/ (use cases)
    ↓
infrastructure/ (db pool, SQLx repositories)
    ↓
presentation/ (routes, handlers, DTOs)
    ↓
main.rs (wiring)
    ↓
SQLx prepare --check
    ↓
Integration tests
```

### Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| axum | 0.8 | HTTP framework |
| tokio | 1 | Async runtime |
| sqlx | 0.8 | Database access (postgres + chrono + uuid) |
| serde / serde_json | 1 | Serialization |
| tower-http | 0.6 | CORS, tracing |
| tracing / tracing-subscriber | 0.1 / 0.3 | Logging |
| thiserror | 2 | Error handling |
| rand | 0.8 | Nanoid generation |
| chrono | 0.4 | Timestamp handling |
| uuid | 1 | Audit field UUIDs |

---

## 2. Testing Strategy

### Unit Tests

| Test ID | Description | Module |
|---------|-------------|--------|
| UT-001 | Partner nanoid format | domain |
| UT-002 | Station nanoid format | domain |
| UT-003 | Charger nanoid format | domain |
| UT-004 | Partner validation | domain |
| UT-005 | Station lat/lon validation | domain |
| UT-006 | Charger count constraints | domain |

### Integration Tests

| Test ID | Description |
|---------|-------------|
| IT-001 | Health endpoint |
| IT-002 | Partner full CRUD lifecycle |
| IT-003 | Station CRUD with partner FK |
| IT-004 | Charger CRUD with station FK |
| IT-005 | Soft-delete hides from list |
| IT-006 | Pagination works |
| IT-007 | Validation rejects bad input |

### Hard Stop Pre-checks

- [ ] No hard deletes (`DELETE FROM`)
- [ ] All queries parameterized (no SQL injection)
- [ ] Business logic in domain/application only
- [ ] SQLx compile validation passes
- [ ] ID format: PREFIX-nanoid(12)
