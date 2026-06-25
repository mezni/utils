# Sprint 05 — System State

**Date**: 2026-06-25
**Branch**: `sprint/05-admin-service-crud`

---

## Service Topology

| Service | Port | Status | Schema Ownership |
|---------|------|--------|-----------------|
| `auth-service` | 3000 | ⏳ Not implemented | — |
| `driver-service` | 3001 | ✅ Running | `gis` |
| `admin-service` | 3002 | ✅ Bootstrapped | `ev` |

## Service: `admin-service`

- **Location**: `/source/services/admin-service/`
- **Port**: 3002
- **Architecture**: Clean Architecture (domain → application → infrastructure → presentation)
- **Database**: `platform_db` → `ev` schema

### Layer Structure

| Layer | Files | Description |
|-------|-------|-------------|
| `domain/` | partner.rs, station.rs, charger.rs, nanoid.rs, errors.rs | Pure domain logic, validation, entities |
| `application/` | partner_use_cases.rs, station_use_cases.rs, charger_use_cases.rs | Use-case orchestration |
| `infrastructure/` | db.rs, repository.rs | SQLx pool, CRUD repositories |
| `presentation/` | routes.rs, health.rs, partner_handler.rs, station_handler.rs, charger_handler.rs, dto.rs | HTTP handlers, DTOs, routing |
| `lib.rs` | — | Library crate for integration tests |
| `tests/api_test.rs` | — | 7 integration tests |

### API Endpoints (16 total)

- `GET /api/v1/health`
- Partners: CRUD (5 endpoints)
- Stations: CRUD (5 endpoints)
- Chargers: CRUD (5 endpoints)

### Identity Compliance

| Entity | Prefix | Format | Status |
|--------|--------|--------|--------|
| Partner | `OPR` | `OPR-` + nanoid(12) | ✅ |
| Station | `STA` | `STA-` + nanoid(12) | ✅ |
| Charger | `CHG` | `CHG-` + nanoid(12) | ✅ |

### Soft Delete

All 3 entities enforce soft-delete (deleted_at IS NULL filter, UPDATE on delete).

### Known Issues

| ID | Issue | Status |
|----|-------|--------|
| KNOWN-001 | Test stations leaking | Not applicable (no tests) |
| KNOWN-002 | Missing `deleted_at` | ✅ Enforced |
| KNOWN-003 | Duplicate nearby endpoint | Not applicable |
| KNOWN-004 | CI grep brittle | Not applicable |

## Infrastructure

- **PostgreSQL**: Running on port 5432 (postgis/postgis:16-3.4)
- **docker-compose.yml**: Includes admin-service on port 3002
