# Sprint 04 — Implementation Plan

**Status**: PLANNED
**Date**: 2026-06-25

---

## 1. Architecture Design

### System Context (Sprint 04 scope)

```
┌─────────────────────────┐
│    platform_db          │
│  ┌───────────────────┐  │
│  │ gis (Sprint 01)   │  │  ← ingestion layer (driver-service)
│  │ └─ osm_charging_  │  │
│  │    stations       │  │
│  └───────────────────┘  │
│  ┌───────────────────┐  │
│  │ ev (Sprint 04)    │  │  ← business domain (admin-service)
│  │ ├─ lookup tables  │  │
│  │ ├─ partners       │  │
│  │ ├─ stations       │  │
│  │ └─ chargers       │  │
│  └───────────────────┘  │
└─────────────────────────┘
          ▲
          │ GIS → EV migration (idempotent)
          │
┌─────────────────────────┐
│  gis.osm_charging_      │
│  stations               │
└─────────────────────────┘
```

### Service Impact Map

| Service | Port | Impact | Notes |
|---------|------|--------|-------|
| `auth-service` | 3000 | None | No changes |
| `driver-service` | 3001 | None | No changes |
| `admin-service` | 3002 | Future owner of `ev` schema | Schema exists, service TBD |

### Migration Dependency Graph

```
001_create_schema.sql
    ↓
002_lookup_tables.sql
    ↓
003_create_partners.sql
    ↓
004_create_stations.sql
    ↓
005_create_chargers.sql
    ↓
006_migrate_gis_to_ev.sql
```

### External Dependencies

| Dependency | Purpose | Version |
|------------|---------|---------|
| PostgreSQL | Database | 16+ |
| PostGIS | GEOGRAPHY type, spatial index | 3.x |
| hstore | Key-value tags | 16+ (contrib) |
| pgcrypto | Random ID generation | 16+ (contrib) |

---

## 2. DB Schema Changes

### Migration 001 — Create `ev` schema + extensions

Creates the `ev` namespace and enables required PostgreSQL extensions.

### Migration 002 — Create EV lookup tables

Creates reference/lookup tables with seed data:
- `ev.access_types` (public, restricted, private)
- `ev.data_sources` (osm, partner, manual)
- `ev.connector_types` (CCS, CHAdeMO, Type2, Type1, etc.)
- `ev.current_types` (AC, DC)
- `ev.connector_statuses` (available, in_use, offline, faulted)

### Migration 003 — Create `ev.partners`

Operator/partner table with OPR-nanoid(12) identity and audit columns.

### Migration 004 — Create `ev.stations`

Station table with STA-nanoid(12), PostGIS geography column, GIST spatial index, HSTORE tags.

### Migration 005 — Create `ev.chargers`

Charger/connector table with CHG-nanoid(12), FK constraints, count validation, unique connector constraint.

### Migration 006 — GIS → EV data migration

Idempotent migration from `gis.osm_charging_stations` to `ev.stations` with PostGIS point conversion.

---

## 3. Testing Strategy

### SQL Validation Tests

| Test ID | Description | Type |
|---------|-------------|------|
| T-001 | Verify all 5 lookup tables created with correct columns | Schema |
| T-002 | Verify ev.partners has OPR- prefix ID | Identity |
| T-003 | Verify ev.stations has STA- prefix ID and location column | Identity |
| T-004 | Verify ev.chargers has CHG- prefix ID | Identity |
| T-005 | Verify spatial index exists on ev.stations.location | Spatial |
| T-006 | Verify charger count constraints | Constraint |
| T-007 | Verify FK: chargers → stations → partners | Integrity |
| T-008 | Verify GIS → EV migration idempotent | Migration |
| T-009 | Verify soft-delete filtering | Behavior |

### Hard Stop Pre-checks

- [ ] No UUID as entity ID
- [ ] Spatial index present
- [ ] Soft-delete on all entities
- [ ] Schema ownership documented
- [ ] SQL syntax valid
- [ ] Idempotent migrations
