# Data Model: Admin Service (Sprint 1.1)

**Date**: 2026-06-19 | **Phase**: 1 — Design & Contracts | **Schema**: inventory

## Entity Relationship Diagram

```
┌─────────────┐       ┌─────────────┐       ┌─────────────┐
│   partners  │──1:N──│   stations  │──1:N──│   chargers  │
│   (OPR-*)   │       │   (STA-*)   │       │   (CHG-*)   │
└─────────────┘       └─────────────┘       └─────────────┘
     │                      │                      │
     │ ON DELETE SET NULL   │ ON DELETE CASCADE    │
     │ (soft: partner_id    │ (soft: propagate     │
     │  remains, partner    │  deleted_at to       │
     │  hidden via          │  chargers when       │
     │  deleted_at)         │  station deleted)    │
     └──────────────────────┴──────────────────────┘

Lookup Tables (ENUM — seed only, no CRUD in Sprint 1.1):
access_types ──── stations (via access_type_id, future sprint)
data_sources ──── stations (via data_source_id, future sprint)
connector_types ─ chargers (via connector_type_id)
current_types ─── chargers (via current_type_id)
connector_statuses ─ chargers (via status_id)
```

## Core Tables

### partners

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | VARCHAR(32) | PRIMARY KEY, CHECK(~'^OPR-[A-Za-z0-9_-]{12}$') | Canonical nanoid identifier |
| name | VARCHAR(255) | NOT NULL | Organization name |
| network_type | VARCHAR(20) | NOT NULL, CHECK('INDIVIDUAL'/'COMPANY') | Network classification |
| support_phone | VARCHAR(50) | NULLABLE | Contact phone |
| support_email | VARCHAR(255) | NULLABLE | Contact email |
| is_verified | BOOLEAN | DEFAULT FALSE | Verification flag |
| deleted_at | TIMESTAMPTZ | NULLABLE | Soft delete timestamp |
| created_at | TIMESTAMPTZ | DEFAULT NOW() | Creation timestamp |
| updated_at | TIMESTAMPTZ | DEFAULT NOW() | Last update timestamp (auto-update) |

### stations

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | VARCHAR(32) | PRIMARY KEY, CHECK(~'^STA-[A-Za-z0-9_-]{12}$') | Canonical nanoid identifier |
| partner_id | VARCHAR(32) | NULLABLE, REFERENCES partners(id) ON DELETE SET NULL | Owning partner |
| name | VARCHAR(255) | NOT NULL | Station name |
| address | TEXT | NULLABLE | Physical address |
| location | GEOGRAPHY(Point, 4326) | NOT NULL | Spatial location (GIST index) |
| deleted_at | TIMESTAMPTZ | NULLABLE | Soft delete timestamp |
| created_at | TIMESTAMPTZ | DEFAULT NOW() | Creation timestamp |
| updated_at | TIMESTAMPTZ | DEFAULT NOW() | Last update timestamp |
| created_by | VARCHAR(36) | NULLABLE | User who created (future auth) |
| updated_by | VARCHAR(36) | NULLABLE | User who last updated |

### chargers

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | VARCHAR(32) | PRIMARY KEY, CHECK(~'^CHG-[A-Za-z0-9_-]{12}$') | Canonical nanoid identifier |
| station_id | VARCHAR(32) | NOT NULL, REFERENCES stations(id) ON DELETE CASCADE | Parent station |
| connector_type_id | BIGINT | NOT NULL, REFERENCES connector_types(id) | Connector standard (Type2/CCS/CHAdeMO) |
| current_type_id | BIGINT | NOT NULL, REFERENCES current_types(id) | Current type (AC/DC) |
| status_id | BIGINT | NOT NULL, REFERENCES connector_statuses(id) | Operational status |
| power_kw | DECIMAL(5,2) | NULLABLE | Max power output (kW) |
| voltage | INT | NULLABLE | Voltage rating |
| amperage | INT | NULLABLE | Current rating |
| count_available | INT | DEFAULT 1, CHECK(>= 0) | Available connectors |
| count_total | INT | DEFAULT 1, CHECK(>= 1 AND >= count_available) | Total connectors |
| deleted_at | TIMESTAMPTZ | NULLABLE | Soft delete timestamp |
| created_at | TIMESTAMPTZ | DEFAULT NOW() | Creation timestamp |
| updated_at | TIMESTAMPTZ | DEFAULT NOW() | Last update timestamp |
| created_by | VARCHAR(36) | NULLABLE | User who created |
| updated_by | VARCHAR(36) | NULLABLE | User who last updated |

**Unique Constraint**: (station_id, connector_type_id, current_type_id) — one charger type per station per current type.

## Lookup Tables (Seed Only)

### access_types
| id (SERIAL) | name | description |
|-------------|------|-------------|
| 1 | public | Open to all drivers |
| 2 | restricted | Limited access (e.g., customers only) |
| 3 | private | Not publicly accessible |

### data_sources
| id (SERIAL) | name | description |
|-------------|------|-------------|
| 1 | manual | Manually entered by admin |
| 2 | osm | Imported from OpenStreetMap |
| 3 | partner | Provided by partner/operator |

### connector_types
| id (SERIAL) | name | description |
|-------------|------|-------------|
| 1 | Type2 | Standard AC connector (IEC 62196 Type 2) |
| 2 | CCS | Combined Charging System (DC fast) |
| 3 | CHAdeMO | CHAdeMO DC fast charging |
| 4 | Tesla | Tesla proprietary connector |

### current_types
| id (SERIAL) | name | description |
|-------------|------|-------------|
| 1 | AC | Alternating current |
| 2 | DC | Direct current |

### connector_statuses
| id (SERIAL) | name | description |
|-------------|------|-------------|
| 1 | available | Connector is free to use |
| 2 | occupied | Connector is in use |
| 3 | offline | Connector is out of service |
| 4 | unknown | Status cannot be determined |

## Identity & Uniqueness Rules

- All primary IDs use format: `<PREFIX>-nanoid(12)` (e.g., OPR-k8F3aZ91LmQx)
- Enforced via `CHECK (id ~ '^(OPR|STA|CHG)-[A-Za-z0-9_-]{12}$')` on each table
- IDs generated server-side via shared nanoid(12) utility
- chargers have unique constraint on (station_id, connector_type_id, current_type_id)
- partner name has no uniqueness constraint (multiple partners can share a name)

## Lifecycle / State Transitions

### Soft Deletion
```
ACTIVE ──DELETE──→ SOFT-DELETED
  │                    │
  │ [NO RESTORE API]   │
  │                    ↓
  │              DB-level recovery only
  │
  └──PATCH──→ UPDATED (field-level changes)
```

- DELETE sets `deleted_at = NOW()` — record is hidden from all read queries
- Read queries filter `WHERE deleted_at IS NULL`
- No restore endpoint — recovery requires DBA intervention
- Unique constraints remain active for soft-deleted records (prevents ID reuse)
- Logical cascade: soft-deleting a station propagates `deleted_at` to its chargers
- Partner deletion does NOT cascade to stations (partner_id set to NULL on stations is NOT done for soft delete — station retains partner_id but partner is hidden)

### Update Semantics
- All updates use PATCH semantics — only provided fields are modified
- `updated_at` is set to `NOW()` on every successful update
- Null fields in PATCH request are treated as "no change" (not "set to null")
- To clear an optional field, client must send explicit null value
