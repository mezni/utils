# Data Model: Backend Core — Schema, Identity & CRUD

**Phase**: 1 (Design & Contracts)

## Entity Relationship Diagram

```
┌──────────────┐       ┌──────────────────────┐
│    users     │ 1───1 │  partner_profiles     │
│  USR-xxxx   │       │  PRT-xxxx             │
└──────┬───────┘       └──────────────────────┘
       │
       │ owner (partner or admin role only)
       │
       ▼
┌──────────────┐       ┌──────────────────────┐
│   stations   │ 1───N │      chargers        │
│  STN-xxxx   │       │  CHG-xxxx            │
└──────────────┘       └───────┬──────────────┘
                               │
                               │ references
                               ▼
                       ┌──────────────────────┐
                       │  connector_types     │
                       │  CNT-xxxx            │
                       └──────────────────────┘
```

## Enumerations

### `user_role`

| Value | Description |
|-------|-------------|
| `admin` | Full platform administrator |
| `partner` | Station owner / operator |
| `driver` | EV driver (mobile app user) |

### `partner_classification`

| Value | Description |
|-------|-------------|
| `business` | Corporate / business partner |
| `private` | Individual / private partner |

### `current_type`

| Value | Description |
|-------|-------------|
| `AC` | Alternating current |
| `DC` | Direct current |

### `charger_status`

| Value | Description |
|-------|-------------|
| `available` | Charger is free and operational |
| `occupied` | Charger is in use |
| `faulted` | Charger has a fault / error |
| `offline` | Charger is disconnected |

## Entities

### User

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `TEXT` | PK, `USR-` prefixed | Semantic identifier |
| `email` | `TEXT` | UNIQUE, NOT NULL | Login email |
| `username` | `TEXT` | UNIQUE, NOT NULL | Display name |
| `password_hash` | `TEXT` | NOT NULL | Argon2id PHC string |
| `role` | `user_role` | NOT NULL | admin / partner / driver |
| `is_test` | `BOOLEAN` | NOT NULL DEFAULT false | Sandbox flag |
| `created_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() | Creation timestamp |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() | Last modification (optimistic lock token) |
| `deleted_at` | `TIMESTAMPTZ` | NULL | Soft-delete marker |

**Validation rules**:
- `email`: valid email format (RFC 5322 simplified)
- `username`: 2-50 characters, alphanumeric + underscores
- `password_hash`: always Argon2id PHC string (never plaintext)
- On registration, raw password: minimum 8 characters

**State transitions**: Active → Removed (soft-delete via `deleted_at`)

**Soft-delete**: Yes. `WHERE deleted_at IS NULL` on all read queries.

---

### Partner Profile

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `TEXT` | PK, `PRT-` prefixed | Semantic identifier |
| `user_id` | `TEXT` | FK → users.id, UNIQUE, NOT NULL | Owning user (must have role=partner) |
| `classification` | `partner_classification` | NOT NULL | business / private |
| `display_name` | `TEXT` | NOT NULL | Public-facing partner name |
| `tax_id` | `TEXT` | NULL | Tax identification number |
| `contact_phone` | `TEXT` | NULL | Contact phone number |
| `is_test` | `BOOLEAN` | NOT NULL DEFAULT false | Sandbox flag |
| `created_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() | Creation timestamp |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() | Last modification (optimistic lock token) |
| `deleted_at` | `TIMESTAMPTZ` | NULL | Soft-delete marker |

**Validation rules**:
- `user_id`: must reference a user with `role = 'partner'`
- `display_name`: 2-100 characters
- `tax_id`: optional, up to 30 characters
- `contact_phone`: optional, valid phone format

**State transitions**: Active → Removed (soft-delete via `deleted_at`)

**Soft-delete**: Yes. `WHERE deleted_at IS NULL` on all read queries.

---

### Station

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `TEXT` | PK, `STN-` prefixed | Semantic identifier |
| `owner_id` | `TEXT` | FK → users.id, NOT NULL | Owning partner or admin |
| `name` | `TEXT` | NOT NULL | Station display name |
| `address` | `TEXT` | NOT NULL | Street address |
| `city` | `TEXT` | NOT NULL | City name |
| `coordinates` | `GEOGRAPHY(Point, 4326)` | NOT NULL | Spatial location (longitude-first) |
| `is_operational` | `BOOLEAN` | NOT NULL DEFAULT true | Operational status |
| `is_test` | `BOOLEAN` | NOT NULL DEFAULT false | Sandbox flag |
| `created_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() | Creation timestamp |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() | Last modification (optimistic lock token) |
| `deleted_at` | `TIMESTAMPTZ` | NULL | Soft-delete marker |

**Validation rules**:
- `owner_id`: must reference a user with `role IN ('partner', 'admin')`; driver-role users rejected
- `name`: 2-150 characters
- `address`: 2-250 characters
- `city`: 2-100 characters
- `coordinates`: longitude -180 to 180, latitude -90 to 90 (application + DB CHECK)

**State transitions**: Active → Removed (soft-delete via `deleted_at`). On removal, all associated chargers are permanently deleted.

**Soft-delete**: Yes. `WHERE deleted_at IS NULL` on all read queries. Deleting a station cascades to permanently delete all its chargers.

**Spatial index**: `CREATE INDEX idx_stations_coordinates ON stations USING GIST (coordinates)`

**Pagination index**: `CREATE INDEX idx_stations_created_at_id ON stations (created_at ASC, id ASC) WHERE deleted_at IS NULL`

---

### Charger

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `TEXT` | PK, `CHG-` prefixed | Semantic identifier |
| `station_id` | `TEXT` | FK → stations.id, NOT NULL | Parent station |
| `connector_type_id` | `TEXT` | FK → connector_types.id, NOT NULL | Connector type reference |
| `power_kw` | `FLOAT8` | NOT NULL, CHECK > 0 | Power rating in kilowatts |
| `current_type` | `current_type` | NOT NULL | AC / DC |
| `status` | `charger_status` | NOT NULL DEFAULT 'available' | Current status |
| `created_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() | Creation timestamp |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() | Last modification (optimistic lock token) |

**Validation rules**:
- `power_kw`: positive float (CHECK > 0)
- `status`: must be one of available/occupied/faulted/offline

**State transitions**: available ↔ occupied ↔ faulted ↔ offline ↔ available (any transition allowed)

**Soft-delete**: No. Chargers are permanently deleted (DELETE, not soft-delete). Per FR-008.

**Pagination index**: `CREATE INDEX idx_chargers_station_created_at_id ON chargers (station_id, created_at ASC, id ASC)`

---

### Connector Type

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `TEXT` | PK, `CNT-` prefixed | Semantic identifier |
| `name` | `TEXT` | UNIQUE, NOT NULL | Type name (e.g., "Type 2 AC") |
| `description` | `TEXT` | NOT NULL | Type description |
| `is_test` | `BOOLEAN` | NOT NULL DEFAULT false | Sandbox flag |
| `created_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() | Creation timestamp |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL DEFAULT NOW() | Last modification (optimistic lock token) |
| `deleted_at` | `TIMESTAMPTZ` | NULL | Soft-delete marker |

**Validation rules**:
- `name`: unique, 2-100 characters
- `description`: 2-500 characters
- Cannot be removed while referenced by existing chargers (FR-009)

**State transitions**: Active → Removed (soft-delete via `deleted_at`)

**Soft-delete**: Yes. `WHERE deleted_at IS NULL` on all read queries. Removal blocked if any charger references this type.

**Pagination index**: `CREATE INDEX idx_connector_types_created_at_id ON connector_types (created_at ASC, id ASC) WHERE deleted_at IS NULL`

---

## Seed Data

All seed records carry `is_test = true`. Deterministic IDs and values.

| Entity | Count | ID Pattern |
|--------|-------|------------|
| Connector Types | 2 | `CNT-seed00000001`, `CNT-seed00000002` |
| Users (partner) | 5 | `USR-seedprt00001` through `USR-seedprt00005` |
| Partner Profiles | 5 | `PRT-seedprt00001` through `PRT-seedprt00005` |
| Admin User | 1 | `USR-seedadmin01` |
| Stations | 100 | `STN-seed00000001` through `STN-seed00000100` |
| Chargers | 300 | `CHG-seed00000001` through `CHG-seed00000300` |

Total: 413 records, all with `is_test = true`.
