# Data Model: Backend Services

**Feature**: `002-backend-services` | **Date**: 2026-06-11

## Entity: Partner

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| id | VARCHAR(50) PK | Format: `PRT-{nanoid}` | Server-generated |
| name | VARCHAR(255) | NOT NULL, UNIQUE | |
| contact_email | VARCHAR(255) | NOT NULL | |
| created_at | TIMESTAMP | NOT NULL, DEFAULT NOW() | |
| updated_at | TIMESTAMP | NOT NULL, DEFAULT NOW() | |

**Relationships**: A partner has many stations (`partner_id` FK on station)

**API Usage**: Partner querying is out of scope for Phase 2. Partners are created via seed data. Station create references an existing `partner_id`.

---

## Entity: Station

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| id | VARCHAR(50) PK | Format: `STA-{nanoid}` | Server-generated |
| name | VARCHAR(255) | NOT NULL | |
| address | VARCHAR(255) | NOT NULL | |
| lat | DOUBLE PRECISION | NOT NULL, range [-90, 90] | Plain double for easy serialization |
| lng | DOUBLE PRECISION | NOT NULL, range [-180, 180] | Plain double for easy serialization |
| location | GEOMETRY(Point, 4326) | GENERATED ALWAYS AS (ST_Point(lng, lat)) STORED | Auto-computed; not in API payload |
| status | VARCHAR(20) | NOT NULL, DEFAULT 'offline' | One of: available, busy, offline, maintenance |
| opening_hours | VARCHAR(255) | Nullable | Free-text format (e.g., "06:00-23:00") |
| partner_id | VARCHAR(50) | NOT NULL, FK → inventory.partner(id) | Required in create request |
| created_at | TIMESTAMP | NOT NULL, DEFAULT NOW() | |
| updated_at | TIMESTAMP | NOT NULL, DEFAULT NOW() | |
| deleted_at | TIMESTAMP | Nullable | Soft-delete: NULL = active, SET = deleted |

**Indexes**: GIST on location (WHERE deleted_at IS NULL), BTREE on partner_id and status (partial)

**Relationships**:
- Belongs to Partner (`partner_id`)
- Has many Chargers (`station_id` on charger)

**State transitions**:
- `available` → `busy`, `offline`, `maintenance`
- `busy` → `available`, `offline`, `maintenance`
- `offline` → `available`, `maintenance`
- `maintenance` → `available`, `offline`
- Any status → soft-deleted (`deleted_at = NOW()`)

---

## Entity: Charger

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| id | VARCHAR(50) PK | Format: `CHR-{nanoid}` | Server-generated |
| station_id | VARCHAR(50) | NOT NULL, FK → inventory.station(id) | |
| type | VARCHAR(20) | NOT NULL | One of: CCS2, CHAdeMO, Type2 |
| power_kw | FLOAT | NOT NULL, > 0 | |
| status | VARCHAR(20) | NOT NULL, DEFAULT 'offline' | One of: available, busy, offline, maintenance |
| price_per_kwh | FLOAT | NOT NULL, DEFAULT 0, >= 0 | |
| created_at | TIMESTAMP | NOT NULL, DEFAULT NOW() | |
| updated_at | TIMESTAMP | NOT NULL, DEFAULT NOW() | |
| deleted_at | TIMESTAMP | Nullable | Soft-delete |

**Relationships**: Belongs to Station (`station_id`)

**Constraints**: A station MUST have at least one charger (validated at create).

---

## Entity: Event (Analytics)

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| id | BIGSERIAL PK | Auto-increment | Server-assigned |
| event_type | VARCHAR(50) | NOT NULL | Required in payload |
| session_id | VARCHAR(50) | NOT NULL | Required in payload |
| user_id | VARCHAR(50) | Nullable | Optional |
| payload | JSONB | NOT NULL | Arbitrary JSON; default empty object |
| occurred_at | TIMESTAMP | NOT NULL | Required in payload (ISO 8601 UTC) |
| ingested_at | TIMESTAMP | NOT NULL, DEFAULT CURRENT_TIMESTAMP | Server-set |
| client_ip | INET | Nullable | Optional, from request |

**Table**: `analytics_db.public.raw_events`

**Append-Only**: RULEs prevent UPDATE and DELETE on all rows.

**Validation minimum fields**: event_type, session_id, occurred_at (FR-016).

---

## Validation Rules

### Station Create (POST /api/v1/stations)
| Field | Rule |
|-------|------|
| name | Required, non-empty, max 255 chars |
| address | Required, non-empty, max 255 chars |
| lat | Required, range [-90, 90] |
| lng | Required, range [-180, 180] |
| partner_id | Required, must reference existing partner |
| chargers | Required array, min 1, max 20 |
| chargers[].type | Required, one of: CCS2, CHAdeMO, Type2 |
| chargers[].power_kw | Required, > 0, max 1000 |
| chargers[].price_per_kwh | Optional, default 0, >= 0 |

### Station Update (PUT /api/v1/stations/{id})
- All fields optional (partial update)
- Only provided fields are updated
- Cannot update `id`, `created_at`, `deleted_at`
- Charger management via separate endpoint (out of scope for Phase 2)

### Station Soft-Delete (DELETE /api/v1/stations/{id})
- Sets `deleted_at = NOW()`
- Soft-deleted stations excluded from all discovery queries
- Soft-deleted stations still accessible by direct ID (for admin recovery — Phase 2+)

### Event Ingest (POST /api/v1/events)
| Field | Rule |
|-------|------|
| event_type | Required, non-empty, max 50 chars |
| session_id | Required, non-empty, max 50 chars |
| occurred_at | Required, valid ISO 8601 UTC timestamp |
| user_id | Optional, max 50 chars |
| payload | Optional, if provided must be valid JSON object |

### Batch Ingest (POST /api/v1/events/batch)
- Max 100 events per batch (FR-011)
- All-or-nothing: any validation failure rejects entire batch (FR-017)
- Response includes array of all validation failures on error
