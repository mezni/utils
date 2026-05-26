# Data Model: Admin Data Views & CRUD

## Entity: Partner Profile

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| id | string | API | Format: `PRT-` + 12-char nanoid |
| display_name | string | API | Displayed in table, owner dropdown |
| classification | enum | API | "Business" or "Private" |
| tax_id | string? | API | Required if classification=Business; hidden if Private |
| contact_phone | string | API | |
| logo_url | string? | API | |
| created_at | string (ISO) | API | Displayed in table |
| deleted_at | string (ISO)? | API | null = active; soft-deleted entities excluded |

**Validation**: tax_id required when classification="Business"; email unique across users; display_name required

---

## Entity: Station

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| id | string | API | Format: `STN-` + 12-char nanoid |
| name | string | API | Displayed in table, map popup |
| address | string | API | |
| city | string | API | Displayed in table |
| latitude | number | API | Used for map marker placement |
| longitude | number | API | Used for map marker placement |
| owner_id | string | API | References partner `PRT-` ID |
| owner_name | string | API | Partner display_name for table display |
| is_operational | boolean | API | Toggle in create/edit form |
| is_test | boolean | API | Displayed in table; from backend |
| created_at | string (ISO) | API | |

**Relationships**: Belongs to Partner (owner_id → Partner.id). Has many Chargers.

**Validation**: name required; coordinates must be valid lng/lat; owner_id must reference existing partner

---

## Entity: Charger

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| id | string | API | Format: `CHG-` + 12-char nanoid |
| station_id | string | API | References station `STN-` ID |
| station_name | string | API | Station display name for table |
| connector_type_id | string | API | References connector type `CNT-` ID |
| connector_type_name | string | API | Connector type display name |
| power_kw | number | API | |
| current_type | enum | API | "AC" or "DC" |
| status | enum | API | "available" / "occupied" / "faulted" / "offline" |

**Status → Badge Mapping**:
- available → green (`bg-green-500`)
- occupied → amber (`bg-amber-500`)
- faulted → red (`bg-red-500`)
- offline → gray (`bg-gray-500`)

**Relationships**: Belongs to Station (station_id → Station.id). Uses Connector Type (connector_type_id → station_connector_types.id). Hard-deleted (no soft delete).

**Validation**: power_kw > 0; status one of the four enum values; current_type one of AC/DC; connector_type_id must reference existing connector type

---

## Entity: Connector Type

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| id | string | API | Format: `CNT-` + 12-char nanoid |
| name | string | API | Unique; displayed in charger dropdown |
| description | string? | API | |
| created_at | string (ISO) | API | |

**Relationships**: Referenced by Chargers. Delete-restricted when referenced (RESTRICT check).

**Validation**: name required and unique across all connector types

---

## State Transitions

### Charger Status
```
available ←→ occupied
available → faulted
occupied → faulted
faulted → available
offline ←→ available
offline → faulted
```

### Soft Delete (Partners, Stations, Connector Types)
```
active → deleted (set deleted_at)
deleted → (hidden from all standard queries)
```

### Hard Delete (Chargers)
```
active → permanently removed from database
```

---

## Component → API Data Flow

| Component | API Endpoint | Method | Purpose |
|-----------|-------------|--------|---------|
| PartnersTable | `/api/v1/partners` | GET | List all partners |
| PartnerFormModal | `/api/v1/partners` | POST | Create partner |
| PartnerFormModal | `/api/v1/partners/:id` | PATCH | Update partner |
| PartnerFormModal | `/api/v1/users` | POST | Create user (bundled with partner) |
| PartnerConfirmDelete | `/api/v1/partners/:id` | DELETE | Soft-delete partner |
| StationsTable | `/api/v1/stations` | GET | List all stations |
| StationFormModal | `/api/v1/stations` | POST | Create station |
| StationFormModal | `/api/v1/stations/:id` | PATCH | Update station |
| StationConfirmDelete | `/api/v1/stations/:id` | DELETE | Soft-delete station |
| ChargersTable | `/api/v1/chargers` | GET | List all chargers (flat view) |
| ChargersTable | `/api/v1/stations/:id/chargers` | GET | List chargers for station (nested) |
| ChargerFormModal | `/api/v1/stations/:id/chargers` | POST | Create charger |
| ChargerFormModal | `/api/v1/chargers/:id` | PATCH | Update charger |
| ChargerConfirmDelete | `/api/v1/chargers/:id` | DELETE | Hard-delete charger |
| ConnectorTypesTable | `/api/v1/connector-types` | GET | List all connector types |
| ConnectorTypeFormModal | `/api/v1/connector-types` | POST | Create connector type |
| ConnectorTypeFormModal | `/api/v1/connector-types/:id` | PATCH | Update connector type |
| ConnectorTypeConfirmDelete | `/api/v1/connector-types/:id` | DELETE | Soft-delete (blocked if in use) |
