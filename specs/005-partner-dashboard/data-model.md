# Data Model: Partner Dashboard — Multi-Tenant Views

## Entity: Partner Profile

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| id | string | API | Format: `PRT-` + 12-char nanoid |
| display_name | string | API | Editable by partner |
| classification | enum | API | "Business" or "Private" — read-only for partner |
| tax_id | string? | API | Required if classification=Business — read-only for partner |
| contact_phone | string | API | Editable by partner |
| logo_url | string? | API | Editable by partner; stored as URL string |
| created_at | string (ISO) | API | Displayed in profile |
| deleted_at | string (ISO)? | API | null = active |

**Validation**: Multiple users can share the same partner_profile_id (multi-user per org). Classification and tax_id are read-only after creation.

---

## Entity: Station (Partner-Scoped)

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| id | string | API | Format: `STN-` + 12-char nanoid |
| name | string | API | Displayed in table, map popup |
| address | string | API | |
| city | string | API | Displayed in table |
| latitude | number | API | Map marker placement |
| longitude | number | API | Map marker placement |
| owner_id | string | API | Auto-assigned to partner's profile — hidden/read-only for partner |
| owner_name | string | API | Partner display_name for table display |
| is_operational | boolean | API | Toggle in create/edit form |
| is_test | boolean | API | From backend; displayed in table |
| created_at | string (ISO) | API | |

**Relationships**: Belongs to Partner (owner_id → Partner.id). Has many Chargers.

**Scoping**: List/detail queries MUST filter by `owner_id = current_partner_profile_id`. Station creation auto-assigns owner_id. Edit locks owner_id.

**Deletion**: Soft-delete (sets `deleted_at`), consistent with admin portal.

---

## Entity: Charger (Partner-Scoped)

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| id | string | API | Format: `CHG-` + 12-char nanoid |
| station_id | string | API | References station `STN-` ID — filtered to partner's stations |
| station_name | string | API | Station display name |
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

**Scoping**: Charger list MUST join through stations to filter by `stations.owner_id = current_partner_profile_id`. Station dropdown in charger form ONLY lists stations owned by the partner.

---

## Entity: User (Partner Context)

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| id | string | API | Format: `USR-` + 12-char nanoid |
| email | string | API | Login credential |
| role | enum | API | Must be "partner" for dashboard access |
| partner_profile_id | string? | API | Links to `PRT-` ID — multiple users can share the same value |

**Scoping**: JWT auth middleware extracts `user_id` → looks up `partner_profile_id` → injects as `owner_id` in all scoped queries.
