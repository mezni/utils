# Data Model: Integration and Hardening

No new entities. This sprint validates and hardens existing entities.

## Existing Entities

### Partner

| Field | Type | Constraints |
|-------|------|-------------|
| `id` | string | Primary key |
| `name` | string | Required |
| `type` | string | 'business' or 'personal' |
| `is_verified` | boolean | Default false |
| `is_live` | boolean | Default false |
| `is_active` | boolean | Default true |
| `created_at`, `created_by`, `updated_at`, `updated_by` | string | Audit fields |

**Validation**: Deletion blocked when partner owns stations (decision to record).

### Station

| Field | Type | Validation |
|-------|------|------------|
| `id` | string | Primary key |
| `partner_id` | string | FK → Partner.id |
| `name` | string | Required |
| `address` | string | Required |
| `latitude` | number | -90 to 90 |
| `longitude` | number | -180 to 180 |
| Audit fields | string | Same as Partner |

### Charger

| Field | Type | Validation |
|-------|------|------------|
| `id` | string | Primary key |
| `station_id` | string | FK → Station.id |
| `connector_type` | string | Required |
| `power_kw` | number | Required, positive |
| `status` | string | 'available', 'in_use', 'maintenance', 'offline' |

### Station Availability

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Primary key |
| `station_id` | string | FK → Station.id |
| `status` | string | 'available', 'partial', 'unavailable' |
| `updated_by` | string | Who updated |
| `updated_at` | string | Timestamp |

## Validation Rules Verified in This Sprint

- Partner name: required on creation and edit
- Station name, address: required
- Station latitude: -90 to 90 (inline error if invalid)
- Station longitude: -180 to 180 (inline error if invalid)
- Charger connector_type, power_kw, status: required
- All Dashboard forms: empty required fields blocked before submission
