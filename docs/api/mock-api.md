# Mock API Reference

**Base URL**: `http://localhost:3001/api`

The mock API is powered by json-server. All resources are read from `source/mock/db.json`. Routing is configured in `source/mock/routes.json` to map `/api/*` to `/*`.

## Resources

### Partners

**Endpoint**: `GET /api/partners`, `POST /api/partners`
**Detail**: `GET /api/partners/:id`, `PATCH /api/partners/:id`, `DELETE /api/partners/:id`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | string | auto | Primary key |
| `name` | string | — | Partner display name |
| `type` | string | `"business"` | `"business"` or `"personal"` |
| `is_verified` | boolean | `false` | Admin verification status |
| `is_live` | boolean | `false` | Live visibility flag |
| `is_active` | boolean | `true` | Active status |
| `created_at` | string | auto | ISO 8601 timestamp |
| `created_by` | string | `"admin"` | Who created |
| `updated_at` | string | auto | ISO 8601 timestamp |
| `updated_by` | string | `"admin"` | Who last updated |

**Visibility rule**: A partner's stations appear on driver apps only when `is_verified=true AND is_live=true AND is_active=true`.

### Stations

**Endpoint**: `GET /api/stations`, `POST /api/stations`
**Detail**: `GET /api/stations/:id`, `PATCH /api/stations/:id`, `DELETE /api/stations/:id`

| Field | Type | Validation | Description |
|-------|------|------------|-------------|
| `id` | string | auto | Primary key |
| `partner_id` | string | required | FK to Partner.id |
| `name` | string | required | Station name |
| `address` | string | — | Street address |
| `latitude` | number | -90 to 90 | WGS84 latitude |
| `longitude` | number | -180 to 180 | WGS84 longitude |
| `created_at` | string | auto | ISO 8601 timestamp |
| `created_by` | string | auto | Who created |
| `updated_at` | string | auto | ISO 8601 timestamp |
| `updated_by` | string | auto | Who last updated |

**Query parameters**: `partner_id` — filter stations by owning partner.

### Chargers

**Endpoint**: `GET /api/chargers`, `POST /api/chargers`
**Detail**: `GET /api/chargers/:id`, `PATCH /api/chargers/:id`, `DELETE /api/chargers/:id`

| Field | Type | Validation | Description |
|-------|------|------------|-------------|
| `id` | string | auto | Primary key |
| `station_id` | string | required | FK to Station.id |
| `connector_type` | string | required | `"type2"`, `"ccs"`, `"chademo"`, `"type1"` |
| `power_kw` | number | positive | Power rating in kW |
| `status` | string | required | `"available"`, `"in_use"`, `"maintenance"`, `"offline"` |
| `created_at` | string | auto | ISO 8601 timestamp |
| `created_by` | string | auto | Who created |
| `updated_at` | string | auto | ISO 8601 timestamp |
| `updated_by` | string | auto | Who last updated |

**Query parameters**: `station_id` — filter chargers by station.

### Station Availability

**Endpoint**: `GET /api/station_availability`, `POST /api/station_availability`

**Note**: This resource is append-only. No update or delete endpoints are used.

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Primary key |
| `station_id` | string | FK to Station.id |
| `status` | string | `"available"`, `"partial"`, `"unavailable"` |
| `updated_by` | string | Who updated |
| `updated_at` | string | ISO 8601 timestamp |

**Query parameters**: `station_id` — filter by station.

## Filter Examples

```bash
# Stations owned by partner with id=1
GET /api/stations?partner_id=1

# Chargers at station with id=3
GET /api/chargers?station_id=3

# Filter by multiple station IDs
GET /api/chargers?station_id=1&station_id=2

# Availability records for a station
GET /api/station_availability?station_id=5
```

## Known Limitations

- **No referential integrity**: Deleting a partner does not cascade to stations. The Dashboard blocks deletion when a partner owns stations, but direct API calls can create orphaned records.
- **No pagination**: json-server returns all records. This is acceptable for MVP-1 scale (< 100 records).
- **No authentication**: All endpoints are publicly accessible. Authentication arrives in MVP-3.
- **No spatial queries**: Filtering by geographic proximity is not supported. MVP-2 adds PostGIS + `ST_DWithin`.
- **No schema validation**: json-server accepts any fields in POST/PATCH bodies. Dashboard form validation enforces constraints client-side.

## Seeded Data

The `db.json` ships with 3 partners in distinct states, 15 stations across Tunisian cities, 24 chargers, and 15 availability records.

| Partner | is_verified | is_live | is_active | Visibility |
|---------|-------------|---------|-----------|------------|
| PRT001 | true | true | true | Visible on driver apps |
| PRT002 | true | true | true | Visible on driver apps |
| PRT003 | false | false | true | Hidden on driver apps |
