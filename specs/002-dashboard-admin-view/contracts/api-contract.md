# API Contract: Dashboard Admin View

**Base URL**: `http://localhost:3001/api`

**Method**: All requests use standard HTTP methods with JSON body.

**Error response shape** (json-server default):
```json
{
  "error": "Not found"
}
```

## Partners

### List All Partners

```
GET /api/partners
```

Response: Array of partner objects.
Status 200.

### Get Single Partner

```
GET /api/partners/:id
```

Response: Single partner object.
Status 200. Status 404 if not found.

### Create Partner

```
POST /api/partners
Content-Type: application/json

{
  "name": "string",
  "type": "business" | "personal"
}
```

Response: Created partner object with defaults (is_verified=false, is_live=false, is_active=true, audit fields set).
Status 201.

### Update Partner

```
PATCH /api/partners/:id
Content-Type: application/json

{
  "name?": "string",
  "type?": "business" | "personal",
  "is_verified?": true,
  "is_active?": true | false
}
```

Response: Updated partner object.
Status 200. Status 404 if not found.

### Delete Partner

```
DELETE /api/partners/:id
```

Response: Empty body.
Status 200. Status 404 if not found.

## Stations

### List All Stations

```
GET /api/stations
GET /api/stations?partner_id=PRT001
```

Response: Array of station objects.
Status 200.

### Get Single Station

```
GET /api/stations/:id
```

Response: Single station object.
Status 200. Status 404 if not found.

### Create Station

```
POST /api/stations
Content-Type: application/json

{
  "partner_id": "PRT-...",
  "name": "string",
  "address": "string",
  "latitude": number,
  "longitude": number
}
```

Response: Created station object with audit fields.
Status 201.

### Update Station

```
PATCH /api/stations/:id
Content-Type: application/json

{
  "name?": "string",
  "address?": "string",
  "latitude?": number,
  "longitude?": number
}
```

Response: Updated station object.
Status 200. Status 404 if not found.

### Delete Station

```
DELETE /api/stations/:id
```

Response: Empty body.
Status 200. Status 404 if not found.

## Chargers

### List All Chargers

```
GET /api/chargers
GET /api/chargers?station_id=STN001
```

Response: Array of charger objects.
Status 200.

### Get Single Charger

```
GET /api/chargers/:id
```

Response: Single charger object.
Status 200. Status 404 if not found.

### Create Charger

```
POST /api/chargers
Content-Type: application/json

{
  "station_id": "STN-...",
  "connector_type": "type2" | "ccs" | "chademo" | "type1",
  "power_kw": number,
  "status": "available" | "in_use" | "maintenance" | "offline"
}
```

Response: Created charger object with audit fields.
Status 201.

### Update Charger

```
PATCH /api/chargers/:id
Content-Type: application/json

{
  "connector_type?": "type2" | "ccs" | "chademo" | "type1",
  "power_kw?": number,
  "status?": "available" | "in_use" | "maintenance" | "offline"
}
```

Response: Updated charger object.
Status 200. Status 404 if not found.

### Delete Charger

```
DELETE /api/chargers/:id
```

Response: Empty body.
Status 200. Status 404 if not found.

## Station Availability

### List All Availability

```
GET /api/station_availability
GET /api/station_availability?station_id=STN001
```

Response: Array of availability objects.
Status 200.

## General Notes

- json-server automatically generates `id`, `created_at`, `created_by`, `updated_at`, `updated_by` on POST. The frontend includes these in POST bodies for Sprint 1.2 until explicit API client is built.
- All PATCH operations are partial — only send changed fields.
- json-server returns 404 for non-existent IDs.
- json-server returns { } empty body on DELETE.
- No authentication headers needed in MVP-1.
- The `/api` prefix is handled by the routes.json rewrite in json-server.
