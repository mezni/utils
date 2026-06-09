# API Contract: Dashboard Partner View

Same base URL and conventions as Sprint 1.2. Additional endpoints noted below.

## Station Availability

### Create Availability Record

```
POST /api/station_availability
Content-Type: application/json

{
  "station_id": "STN001",
  "status": "available" | "partial" | "unavailable",
  "updated_by": "USR-PRT001",
  "updated_at": "2026-06-09T12:00:00Z"
}
```

Response: Created availability object with auto-generated `id`.
Status 201.

### List Availability (filtered)

```
GET /api/station_availability
GET /api/station_availability?station_id=STN001
GET /api/station_availability?station_id=STN001&station_id=STN002
```

json-server supports multiple values for the same filter param.
Response: Array of availability objects.
Status 200.

## Scoped Endpoints (same as Sprint 1.2, used with filters)

```
GET /api/stations?partner_id=PRT001
GET /api/chargers?station_id=STN001&station_id=STN002
```

## Partner Resource

```
GET /api/partners/:id
```

Returns single partner with all fields including `is_verified`, `is_live`, `is_active`.
Used by Partner Overview to populate the status bar.
