# Data Model: Integration & Testing

## Event Record Schema

Used for contract testing the event logging endpoint (`POST /api/v1/events` and `POST /api/v1/events/batch`).

### Single Event

```json
{
  "event_type": "station_detail_view | search | nearby_search | navigate_to_station",
  "actor": {
    "session_id": "uuid-string",
    "ip_address": "string (IPv4 or IPv6)",
    "user_agent": "string (optional)"
  },
  "context": {
    "station_id": "string (STA- prefix, required for station_detail_view and navigate_to_station)",
    "search_query": "string (required for search)",
    "coordinates": {
      "lat": "number (-90 to 90, required for nearby_search)",
      "lng": "number (-180 to 180, required for nearby_search)"
    },
    "result_count": "integer (optional, for search and nearby_search)"
  },
  "timestamp": "string (ISO 8601 UTC)"
}
```

### Batch Event Request

```json
{
  "events": [
    { /* Single Event (up to 100) */ }
  ]
}
```

### Validation Rules

| Field | Rule | Error Code |
|-------|------|------------|
| event_type | Must be one of the four enumerated values | 400 - invalid_event_type |
| actor.session_id | Required, must be valid UUID | 400 - missing_session_id |
| context.station_id | Required for station_detail_view and navigate_to_station | 400 - missing_station_id |
| context.search_query | Required for search events | 400 - missing_search_query |
| context.coordinates | Required for nearby_search events | 400 - missing_coordinates |
| lat | Must be between -90 and 90 | 400 - invalid_coordinates |
| lng | Must be between -180 and 180 | 400 - invalid_coordinates |
| timestamp | Required, must be valid ISO 8601 | 400 - invalid_timestamp |
| Batch size | Max 100 events per request | 400 - batch_size_exceeded |

## Test Data Specification

### Test Stations

Three test stations with known coordinates for deterministic E2E testing:

| ID | Name | Latitude | Longitude | Chargers | Notes |
|----|------|----------|-----------|----------|-------|
| STA-TEST-001 | Tunis Central | 36.8065 | 10.1815 | 2x CCS, 1x CHAdeMO, 2x AC | Primary test station |
| STA-TEST-002 | Tunis Nord | 36.8500 | 10.1500 | 1x CCS, 1x AC | Edge of default 10km radius |
| STA-TEST-003 | Sousse Plage | 35.8300 | 10.6400 | 3x CCS, 1x CHAdeMO | Outside default radius (expand radius test) |

### Test Events

For event logging E2E tests, trigger these user actions and verify corresponding events:

| Action | Expected Event Type | Key Fields to Verify |
|--------|-------------------|---------------------|
| View station detail for STA-TEST-001 | station_detail_view | station_id = STA-TEST-001 |
| Search "Tunis" | search | search_query = "Tunis", result_count > 0 |
| Nearby search at Tunis Central coords | nearby_search | lat = 36.8065, lng = 10.1815 |
| Navigate to STA-TEST-001 | navigate_to_station | station_id = STA-TEST-001 |
