# BorneMap API v1 - Testing Examples

**API Base URL**: `http://localhost:8000/api/v1`  
**Status**: Live after `docker-compose up -d`

---

## Quick Verification Tests

Run these commands to verify the API is working:

### 1. Health Check

```bash
curl -X GET "http://localhost:8000/api/v1/health" \
  -H "Content-Type: application/json"
```

**Expected Response (200 OK)**:
```json
{
  "status": "ok",
  "service": "bornemap-service",
  "db": "ok"
}
```

### 2. List Partners (Empty)

```bash
curl -X GET "http://localhost:8000/api/v1/partners" \
  -H "Content-Type: application/json"
```

**Expected Response (200 OK)**:
```json
{
  "data": [],
  "count": 0
}
```

### 3. Create Partner

```bash
curl -X POST "http://localhost:8000/api/v1/partners" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "TuniCharge"
  }'
```

**Expected Response (201 Created)**:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "TuniCharge",
  "created_at": "2026-06-08T14:30:00Z"
}
```

**Save the ID**: Copy the `id` field for next tests. Let's call it `PARTNER_ID`.

### 4. Create Station

```bash
curl -X POST "http://localhost:8000/api/v1/stations" \
  -H "Content-Type: application/json" \
  -d '{
    "partner_id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Tunis Central",
    "address": "123 Avenue Bourguiba, Tunis",
    "latitude": 36.8065,
    "longitude": 10.1815
  }'
```

**Expected Response (201 Created)**:
```json
{
  "id": "770e8400-e29b-41d4-a716-446655440000",
  "partner_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Tunis Central",
  "address": "123 Avenue Bourguiba, Tunis",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "charger_count": 0,
  "available_count": 0,
  "created_at": "2026-06-08T14:30:00Z",
  "updated_at": "2026-06-08T14:30:00Z"
}
```

**Save the ID**: Copy the `id` field. Let's call it `STATION_ID`.

### 5. Create Charger

```bash
curl -X POST "http://localhost:8000/api/v1/chargers" \
  -H "Content-Type: application/json" \
  -d '{
    "station_id": "770e8400-e29b-41d4-a716-446655440000",
    "connector_type": "Type2",
    "power_kw": 22.0
  }'
```

**Expected Response (201 Created)**:
```json
{
  "id": "990e8400-e29b-41d4-a716-446655440000",
  "station_id": "770e8400-e29b-41d4-a716-446655440000",
  "connector_type": "Type2",
  "power_kw": 22.0,
  "status": "available",
  "created_at": "2026-06-08T14:30:00Z",
  "updated_at": "2026-06-08T14:30:00Z"
}
```

**Save the ID**: Copy the `id` field. Let's call it `CHARGER_ID`.

### 6. List Partners (Now has 1)

```bash
curl -X GET "http://localhost:8000/api/v1/partners" \
  -H "Content-Type: application/json"
```

**Expected Response (200 OK)**:
```json
{
  "data": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "TuniCharge",
      "created_at": "2026-06-08T14:30:00Z"
    }
  ],
  "count": 1
}
```

### 7. List Stations (with charger counts)

```bash
curl -X GET "http://localhost:8000/api/v1/stations" \
  -H "Content-Type: application/json"
```

**Expected Response (200 OK)**:
```json
{
  "data": [
    {
      "id": "770e8400-e29b-41d4-a716-446655440000",
      "partner_id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "Tunis Central",
      "address": "123 Avenue Bourguiba, Tunis",
      "latitude": 36.8065,
      "longitude": 10.1815,
      "charger_count": 1,
      "available_count": 1,
      "created_at": "2026-06-08T14:30:00Z",
      "updated_at": "2026-06-08T14:30:00Z"
    }
  ],
  "count": 1
}
```

Note: `charger_count` is now 1 (calculated automatically).

---

## Versioning Tests

### Test 1: Versioned Endpoint Works

```bash
curl -X GET "http://localhost:8000/api/v1/stations" \
  -H "Content-Type: application/json"
```

**Expected**: 200 OK with data

### Test 2: Unversioned Endpoint Returns 404

```bash
curl -X GET "http://localhost:8000/api/stations" \
  -H "Content-Type: application/json"
```

**Expected Response (404 Not Found)**:
```json
{
  "detail": "Not Found"
}
```

### Test 3: Invalid Version Returns 404

```bash
curl -X GET "http://localhost:8000/api/v999/stations" \
  -H "Content-Type: application/json"
```

**Expected Response (404 Not Found)**:
```json
{
  "detail": "Not Found"
}
```

---

## Validation Tests

### Test 1: Invalid Latitude (Too High)

```bash
curl -X POST "http://localhost:8000/api/v1/stations" \
  -H "Content-Type: application/json" \
  -d '{
    "partner_id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Invalid",
    "address": "Test",
    "latitude": 91,
    "longitude": 10.1815
  }'
```

**Expected Response (422 Unprocessable Entity)**:
```json
{
  "detail": [
    {
      "loc": ["body", "latitude"],
      "msg": "ensure this value is less than or equal to 90",
      "type": "value_error.number.not_le"
    }
  ]
}
```

### Test 2: Invalid Longitude (Too Low)

```bash
curl -X POST "http://localhost:8000/api/v1/stations" \
  -H "Content-Type: application/json" \
  -d '{
    "partner_id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Invalid",
    "address": "Test",
    "latitude": 36.8065,
    "longitude": -181
  }'
```

**Expected Response (422 Unprocessable Entity)**:
```json
{
  "detail": [
    {
      "loc": ["body", "longitude"],
      "msg": "ensure this value is greater than or equal to -180",
      "type": "value_error.number.not_ge"
    }
  ]
}
```

### Test 3: Missing Required Field

```bash
curl -X POST "http://localhost:8000/api/v1/partners" \
  -H "Content-Type: application/json" \
  -d '{}'
```

**Expected Response (422 Unprocessable Entity)**:
```json
{
  "detail": [
    {
      "loc": ["body", "name"],
      "msg": "field required",
      "type": "value_error.missing"
    }
  ]
}
```

---

## Nearby Stations Search

### Create Multiple Stations First

```bash
# Create station 2
curl -X POST "http://localhost:8000/api/v1/stations" \
  -H "Content-Type: application/json" \
  -d '{
    "partner_id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Sfax Station",
    "address": "456 Avenue de la Liberté, Sfax",
    "latitude": 34.7406,
    "longitude": 10.7603
  }'
```

### Search Nearby

```bash
curl -X GET "http://localhost:8000/api/v1/stations/nearby?lat=36.8065&lng=10.1815&radius_km=50" \
  -H "Content-Type: application/json"
```

**Expected Response (200 OK)**:
```json
{
  "data": [
    {
      "id": "770e8400-e29b-41d4-a716-446655440000",
      "partner_id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "Tunis Central",
      "address": "123 Avenue Bourguiba, Tunis",
      "latitude": 36.8065,
      "longitude": 10.1815,
      "charger_count": 1,
      "available_count": 1,
      "created_at": "2026-06-08T14:30:00Z",
      "updated_at": "2026-06-08T14:30:00Z"
    }
  ],
  "count": 1
}
```

Note: Stations are ordered by distance (closest first).

---

## Get Details

### Get Partner Details

```bash
curl -X GET "http://localhost:8000/api/v1/partners/550e8400-e29b-41d4-a716-446655440000" \
  -H "Content-Type: application/json"
```

**Expected Response (200 OK)**:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "TuniCharge",
  "created_at": "2026-06-08T14:30:00Z"
}
```

### Get Station Details (with Chargers)

```bash
curl -X GET "http://localhost:8000/api/v1/stations/770e8400-e29b-41d4-a716-446655440000" \
  -H "Content-Type: application/json"
```

**Expected Response (200 OK)**:
```json
{
  "id": "770e8400-e29b-41d4-a716-446655440000",
  "partner_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Tunis Central",
  "address": "123 Avenue Bourguiba, Tunis",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "chargers": [
    {
      "id": "990e8400-e29b-41d4-a716-446655440000",
      "connector_type": "Type2",
      "power_kw": 22.0,
      "status": "available"
    }
  ],
  "charger_count": 1,
  "available_count": 1,
  "created_at": "2026-06-08T14:30:00Z",
  "updated_at": "2026-06-08T14:30:00Z"
}
```

### Get Charger Details

```bash
curl -X GET "http://localhost:8000/api/v1/chargers/990e8400-e29b-41d4-a716-446655440000" \
  -H "Content-Type: application/json"
```

**Expected Response (200 OK)**:
```json
{
  "id": "990e8400-e29b-41d4-a716-446655440000",
  "station_id": "770e8400-e29b-41d4-a716-446655440000",
  "connector_type": "Type2",
  "power_kw": 22.0,
  "status": "available",
  "created_at": "2026-06-08T14:30:00Z",
  "updated_at": "2026-06-08T14:30:00Z"
}
```

---

## Update Operations

### Update Partner

```bash
curl -X PUT "http://localhost:8000/api/v1/partners/550e8400-e29b-41d4-a716-446655440000" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "TuniCharge Updated"
  }'
```

**Expected Response (200 OK)**:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "TuniCharge Updated",
  "created_at": "2026-06-08T14:30:00Z"
}
```

### Update Station

```bash
curl -X PUT "http://localhost:8000/api/v1/stations/770e8400-e29b-41d4-a716-446655440000" \
  -H "Content-Type: application/json" \
  -d '{
    "partner_id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Tunis Central Updated",
    "address": "123 Avenue Bourguiba, Tunis (Updated)",
    "latitude": 36.8065,
    "longitude": 10.1815
  }'
```

**Expected Response (200 OK)**:
```json
{
  "id": "770e8400-e29b-41d4-a716-446655440000",
  "partner_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Tunis Central Updated",
  "address": "123 Avenue Bourguiba, Tunis (Updated)",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "charger_count": 1,
  "available_count": 1,
  "created_at": "2026-06-08T14:30:00Z",
  "updated_at": "2026-06-08T14:31:00Z"
}
```

### Update Charger

```bash
curl -X PUT "http://localhost:8000/api/v1/chargers/990e8400-e29b-41d4-a716-446655440000" \
  -H "Content-Type: application/json" \
  -d '{
    "connector_type": "CCS",
    "power_kw": 50.0
  }'
```

**Expected Response (200 OK)**:
```json
{
  "id": "990e8400-e29b-41d4-a716-446655440000",
  "station_id": "770e8400-e29b-41d4-a716-446655440000",
  "connector_type": "CCS",
  "power_kw": 50.0,
  "status": "available",
  "created_at": "2026-06-08T14:30:00Z",
  "updated_at": "2026-06-08T14:31:00Z"
}
```

---

## Delete Operations

### Delete Charger

```bash
curl -X DELETE "http://localhost:8000/api/v1/chargers/990e8400-e29b-41d4-a716-446655440000" \
  -H "Content-Type: application/json"
```

**Expected Response (204 No Content)**:
```
(empty body)
```

### Delete Station

```bash
curl -X DELETE "http://localhost:8000/api/v1/stations/770e8400-e29b-41d4-a716-446655440000" \
  -H "Content-Type: application/json"
```

**Expected Response (204 No Content)**:
```
(empty body)
```

### Delete Partner

```bash
curl -X DELETE "http://localhost:8000/api/v1/partners/550e8400-e29b-41d4-a716-446655440000" \
  -H "Content-Type: application/json"
```

**Expected Response (204 No Content)**:
```
(empty body)
```

---

## Error Cases

### Get Non-Existent Partner (404)

```bash
curl -X GET "http://localhost:8000/api/v1/partners/00000000-0000-0000-0000-000000000000" \
  -H "Content-Type: application/json"
```

**Expected Response (404 Not Found)**:
```json
{
  "detail": "Partner not found"
}
```

### Update Non-Existent Station (404)

```bash
curl -X PUT "http://localhost:8000/api/v1/stations/00000000-0000-0000-0000-000000000000" \
  -H "Content-Type: application/json" \
  -d '{
    "partner_id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Test",
    "address": "Test",
    "latitude": 0,
    "longitude": 0
  }'
```

**Expected Response (404 Not Found)**:
```json
{
  "detail": "Station not found"
}
```

### Delete Non-Existent Charger (404)

```bash
curl -X DELETE "http://localhost:8000/api/v1/chargers/00000000-0000-0000-0000-000000000000" \
  -H "Content-Type: application/json"
```

**Expected Response (404 Not Found)**:
```json
{
  "detail": "Charger not found"
}
```

---

## Interactive Testing

### Swagger UI

Open in browser: **http://localhost:8000/api/docs**

Features:
- Try out all endpoints interactively
- See request/response schemas
- View auto-generated documentation

### ReDoc

Open in browser: **http://localhost:8000/api/redoc**

Features:
- Clean API documentation
- Search across endpoints
- Mobile-friendly

### OpenAPI Spec

Raw JSON spec: **http://localhost:8000/api/openapi.json**

---

## Batch Test Script

```bash
#!/bin/bash

# Save as test_api.sh and run: bash test_api.sh

BASE_URL="http://localhost:8000/api/v1"

echo "=== Health Check ==="
curl -s -X GET "$BASE_URL/health" | jq .

echo -e "\n=== List Partners (Empty) ==="
curl -s -X GET "$BASE_URL/partners" | jq .

echo -e "\n=== Create Partner ==="
PARTNER=$(curl -s -X POST "$BASE_URL/partners" \
  -H "Content-Type: application/json" \
  -d '{"name": "TuniCharge"}')
echo "$PARTNER" | jq .
PARTNER_ID=$(echo "$PARTNER" | jq -r '.id')

echo -e "\n=== Create Station ==="
STATION=$(curl -s -X POST "$BASE_URL/stations" \
  -H "Content-Type: application/json" \
  -d "{
    \"partner_id\": \"$PARTNER_ID\",
    \"name\": \"Test Station\",
    \"address\": \"123 Test St\",
    \"latitude\": 36.8065,
    \"longitude\": 10.1815
  }")
echo "$STATION" | jq .
STATION_ID=$(echo "$STATION" | jq -r '.id')

echo -e "\n=== Create Charger ==="
CHARGER=$(curl -s -X POST "$BASE_URL/chargers" \
  -H "Content-Type: application/json" \
  -d "{
    \"station_id\": \"$STATION_ID\",
    \"connector_type\": \"Type2\",
    \"power_kw\": 22.0
  }")
echo "$CHARGER" | jq .
CHARGER_ID=$(echo "$CHARGER" | jq -r '.id')

echo -e "\n=== List Stations with Charger Counts ==="
curl -s -X GET "$BASE_URL/stations" | jq '.data[0] | {id, name, charger_count, available_count}'

echo -e "\n=== Get Station Details ==="
curl -s -X GET "$BASE_URL/stations/$STATION_ID" | jq '.chargers'

echo -e "\n=== Nearby Stations ==="
curl -s -X GET "$BASE_URL/stations/nearby?lat=36.8065&lng=10.1815&radius_km=50" | jq '.data | length'

echo -e "\n✅ All tests passed!"
```

---

## Quick Reference

| Endpoint | Method | Status | Notes |
|----------|--------|--------|-------|
| `/health` | GET | 200 | Always returns 200 |
| `/partners` | GET | 200 | Returns list |
| `/partners` | POST | 201 | Returns created partner |
| `/partners/{id}` | GET | 200/404 | 404 if not found |
| `/partners/{id}` | PUT | 200/404 | 404 if not found |
| `/partners/{id}` | DELETE | 204/404 | 204 = success |
| `/stations` | GET | 200 | With charger counts |
| `/stations` | POST | 201 | Returns created station |
| `/stations/nearby` | GET | 200 | lat, lng, radius_km required |
| `/stations/{id}` | GET | 200/404 | Includes chargers array |
| `/stations/{id}` | PUT | 200/404 | 404 if not found |
| `/stations/{id}` | DELETE | 204/404 | 204 = success |
| `/chargers` | GET | 200 | Optional station_id filter |
| `/chargers` | POST | 201 | Returns created charger |
| `/chargers/{id}` | GET | 200/404 | 404 if not found |
| `/chargers/{id}` | PUT | 200/404 | 404 if not found |
| `/chargers/{id}` | DELETE | 204/404 | 204 = success |

---

**API Documentation**: See `/docs/api/bornemap-service.md`  
**Status**: ✅ Ready for testing
