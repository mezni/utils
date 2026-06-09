# REST API Contracts: MVP-1 Mock API

**Base URL**: `http://localhost:3001/api`

**Data format**: JSON

**Prefix**: All endpoints are served under `/api` via `routes.json` (`/api/*` → `/$1`)

**Server**: json-server (no custom logic)

---

## Partners

### GET /api/partners

Returns all partners.

**Response 200**:
```json
[
  {
    "id": 1,
    "name": "Énergie Verte Tunisia",
    "phone": "+216 71 123 456",
    "email": "contact@energieverte.tn"
  },
  {
    "id": 2,
    "name": "Tunisie Charge SA",
    "phone": "+216 73 789 012",
    "email": "support@tunisiecharge.tn"
  }
]
```

---

## Stations

### GET /api/stations

Returns all stations.

**Query parameters**: `partner_id` (optional) — filter by partner

**Response 200**:
```json
[
  {
    "id": 1,
    "name": "Station Tunis Centre",
    "partner_id": 1,
    "latitude": 36.8065,
    "longitude": 10.1815,
    "address": "Avenue Habib Bourguiba, Tunis",
    "status": "available"
  }
]
```

### GET /api/stations/:id

Returns a single station by ID.

**Response 200**:
```json
{
  "id": 1,
  "name": "Station Tunis Centre",
  "partner_id": 1,
  "latitude": 36.8065,
  "longitude": 10.1815,
  "address": "Avenue Habib Bourguiba, Tunis",
  "status": "available"
}
```

**Response 404**:
```json
{}
```

---

## Chargers

### GET /api/chargers

Returns all chargers.

**Query parameters**: `station_id` (optional) — filter by station

**Response 200**:
```json
[
  {
    "id": 1,
    "station_id": 1,
    "type": "DC",
    "power_kw": 150,
    "status": "available"
  }
]
```

### GET /api/chargers/:id

Returns a single charger by ID.

**Response 200**:
```json
{
  "id": 1,
  "station_id": 1,
  "type": "DC",
  "power_kw": 150,
  "status": "available"
}
```

**Response 404**:
```json
{}
```

---

## Design Tokens

Design tokens are not served via API. They are consumed as TypeScript imports from `source/packages/ui/src/tokens/`.
