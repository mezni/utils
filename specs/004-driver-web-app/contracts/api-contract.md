# API Contract: Driver Web App

Same base URL and conventions as Sprint 1.1/1.2. The Driver Web App consumes the existing API — no new endpoints.

## Consumed Endpoints

### List Partners

```
GET /api/partners
```

Used to determine which partners are verified, live, and active.
Response: Array of Partner objects.
Status 200.

### List Stations

```
GET /api/stations
```

Used to get all station locations and details.
Response: Array of Station objects.
Status 200.

### List Chargers

```
GET /api/chargers
```

Used to compute available_count per station.
Response: Array of Charger objects.
Status 200.

### Get Station by ID

```
GET /api/stations/:id
```

Used by Station Detail screen.
Response: Single Station object.
Status 200.

### Get Chargers by Station ID

```
GET /api/chargers?station_id=STN001
```

Used by Station Detail screen to get chargers for a specific station.
Response: Array of Charger objects.
Status 200.
