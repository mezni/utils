# API Contract: Sprint 1 Backend and Database

## Base Rules

- All endpoints are served under `/api`.
- MVP-1 endpoints are unauthenticated.
- `GET /api/health` must perform a database check.
- Public identifiers are UUID strings.

## Resources

### Health

- `GET /api/health`

Response:

- `200` with a success payload when the database is reachable.

### Partners

- `GET /api/partners`
- `GET /api/partners/:id`
- `POST /api/partners`
- `PUT /api/partners/:id`
- `DELETE /api/partners/:id`

Partner payload:

- `id`
- `name`
- `created_at`
- `updated_at`

Create/update request fields:

- `name`

### Stations

- `GET /api/stations`
- `GET /api/stations/:id`
- `POST /api/stations`
- `PUT /api/stations/:id`
- `DELETE /api/stations/:id`
- `GET /api/stations/nearby`

Station payload:

- `id`
- `partner_id`
- `name`
- `latitude`
- `longitude`
- `address`
- `city`
- `governorate`
- `created_at`
- `updated_at`

Nearby query parameters:

- `lat`
- `lng`
- `radius_km`

Nearby response:

- ordered list of stations by ascending distance
- each item includes station fields and `distance_km`

### Chargers

- `GET /api/chargers`
- `GET /api/chargers/:id`
- `POST /api/chargers`
- `PUT /api/chargers/:id`
- `DELETE /api/chargers/:id`

Charger payload:

- `id`
- `station_id`
- `label`
- `connector_type`
- `power_kw`
- `status`
- `created_at`
- `updated_at`

## Standard Errors

- `404` when a requested resource does not exist.
- `422` when request validation fails.
- `500` when an unexpected server or database failure occurs.
