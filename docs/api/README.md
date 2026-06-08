# API Documentation

## Base Rule
All endpoints are served under `/api`.

## MVP-1 Endpoints
- `GET /api/health`
- `GET /api/stations/nearby`
- `GET /api/stations`
- `GET /api/stations/:id`
- `POST /api/stations`
- `PUT /api/stations/:id`
- `DELETE /api/stations/:id`
- `GET /api/partners`
- `GET /api/partners/:id`
- `POST /api/partners`
- `PUT /api/partners/:id`
- `DELETE /api/partners/:id`
- `GET /api/chargers`
- `GET /api/chargers/:id`
- `POST /api/chargers`
- `PUT /api/chargers/:id`
- `DELETE /api/chargers/:id`

## Notes
- all endpoints are unauthenticated in MVP-1
- `GET /api/health` must include a database check
- SQL must use bind parameters only
