# API Documentation

## Services

### auth-service (`:3001`)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/health` | No | Health check |
| POST | `/auth/register` | No | Register account |
| POST | `/auth/login` | No | Login, returns JWT |

### admin-service (`:3002`)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/health` | No | Health check |
| POST | `/partners` | Admin | Create partner |
| GET | `/partners` | Admin | List partners |
| GET | `/partners/:id` | Admin | Get partner |
| PUT | `/partners/:id` | Admin | Update partner |
| DELETE | `/partners/:id` | Admin | Delete partner |
| POST | `/stations` | Admin/Partner | Create station |
| GET | `/stations` | Admin/Partner | List stations |
| GET | `/stations/:id` | Admin/Partner | Get station |
| PUT | `/stations/:id` | Admin/Partner | Update station |
| DELETE | `/stations/:id` | Admin/Partner | Delete station |
| POST | `/stations/:id/connectors` | Admin/Partner | Add connector |
| PUT | `/connectors/:id` | Admin/Partner | Update connector |
| DELETE | `/connectors/:id` | Admin/Partner | Remove connector |

### driver-service (`:3003`)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/health` | No | Health check |
| GET | `/stations/nearby?lat&lon&radius` | No | Find nearby stations |
| GET | `/stations/:id` | No | Get station details |

## Response Format

### Success
```json
{
  "data": { ... }
}
```

### Paginated
```json
{
  "data": [ ... ],
  "page": 1,
  "per_page": 20,
  "total": 100
}
```

### Error
```json
{
  "error": "Description of the error"
}
```
