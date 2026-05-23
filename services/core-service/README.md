# Core Service

The Core Service is the backbone of the BorneMap EV charging platform, providing the foundational CRUD operations for companies, stations, and chargers.

## Features

- **Company Management**: Create, read, update, and delete companies
- **Station Management**: Manage charging stations with geographic coordinates
- **Charger Management**: Handle individual chargers with status tracking
- **Soft Delete**: All entities support soft deletion with restoration capability
- **Optimistic Concurrency Control**: Version-based conflict detection
- **Comprehensive Validation**: Input validation at all layers
- **Health Checks**: Service health monitoring and metrics
- **JWT Authentication**: Secure API endpoints with JWT validation

## API Endpoints

### Companies
- `POST /api/v1/companies` - Create a new company
- `GET /api/v1/companies` - Get all companies
- `GET /api/v1/companies/{id}` - Get a specific company
- `PUT /api/v1/companies/{id}` - Update a company
- `DELETE /api/v1/companies/{id}` - Soft delete a company
- `POST /api/v1/companies/{id}/restore` - Restore a soft-deleted company

### Stations
- `POST /api/v1/stations` - Create a new station
- `GET /api/v1/stations` - Get all stations
- `GET /api/v1/stations/{id}` - Get a specific station
- `PUT /api/v1/stations/{id}` - Update a station
- `DELETE /api/v1/stations/{id}` - Soft delete a station
- `POST /api/v1/stations/{id}/restore` - Restore a soft-deleted station
- `GET /api/v1/stations/nearby` - Find stations near a location

### Chargers
- `POST /api/v1/chargers` - Create a new charger
- `GET /api/v1/chargers` - Get all chargers
- `GET /api/v1/chargers/{id}` - Get a specific charger
- `PUT /api/v1/chargers/{id}` - Update a charger
- `DELETE /api/v1/chargers/{id}` - Soft delete a charger
- `POST /api/v1/chargers/{id}/restore` - Restore a soft-deleted charger
- `PUT /api/v1/chargers/{id}/status` - Update charger status

### Health & Metrics
- `GET /health/core-service` - Service health check
- `GET /metrics/core-service` - Prometheus metrics

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | Required |
| `JWT_SECRET` | JWT signing secret | Required |
| `SERVER_HOST` | Server host address | `0.0.0.0` |
| `SERVER_PORT` | Server port | `8080` |
| `RUST_LOG` | Logging level | `info` |
| `LOG_FORMAT` | Log format (json or pretty) | `json` |
| `RABBITMQ_URL` | RabbitMQ connection string | Required |
| `CORS_ORIGINS` | Comma-separated CORS origins | `http://localhost:3000` |

## Development

### Prerequisites
- Rust 1.75+
- PostgreSQL 15+
- RabbitMQ 3.12+

### Setup
1. Copy environment variables:
   ```bash
   cp .env.example .env
   ```

2. Install dependencies:
   ```bash
   cargo build
   ```

3. Run database migrations:
   ```bash
   psql -h localhost -p 5432 -U bornemap -d bornemap -f migrations/001_initial_schema.sql
   ```

4. Start the service:
   ```bash
   cargo run
   ```

### Testing
```bash
# Run all tests
cargo test

# Run unit tests only
cargo test --lib

# Run integration tests
cargo test --test integration
```

## Docker

### Build
```bash
docker build -t bornemap/core-service:latest .
```

### Run with Docker Compose
```bash
docker-compose up core-service
```

## Architecture

The Core Service follows a layered architecture:

```
┌─────────────────┐
│   Handlers      │ ← HTTP layer (Actix Web)
├─────────────────┤
│   Services      │ ← Business logic
├─────────────────┤
│  Repositories   │ ← Data access
├─────────────────┤
│    Models       │ ← Domain entities
└─────────────────┘
```

### Key Components

- **Models**: Domain entities with validation and business rules
- **Repositories**: Data access layer with SQLx and PostgreSQL
- **Services**: Business logic and orchestration
- **Handlers**: HTTP endpoints with Actix Web
- **Middleware**: Authentication, error handling, logging

## Database Schema

The service uses PostgreSQL with the following main tables:
- `companies` - Company information
- `stations` - Charging station details with geographic coordinates
- `chargers` - Individual charger information with status tracking

All tables include:
- Soft delete support (`deleted_at` field)
- Optimistic concurrency control (`version` field)
- Audit timestamps (`created_at`, `updated_at`)

## Error Handling

The service uses standardized error responses:

```json
{
  "error": "ERROR_TYPE",
  "message": "Human-readable error message"
}
```

Common error types:
- `VALIDATION_ERROR` - Input validation failed
- `NOT_FOUND` - Resource not found
- `OPTIMISTIC_LOCK_ERROR` - Concurrent modification detected
- `DATABASE_ERROR` - Database operation failed
- `EMAIL_ALREADY_EXISTS` - Duplicate email address

## Security

- JWT-based authentication for all protected endpoints
- Input validation at all layers
- SQL injection prevention with parameterized queries
- CORS configuration for cross-origin requests
- Rate limiting (to be implemented)