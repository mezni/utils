# BorneMap

Geospatial EV charging discovery platform for the Tunisian market.

## Quick Start

```bash
make up          # Start PostGIS database
make dev-api     # Run api-service on :8080
```

Then open [http://127.0.0.1:8080/api/v1/stations/nearby?lat=36.8065&lng=10.1815]()

For the mobile app:

```bash
cd apps/mobile-driver
npx expo start
```

## Tech Stack

- **Backend**: Rust + Actix-web
- **Database**: PostgreSQL + PostGIS
- **Mobile**: React Native + Expo Go
- **Cache**: Redis (future)
- **Broker**: RabbitMQ (future)

## Project Structure

```
├── apps/mobile-driver/     # React Native / Expo Go mobile app
├── apps/web-admin/         # React admin portal (future)
├── backend/                # Rust multi-crate workspace
│   ├── api-service/        # HTTP gateway and business router
│   ├── auth-service/       # Identity provider (stub)
│   ├── domain/             # Domain entities and shared types
│   └── infra/              # Database pools and PostGIS clients
├── db/                     # Migrations and seed data
├── deployments/            # Production Docker Compose and nginx
├── docs/                   # Architecture docs and runbooks
├── specs/                  # Feature specifications
└── .github/workflows/      # CI pipeline
```

## Make Commands

| Command | Description |
|---------|-------------|
| `make up` | Start PostGIS database |
| `make down` | Stop database |
| `make status` | Check container status |
| `make test-backend` | Run `cargo test` |
| `make dev-api` | Run api-service |

## Constitution

Core principles are defined in [.specify/memory/constitution.md](.specify/memory/constitution.md).
