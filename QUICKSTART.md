# BorneMap - EV Charging Infrastructure

A comprehensive admin dashboard for managing EV charging infrastructure, including partners, stations, and chargers.

## Architecture

Clean Architecture pattern with clear separation:
- **platform-core**: Domain entities, ID generation, error handling, pagination
- **platform-db**: Database abstraction, repository traits, SQLx implementations
- **admin-service**: Backend API with Actix-Web
- **admin-dashboard**: React frontend with TypeScript

## Quick Start

```bash
# Build and start all services
docker compose up -d

# Access dashboard at http://localhost:3000
# API at http://localhost:8080/api/v1
```

## API Endpoints

### Dashboard
- `GET /api/v1/dashboard/kpis` - Get KPI metrics

### Partners
- `GET /api/v1/partners` - List all partners (paginated)
- `GET /api/v1/partners/:id` - Get partner by ID
- `POST /api/v1/partners` - Create partner
- `PATCH /api/v1/partners/:id` - Update partner name
- `PUT /api/v1/partners/:id` - Soft delete partner
- `DELETE /api/v1/partners/:id` - Hard delete partner

### Stations
- `GET /api/v1/stations` - List all stations (paginated, filterable by partner_id)
- `GET /api/v1/stations/:id` - Get station by ID
- `POST /api/v1/stations` - Create station
- `PATCH /api/v1/stations/:id` - Update station name/location
- `PUT /api/v1/stations/:id` - Soft delete station
- `DELETE /api/v1/stations/:id` - Hard delete station

### Chargers
- `GET /api/v1/chargers` - List all chargers (paginated, filterable by station_id)
- `GET /api/v1/chargers/:id` - Get charger by ID
- `POST /api/v1/chargers` - Create charger
- `PATCH /api/v1/chargers/:id` - Update power rating
- `PUT /api/v1/chargers/:id` - Soft delete charger
- `DELETE /api/v1/chargers/:id` - Hard delete charger

## Database Schema

All tables in `ev` schema with soft delete support:
- `partners` - Network operators
- `stations` - Charging locations
- `chargers` - Charging units

Features:
- **Soft Delete**: Deletion is reversible via `undelete` endpoint
- **Pagination**: Default 50 items, max 100 per page
- **Validation**: Input validation at service layer
- **Idempotent IDs**: Deterministic ID generation using hash

## Features

- ✅ Full CRUD operations
- ✅ Pagination (default 50, max 100)
- ✅ Soft delete with undo
- ✅ Entity filtering (stations by partner_id, chargers by station_id)
- ✅ Input validation
- ✅ API versioning (v1)
- ✅ Docker Compose deployment
- ✅ Health check endpoint
- ✅ Responsive UI with dark OLED theme

## Screenshots

(Dashboard with KPI cards, Partners table, Station detail page with inline edit)

## Security Notes

⚠️ **Production Deployment Recommendations:**
1. Enable PostgreSQL SSL (`sslmode=require`)
2. Use environment variables for credentials (secrets manager)
3. Add rate limiting
4. Implement JWT authentication
5. Enable CORS with specific origins only
6. Add health check monitoring

## Design System

Theme: Dark OLED
- Background: `#020617` (slate-950)
- Surface: `#0F172A` (slate-900)
- Primary Accent: `#F97316` (orange-500)
- Success Accent: `#22C55E` (green-500)

Typography: Fira Code (headings) + Fira Sans (body)

## Development

### Rust Backend
```bash
cd services/admin-service
cargo build --release
```

### Frontend
```bash
cd apps/admin-dashboard
npm install
npm run build
```

## License

MIT
