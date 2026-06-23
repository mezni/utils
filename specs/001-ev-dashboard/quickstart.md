# Quickstart: EV Dashboard Platform Kernel

**Feature**: EV Dashboard Platform Kernel
**Date**: 2026-06-23
**Branch**: 001-ev-dashboard

## Overview

This quickstart guide helps you set up and run the EV Dashboard Platform Kernel locally. The system consists of:

- **Backend**: Rust (Actix-Web + SQLx + PostgreSQL)
- **Frontend**: React + TypeScript + Vite + TailwindCSS
- **Database**: PostgreSQL 16+
- **Infrastructure**: Docker + Docker Compose

---

## Prerequisites

Before you begin, ensure you have:

- **Docker** installed (version 20.10+)
- **Docker Compose** installed (version 2.0+)
- **Rust** installed (version 1.75+)
- **Node.js** installed (version 18+)
- **npm** or **yarn** package manager
- **Git** for version control

### Verify Installation

```bash
# Check Docker
docker --version
# Expected: Docker version 20.10.0 or higher

# Check Docker Compose
docker-compose --version
# Expected: Docker Compose version 2.0.0 or higher

# Check Rust
rustc --version
# Expected: rustc 1.75.0 or higher

# Check Node.js
node --version
# Expected: v18.0.0 or higher
```

---

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd BorneMap
```

### 2. Checkout Feature Branch

```bash
git checkout 001-ev-dashboard
```

### 3. Build Docker Images

Build all service images:

```bash
docker-compose build
```

**What this does**:
- Builds PostgreSQL database image with initial schema
- Builds admin-service (Rust backend)
- Builds admin-dashboard (React frontend)

### 4. Start Infrastructure

Start all services:

```bash
docker-compose up -d
```

**What this does**:
- Starts PostgreSQL database
- Starts admin-service on port 8080
- Starts admin-dashboard on port 3000

### 5. Verify Services are Running

```bash
# Check Docker containers
docker-compose ps

# Expected output:
# NAME                STATUS          PORTS
# bornemap-db         Up              0.0.0.0:5432->5432/tcp
# bornemap-api        Up              0.0.0.0:8080->8080/tcp
# bornemap-ui         Up              0.0.0.0:3000->80/tcp

# Check PostgreSQL is ready
docker-compose exec postgres pg_isready

# Expected: PostgreSQL 16.x (or similar version) - accepting connections
```

---

## Development Workflow

### Option 1: Using Docker (Recommended for Development)

#### Run Backend in Docker

```bash
# Start backend service
docker-compose up -d admin-service

# View backend logs
docker-compose logs -f admin-service

# Stop backend service
docker-compose stop admin-service
```

#### Run Frontend in Docker

```bash
# Start frontend service
docker-compose up -d admin-dashboard

# View frontend logs
docker-compose logs -f admin-dashboard

# Stop frontend service
docker-compose stop admin-dashboard
```

#### Database Operations in Docker

```bash
# Connect to PostgreSQL database
docker-compose exec postgres psql -U admin -d platform_db

# Run a query
SELECT * FROM ev.partners LIMIT 5;

# Exit database
\q
```

### Option 2: Local Development (Advanced)

#### Backend Development

1. **Install Dependencies**:

```bash
cd services/admin-service
cargo build
```

2. **Run Backend Locally**:

```bash
cd services/admin-service
cargo run
```

3. **Run Backend Tests**:

```bash
cd services/admin-service
cargo test
```

#### Frontend Development

1. **Install Dependencies**:

```bash
cd apps/admin-dashboard
npm install
```

2. **Run Frontend in Development Mode**:

```bash
cd apps/admin-dashboard
npm run dev
```

3. **Run Frontend Tests**:

```bash
cd apps/admin-dashboard
npm test
```

4. **Build Frontend for Production**:

```bash
cd apps/admin-dashboard
npm run build
```

---

## API Testing

### Using cURL

#### Test Dashboard KPIs

```bash
curl http://localhost:8080/api/v1/dashboard/kpis
```

**Expected Response**:
```json
{
  "success": true,
  "data": {
    "partners_count": 0,
    "stations_count": 0,
    "chargers_count": 0
  },
  "error": null
}
```

#### Create a Partner

```bash
curl -X POST http://localhost:8080/api/v1/partners \
  -H "Content-Type: application/json" \
  -d '{"name": "Example EV Network"}'
```

**Expected Response**:
```json
{
  "success": true,
  "data": {
    "id": "PRT-abc123456789",
    "name": "Example EV Network",
    "created_at": "2026-06-23T10:00:00Z"
  },
  "error": null
}
```

#### List Partners

```bash
curl http://localhost:8080/api/v1/partners?page=1&limit=10
```

#### Create a Station

```bash
curl -X POST http://localhost:8080/api/v1/stations \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Downtown Station",
    "location": "123 Main Street, New York",
    "partner_id": "PRT-abc123456789"
  }'
```

#### Create a Charger

```bash
curl -X POST http://localhost:8080/api/v1/chargers \
  -H "Content-Type: application/json" \
  -d '{
    "station_id": "STA-xyz987654321",
    "status": "active",
    "power_rating": 50
  }'
```

### Using PostgreSQL Client

```bash
# Connect to database
docker-compose exec postgres psql -U admin -d platform_db

# View all partners
SELECT id, name, created_at FROM ev.partners;

# View all stations
SELECT id, name, partner_id, created_at FROM ev.stations;

# View all chargers
SELECT id, station_id, status, power_rating, created_at FROM ev.chargers;

# Check database schema
\d ev.partners
\d ev.stations
\d ev.chargers

# Exit
\q
```

### Using API Documentation (Swagger/OpenAPI)

The API contract is documented in OpenAPI format at:

**Main API Specification**:
```
specs/001-ev-dashboard/contracts/api.yaml
```

**Dashboard KPIs Specification**:
```
specs/001-ev-dashboard/contracts/dashboard.yaml
```

You can use tools like:
- [Swagger UI](https://swagger.io/tools/swagger-ui/)
- [Redoc](https://github.com/Redocly/redoc)
- [Postman](https://www.postman.com/)

---

## Frontend Testing

### Start Frontend Application

```bash
docker-compose up -d admin-dashboard
```

Access the frontend at: http://localhost:3000

### Initial State

When you first start the application:

1. **Dashboard**: Shows all counts as 0
2. **No Data**: Partners, stations, and chargers are not yet created

### Test the Frontend

1. **Create a Partner**:
   - Go to `/data/partners`
   - Click "Create Partner"
   - Enter name: "Example EV Network"
   - Click "Create"

2. **Create a Station**:
   - Go to `/data/stations`
   - Click "Create Station"
   - Enter name: "Downtown Station"
   - Enter location: "123 Main Street"
   - Select the partner from dropdown
   - Click "Create"

3. **View Dashboard**:
   - Go to `/dashboard`
   - See counts update to reflect created entities

---

## Database Migrations

### Apply Migrations

Migrations are automatically applied when the database is first initialized:

```bash
# Restart the database to re-apply migrations
docker-compose restart postgres
```

### View Migrations

```bash
# Check database version
docker-compose exec postgres psql -U admin -d platform_db -c "
SELECT * FROM ev.migrations ORDER BY version DESC;
"
```

### Manual Migration (Not Recommended)

```bash
# Connect to database
docker-compose exec postgres psql -U admin -d platform_db

# Apply migration manually
\i migrations/001_create_schema.sql
\i migrations/002_create_partners.sql
\i migrations/003_create_stations.sql
\i migrations/004_create_chargers.sql
```

---

## Common Issues and Solutions

### Issue: Docker containers not starting

**Symptoms**: `docker-compose ps` shows containers as "Exited"

**Solution**:
```bash
# View logs
docker-compose logs postgres
docker-compose logs admin-service
docker-compose logs admin-dashboard

# Restart containers
docker-compose down
docker-compose up -d
```

### Issue: Can't connect to PostgreSQL

**Symptoms**: Connection refused errors

**Solution**:
```bash
# Check PostgreSQL is running
docker-compose ps postgres

# Check PostgreSQL logs
docker-compose logs postgres

# Rebuild PostgreSQL image
docker-compose up -d --force-recreate postgres
```

### Issue: Backend API returns 500 errors

**Symptoms**: All API calls return "Internal Server Error"

**Solution**:
```bash
# View backend logs
docker-compose logs -f admin-service

# Check for errors in logs
# Common causes:
# - Database connection issues
# - Migration failures
# - Runtime errors
```

### Issue: Frontend can't connect to backend

**Symptoms**: Frontend shows errors or empty data

**Solution**:
```bash
# Check backend is running
docker-compose ps admin-service

# View frontend logs
docker-compose logs -f admin-dashboard

# Check CORS settings (in .env file)
# Ensure backend URL matches frontend URL
```

### Issue: Database migrations fail

**Symptoms**: "relation already exists" or migration errors

**Solution**:
```bash
# Stop all services
docker-compose down

# Remove database volume (data loss!)
docker-compose down -v

# Restart and rebuild
docker-compose build
docker-compose up -d
```

---

## Project Structure

```
BorneMap/
├── services/
│   └── admin-service/              # Backend (Rust)
│       ├── src/
│       │   ├── presentation/       # HTTP handlers
│       │   ├── application/        # Use-cases
│       │   ├── domain/             # Business logic
│       │   ├── infrastructure/     # SQLx, repositories
│       │   ├── config/             # Configuration
│       │   ├── db/                 # Database pool
│       │   ├── middleware/         # Request/response middleware
│       │   └── common/             # Shared utilities
│       ├── migrations/             # SQLx migrations
│       ├── Cargo.toml
│       └── .env
│
├── apps/
│   └── admin-dashboard/            # Frontend (React)
│       ├── src/
│       │   ├── pages/              # Routing layer
│       │   ├── features/           # Business UI logic
│       │   ├── components/         # Pure UI primitives
│       │   ├── api/                # Transport layer
│       │   ├── hooks/              # React Query hooks
│       │   ├── types/              # TypeScript types
│       │   └── utils/              # Utilities
│       ├── package.json
│       ├── tailwind.config.js
│       └── vite.config.ts
│
├── crates/
│   ├── platform-core/              # Shared Rust crate
│   └── platform-db/                # Shared Rust crate
│
├── infrastructure/
│   ├── docker/
│   │   ├── postgres/
│   │   │   ├── init.sql
│   │   │   └── Dockerfile
│   │   ├── admin-service/
│   │   │   └── Dockerfile
│   │   └── admin-dashboard/
│   │       └── Dockerfile
│   ├── postgres/
│   │   ├── data/                   # volume mount
│   │   └── logs/
│   ├── observability/
│   └── network/
│
├── docs/
│   ├── core/                       # Core documentation
│   └── epics/                      # Epic specifications
│
├── specs/
│   └── 001-ev-dashboard/           # Feature specification
│       ├── spec.md                 # User scenarios, requirements
│       ├── plan.md                 # Implementation plan
│       ├── research.md             # Technical research
│       ├── data-model.md           # Entity definitions
│       ├── quickstart.md           # This file
│       ├── contracts/              # API contracts
│       │   ├── api.yaml
│       │   └── dashboard.yaml
│       ├── tasks.md                # Task breakdown
│       └── checklists/
│
├── docker-compose.yml              # Infrastructure orchestration
└── Cargo.toml                      # Rust workspace root
```

---

## Next Steps

1. **Read the Specification**: `specs/001-ev-dashboard/spec.md`
2. **Review the Plan**: `specs/001-ev-dashboard/plan.md`
3. **Check API Contracts**: `specs/001-ev-dashboard/contracts/api.yaml`
4. **Run Tests**: `cargo test` (backend) and `npm test` (frontend)
5. **Create Tasks**: Run `/speckit.tasks` to generate implementation tasks

---

## Resources

- **Main Documentation**: `docs/`
- **Constitution**: `.specify/memory/constitution.md`
- **API Documentation**: `specs/001-ev-dashboard/contracts/api.yaml`
- **Clean Architecture**: `docs/core/architecture.md`
- **API Standards**: `docs/core/api-standards.md`

---

## Getting Help

If you encounter issues:

1. **Check Logs**: `docker-compose logs <service-name>`
2. **Verify Prerequisites**: Ensure all required tools are installed
3. **Review Documentation**: Check the project documentation
4. **Create Issue**: Report bugs on GitHub Issues

---

## Support

- **Email**: support@bornemap.com
- **Documentation**: https://docs.bornemap.com
- **API Documentation**: https://api.bornemap.com/docs

---

**Last Updated**: 2026-06-23
**Version**: 1.0.0
