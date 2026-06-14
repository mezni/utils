# BorneMap MVP-1 Quick Start Guide

## 📂 Project Structure

```
source/
├── apps/
│   ├── shared-mobile/              # TypeScript type contracts
│   │   ├── src/
│   │   │   ├── constants.ts        # Tunisia bounds, initial region
│   │   │   ├── types.ts            # DTO interfaces
│   │   │   └── index.ts            # Module exports + utilities
│   │   ├── package.json
│   │   └── tsconfig.json
│   └── mobile-app/                 # React Native Expo app (to be implemented)
│
├── database/
│   └── platform_db/
│       ├── platform-init.sql       # Schema bootstrap (3 schemas: config, inventory, gis)
│       └── functions.sql           # PostGIS proximity engine
│
├── infra/
│   ├── docker-compose.yml          # Multi-container orchestration
│   └── traefik/
│       └── dynamic.yml             # Gateway routing rules
│
├── scripts/
│   └── import-tunisia-osm.sh       # OSM data ingestion pipeline
│
└── services/                        # Rust microservices
    ├── shared/                      # Cross-service primitives
    │   ├── src/
    │   │   ├── lib.rs
    │   │   ├── domain.rs           # Shared DTOs
    │   │   ├── auth.rs             # Mock auth context
    │   │   └── logging.rs          # Structured JSON logging
    │   └── Cargo.toml
    │
    ├── libs/
    │   ├── db-core/                # SQLx connection pooling
    │   └── geo-core/               # Geospatial boundary validation
    │
    ├── driver-service/             # Read-optimized proximity lookups
    │   ├── src/
    │   │   ├── main.rs             # Server entry + Swagger UI
    │   │   ├── handlers/
    │   │   │   └── proximity.rs     # GET /nearby endpoint
    │   │   ├── domain.rs           # Query/Response DTOs
    │   │   ├── error.rs            # Error handling
    │   │   └── middleware.rs       # Gateway awareness
    │   ├── Cargo.toml
    │   └── Dockerfile              # Multi-stage build
    │
    ├── admin-service/              # Write operations & asset mgmt
    │   ├── src/
    │   │   ├── main.rs             # Server entry + Swagger UI
    │   │   ├── handlers/           # Partners, Stations, Chargers
    │   │   ├── usecase.rs          # Business logic
    │   │   ├── domain.rs           # Request/Response DTOs
    │   │   ├── error.rs            # Error handling
    │   │   └── middleware.rs       # Gateway awareness
    │   ├── Cargo.toml
    │   └── Dockerfile              # Multi-stage build
    │
    └── Cargo.toml                  # Workspace configuration
```

---

## 🚀 Quick Start (5 minutes)

### 1. Start Infrastructure
```bash
cd source/infra
docker-compose up -d
```

Verify services are running:
```bash
docker-compose ps
# Should show: traefik, database, driver-service, admin-service
```

### 2. Verify Connectivity
```bash
# Check driver service health
curl http://localhost/api/v1/driver/health

# Check admin service health
curl http://localhost/api/v1/admin/health

# View Swagger documentation
open http://localhost/docs/swagger
```

### 3. Create Test Data

**Create a Partner**:
```bash
curl -X POST http://localhost/api/v1/admin/partners \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Green Energy Tunisia",
    "partner_type": "BUSINESS",
    "email": "info@greenergy.tn",
    "phone": "+216-70-123-456"
  }'
```

**Response** (note the `partner_id`):
```json
{
  "data": {
    "id": "par-a1b2c3d4e5f6g7h8",
    "name": "Green Energy Tunisia",
    "partner_type": "BUSINESS",
    "email": "info@greenergy.tn",
    "phone": "+216-70-123-456",
    "verified": false,
    "created_at": "2026-06-14T12:34:56Z",
    "updated_at": "2026-06-14T12:34:56Z"
  },
  "message": "Partner created successfully"
}
```

**Create a Station**:
```bash
curl -X POST http://localhost/api/v1/admin/stations \
  -H "Content-Type: application/json" \
  -d '{
    "partner_id": "par-a1b2c3d4e5f6g7h8",
    "name": "Tunis Central Hub",
    "address": "Avenue de la Liberté, Tunis",
    "email": "hub@greenergy.tn",
    "latitude": 36.8065,
    "longitude": 10.1815
  }'
```

**Publish to Map** (enable live visibility):
```bash
curl -X PATCH http://localhost/api/v1/admin/stations/stn-xxxxxxxxxxxx/live \
  -H "Content-Type: application/json" \
  -d '{ "is_live": true }'
```

**Create Chargers**:
```bash
curl -X POST http://localhost/api/v1/admin/chargers \
  -H "Content-Type: application/json" \
  -d '{
    "station_id": "stn-xxxxxxxxxxxx",
    "identifier_code": "CH-TN-001",
    "plug_type_code": "ccs2",
    "max_power_kw": 150
  }'
```

### 4. Query Nearby Stations

From your location in Tunis:
```bash
curl "http://localhost/api/v1/driver/nearby?longitude=10.1815&latitude=36.8065&search_radius_meters=5000"
```

**Response**:
```json
{
  "stations": [
    {
      "station_id": "stn-xxxxxxxxxxxx",
      "station_name": "Tunis Central Hub",
      "station_address": "Avenue de la Liberté, Tunis",
      "distance_meters": 0.0,
      "latitude": 36.8065,
      "longitude": 10.1815,
      "available_chargers": [
        {
          "charger_id": "chr-xxxxxxxxxxxx",
          "code": "CH-TN-001",
          "plug_type": "ccs2",
          "max_power_kw": 150,
          "status": "ONLINE"
        }
      ]
    }
  ],
  "count": 1
}
```

---

## 📝 Key Endpoints

### Driver Service (Read Operations)

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/api/v1/driver/nearby` | Proximity search (params: longitude, latitude, search_radius_meters) |
| GET | `/api/v1/driver/health` | Health check |
| GET | `/docs/swagger` | API documentation |
| GET | `/docs/openapi.json` | OpenAPI specification |

### Admin Service (Write Operations)

| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST | `/api/v1/admin/partners` | Create partner |
| GET | `/api/v1/admin/partners/{id}` | Get partner by ID |
| POST | `/api/v1/admin/stations` | Create station |
| PATCH | `/api/v1/admin/stations/{id}/live` | Publish/unpublish to map |
| POST | `/api/v1/admin/chargers` | Add charger to station |
| GET | `/api/v1/admin/health` | Health check |
| GET | `/docs/swagger` | API documentation |

---

## 🗄️ Database Access

### Connect to PostgreSQL directly

```bash
# From host machine
psql -h localhost -U platform_admin -d platform_db

# Password: platform_secure_password_2026
```

### Useful Queries

**View all stations**:
```sql
SELECT id, name, latitude, longitude, is_live 
FROM inventory.stations;
```

**Check spatial cache**:
```sql
SELECT id, name, ST_AsText(coordinates) as location
FROM gis.osm_stations;
```

**Test proximity query**:
```sql
SELECT * FROM gis.get_nearby_stations(10.1815, 36.8065, 5000);
```

---

## 🐳 Docker Commands Reference

### View Logs
```bash
# Driver service
docker-compose logs -f driver-service

# Admin service
docker-compose logs -f admin-service

# Database
docker-compose logs -f database

# Gateway
docker-compose logs -f traefik
```

### Stop All Services
```bash
docker-compose down
```

### Stop & Remove Data
```bash
docker-compose down -v
```

### Rebuild Services
```bash
docker-compose build --no-cache
docker-compose up -d
```

---

## 🛠️ Local Development (Without Docker)

### Prerequisites
- Rust 1.70+
- PostgreSQL 15+
- Node.js 18+ (for TypeScript)

### Run Services Locally

**1. Start PostgreSQL**:
```bash
# Using brew or system package manager
brew services start postgresql

# Initialize database
psql -U postgres -c "CREATE DATABASE platform_db;"
psql -U postgres -d platform_db -f source/database/platform_db/platform-init.sql
psql -U postgres -d platform_db -f source/database/platform_db/functions.sql
```

**2. Run Driver Service**:
```bash
cd source/services/driver-service
DATABASE_URL="postgres://postgres@localhost/platform_db" \
RUST_LOG=debug \
cargo run --release
# Runs on http://localhost:8081
```

**3. Run Admin Service** (in another terminal):
```bash
cd source/services/admin-service
DATABASE_URL="postgres://postgres@localhost/platform_db" \
RUST_LOG=debug \
cargo run --release
# Runs on http://localhost:8082
```

**4. Build Shared Mobile Library**:
```bash
cd source/apps/shared-mobile
npm install
npm run build
# Output: dist/
```

---

## 🔍 Troubleshooting

### Issue: Port 80 already in use
```bash
# Find process using port 80
lsof -i :80

# Kill process (macOS/Linux)
kill -9 <PID>
```

### Issue: Database won't start
```bash
# Check database logs
docker-compose logs database

# Inspect volume
docker volume ls
docker volume inspect bornemap-postgres-data

# Reset data
docker-compose down -v
docker-compose up -d database
```

### Issue: Services can't reach database
```bash
# Verify network
docker network inspect bornemap-platform-mesh

# Check service connectivity
docker-compose exec driver-service ping database
```

### Issue: Traefik not routing correctly
```bash
# Check dynamic config is loaded
curl http://localhost:8080/api/routers

# Verify labels are set
docker inspect bornemap-driver-node | grep traefik
```

---

## 📊 Performance Tuning

### Database Connection Pool
Edit `source/services/libs/db-core/src/lib.rs`:
```rust
.max_connections(20)    // Increase for more concurrent requests
.min_connections(5)     // Keep idle connections ready
.acquire_timeout(Duration::from_secs(3))
```

### Logging Level
Environment variable `RUST_LOG`:
```bash
# Verbose debugging
RUST_LOG=debug

# Performance-optimized
RUST_LOG=info

# Specific service
RUST_LOG=driver_service=trace
```

### Proximity Query Radius
Default: 5000 meters (5km)
Customize in driver service query or client code

---

## 🔐 Security Notes

### Default Credentials (Development Only!)
```
Database User: platform_admin
Database Password: platform_secure_password_2026
Database: platform_db
```

⚠️ **For Production**: Use environment variables and secrets management (HashiCorp Vault, AWS Secrets Manager, etc.)

### Network Security
- Services only exposed internally via `platform-mesh` network
- Public access via Traefik gateway (port 80) only
- No direct database exposure to public internet

---

## 📦 TypeScript Integration

### Import Shared Types in Mobile App

**Install from local workspace**:
```json
{
  "dependencies": {
    "@bornemap/shared-mobile": "file:../shared-mobile"
  }
}
```

**Usage in React Native**:
```typescript
import { NearbyStationDto, verifyCoordinateWithinTunisia } from '@bornemap/shared-mobile';

const isValidLocation = verifyCoordinateWithinTunisia(10.1815, 36.8065);

const stations: NearbyStationDto[] = await fetchNearbyStations({
  longitude: 10.1815,
  latitude: 36.8065,
  search_radius_meters: 5000
});
```

---

## 🎯 Next Steps

1. **Complete Mobile UI** (Task 5.2, 5.3)
   - Integrate `@bornemap/shared-mobile` types
   - Build `MapCanvas` with hardware acceleration
   - Add station detail drawer + haptic feedback

2. **Add Tests** (Task 6.1, 6.2, 6.3)
   - Unit tests for geospatial functions
   - Integration tests for API endpoints
   - E2E tests for full discovery flow

3. **Prepare Production Deployment**
   - Kubernetes manifests
   - Helm charts
   - Database migration strategy
   - Monitoring & alerting setup

---

## 📞 Support

### Logs & Debugging
```bash
# Full service logs with timestamps
docker-compose logs --timestamps

# Follow new logs in real-time
docker-compose logs -f

# Single service
docker-compose logs driver-service
```

### Database Debugging
```bash
# Connect to database
docker-compose exec database psql -U platform_admin -d platform_db

# Inside psql:
\dt                                    -- List tables
SELECT version();                      -- Check PostGIS version
SELECT * FROM gis.osm_stations LIMIT 1; -- Check spatial data
```

### Service Health
```bash
# Quick health check
curl -s http://localhost/api/v1/driver/health | jq .
curl -s http://localhost/api/v1/admin/health | jq .
```

---

**Last Updated**: June 14, 2026  
**Version**: MVP-1 Alpha  
**Status**: Ready for Integration Testing
