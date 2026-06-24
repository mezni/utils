# BorneMap - EV Charging Infrastructure Dashboard

A comprehensive admin dashboard for managing EV charging infrastructure, including partners, stations, and chargers.

## 🏗️ Architecture

BorneMap follows a **Clean Architecture** pattern with clear separation of concerns:

```
┌─────────────────────────────────────────────┐
│         Presentation Layer                  │
│  (HTTP Handlers, Routes, Controllers)        │
├─────────────────────────────────────────────┤
│         Application Layer                   │
│  (Services, Business Logic, Validation)     │
├─────────────────────────────────────────────┤
│         Domain Layer                        │
│  (Models, Traits, Error Handling)           │
├─────────────────────────────────────────────┤
│         Infrastructure Layer                │
│  (PostgreSQL, Repositories, Utils)          │
└─────────────────────────────────────────────┘
```

### Key Components

- **platform-core**: Domain entities, ID generation, error handling, pagination
- **platform-db**: Database abstraction, repository traits, SQLx implementations
- **admin-service**: Backend API with Actix-Web
- **admin-dashboard**: React frontend with TypeScript

## 🚀 Quick Start

### Prerequisites

- Rust 1.88+
- Node.js 18+
- Docker & Docker Compose
- PostgreSQL 16+

### Setup

```bash
# Clone the repository
git clone <repo-url>
cd BorneMap

# Build and start all services
docker compose up -d

# Or run locally without Docker:
# 1. Backend
cd services/admin-service
cargo build --release
./target/release/admin-service

# 2. Frontend
cd apps/admin-dashboard
npm install
npm run build

# 3. Set up PostgreSQL database
psql -U bornemap -d bornemap < infrastructure/postgres/init/00-init.sql
```

## 📊 API Documentation

Base URL: `http://localhost:8080/api/v1`

### Dashboard

**GET** `/dashboard/kpis`

Returns KPI metrics for the dashboard.

**Response:**
```json
{
  "success": true,
  "data": {
    "partners_count": 10,
    "stations_count": 25,
    "chargers_count": 150
  }
}
```

### Partners

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/partners` | List all partners (paginated) |
| GET | `/partners/:id` | Get partner by ID |
| POST | `/partners` | Create new partner |
| PATCH | `/partners/:id` | Update partner name |
| PUT | `/partners/:id` | Soft delete partner |
| DELETE | `/partners/:id` | Hard delete partner |

**Request Body (POST/PATCH):**
```json
{
  "name": "GreenCharge Networks"
}
```

### Stations

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/stations` | List all stations (paginated, filterable) |
| GET | `/stations/:id` | Get station by ID |
| POST | `/stations` | Create new station |
| PATCH | `/stations/:id` | Update station name/location |
| PUT | `/stations/:id` | Soft delete station |
| DELETE | `/stations/:id` | Hard delete station |

**Query Parameters (GET /stations):**
- `page` (default: 1)
- `limit` (default: 50, max: 100)
- `partner_id` (optional filter)

**Request Body (POST/PATCH):**
```json
{
  "name": "Downtown Hub",
  "location": "123 Main St, City",
  "partner_id": "PRT-abc123"
}
```

### Chargers

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/chargers` | List all chargers (paginated, filterable) |
| GET | `/chargers/:id` | Get charger by ID |
| POST | `/chargers` | Create new charger |
| PATCH | `/chargers/:id` | Update power rating |
| PUT | `/chargers/:id` | Soft delete charger |
| DELETE | `/chargers/:id` | Hard delete charger |

**Query Parameters (GET /chargers):**
- `page` (default: 1)
- `limit` (default: 50, max: 100)
- `station_id` (optional filter)

**Request Body (POST/PATCH):**
```json
{
  "station_id": "STA-xyz789",
  "status": "ACTIVE",
  "power_rating": 150
}
```

## 🗄️ Database Schema

### EV Schema

All tables are in the `ev` schema with soft delete support.

#### Partners
- `id` (TEXT PRIMARY KEY, format: `PRT-xxxxxxxxxxxx`)
- `name` (TEXT NOT NULL)
- `status` (TEXT NOT NULL DEFAULT 'ACTIVE')
- `is_valid` (BOOLEAN NOT NULL DEFAULT TRUE)
- `created_by`, `updated_by` (TEXT)
- `created_at`, `updated_at` (TIMESTAMP)
- `deleted_at` (TIMESTAMP, nullable)

#### Stations
- `id` (TEXT PRIMARY KEY, format: `STA-xxxxxxxxxxxx`)
- `partner_id` (TEXT FOREIGN KEY → partners.id, CASCADE DELETE)
- `name` (TEXT NOT NULL)
- `location` (TEXT)
- `status` (TEXT NOT NULL DEFAULT 'ACTIVE')
- `created_by`, `updated_by` (TEXT)
- `created_at`, `updated_at` (TIMESTAMP)
- `deleted_at` (TIMESTAMP, nullable)

#### Chargers
- `id` (TEXT PRIMARY KEY, format: `CHR-xxxxxxxxxxxx`)
- `station_id` (TEXT FOREIGN KEY → stations.id, CASCADE DELETE)
- `status` (TEXT NOT NULL DEFAULT 'ACTIVE')
- `power_rating` (INTEGER, 1-1000)
- `created_by`, `updated_by` (TEXT)
- `created_at`, `updated_at` (TIMESTAMP)
- `deleted_at` (TIMESTAMP, nullable)

### Indexes

- `idx_partners_name`, `idx_partners_deleted_at`
- `idx_stations_partner_id`, `idx_stations_name`, `idx_stations_deleted_at`
- `idx_chargers_station_id`, `idx_chargers_status`, `idx_chargers_deleted_at`
- Constraint checks for ID format validation

### Active Views

- `ev.active_partners`: All partners where `deleted_at IS NULL`
- `ev.active_stations`: All stations where `deleted_at IS NULL`
- `ev.active_chargers`: All chargers where `deleted_at IS NULL`

## 🔒 Security Notes

- **Input Validation**: All user input is validated at the service layer
- **SQL Injection Prevention**: Parameterized queries via SQLx
- **SQL Connection Pool**: Configured with max 20 connections
- **Soft Delete**: Deletion is reversible (undelete functionality)

**⚠️ Production Deployment Recommendations:**
1. Enable PostgreSQL SSL (`sslmode=require`)
2. Use environment variables for database credentials (secrets manager)
3. Add rate limiting (actix_governor)
4. Implement JWT authentication
5. Enable CORS with specific origins only
6. Add health check monitoring (Prometheus/Grafana)

## 📦 Development

### Rust Backend

```bash
cd services/admin-service

# Build
cargo build --release

# Run tests
cargo test

# Check for unused code
cargo clippy
```

### Frontend

```bash
cd apps/admin-dashboard

# Install dependencies
npm install

# Start dev server
npm run dev  # Runs on port 5173 (proxies /api to backend)

# Build for production
npm run build

# Preview production build
npm run preview
```

### Docker

```bash
# Build all images
docker compose build

# Start all services
docker compose up -d

# View logs
docker compose logs -f

# Stop all services
docker compose down
```

## 🎨 Design System

### Theme: Dark OLED

- **Background**: `#020617` (slate-950) - Deep black for OLED displays
- **Surface**: `#0F172A` (slate-900) - Card backgrounds
- **Muted**: `#1A1E2F` (slate-800/50) - Secondary backgrounds
- **Border**: `#1E293B` (slate-700/50) - Subtle borders
- **Primary Accent**: `#F97316` (orange-500) - Energy/CTA
- **Success Accent**: `#22C55E` (green-500) - Active status

### Typography

- **Headings**: Fira Code (monospace)
- **Body**: Fira Sans (sans-serif)
- **Font Size**: 16px minimum for body text (iOS)

### Animations

- Page transitions: Fade-in (250ms)
- Card entrances: Slide-up (350ms)
- Loading states: Skeleton shimmer
- Hover effects: Scale (97%), border highlight

## 📈 Features

- ✅ **Full CRUD Operations**: Create, Read, Update, Delete for all entities
- ✅ **Soft Delete**: Deletion is reversible (undelete functionality)
- ✅ **Pagination**: Configurable page size (default: 50, max: 100)
- ✅ **Search/Filter**: Filter stations by partner_id, chargers by station_id
- ✅ **Input Validation**: Frontend + backend validation
- ✅ **Error Handling**: Consistent error messages via `AppError` enum
- ✅ **API Versioning**: `/api/v1` prefix for future compatibility
- ✅ **Docker Compose**: One-command deployment
- ✅ **Health Check**: `/api/v1/health` endpoint
- ✅ **Responsive UI**: Mobile-first design with collapsible sidebar
- ✅ **Loading States**: Skeleton screens and spinners

## 🔧 Configuration

### Environment Variables

#### Backend (`services/admin-service/.env`)
```
DATABASE_URL=postgres://user:pass@localhost:5432/bornemap
RUST_LOG=info
```

#### Database (Docker Compose)
```
POSTGRES_USER=bornemap
POSTGRES_PASSWORD=bomeapassword
POSTGRES_DB=bornemap
```

### File Limits

- Partner name: Max 200 characters
- Charger power rating: 1-1000 kW
- Pagination limit: 1-100 items per page
- ID format: `PRT-`/`STA-`/`CHR-` + 12 alphanumeric characters

## 🧪 Testing

### Rust
```bash
cd crates/platform-core
cargo test
```

### Frontend
```bash
cd apps/admin-dashboard
npm run test
```

## 📝 License

[Your License Here]

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request
