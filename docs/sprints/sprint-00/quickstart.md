# BorneMap — Quickstart Guide

## Prerequisites

- **Rust** (latest stable via rustup)
- **PostgreSQL 16+** with **PostGIS 3.4+**
- **Docker** (optional, for local DB)

## 1. Clone and Setup

```bash
git clone git@github.com:mezni/BorneMap.git
cd BorneMap
```

## 2. Database Setup

### Option A: Docker (recommended for development)

```bash
docker run -d \
  --name bornemap-db \
  -e POSTGRES_PASSWORD=bornemap \
  -e POSTGRES_DB=bornemap \
  -p 5432:5432 \
  postgis/postgis:16-3.4
```

### Option B: Local PostgreSQL

Ensure PostgreSQL + PostGIS are installed:

```bash
# Ubuntu/Debian
sudo apt install postgresql-16 postgresql-16-postgis-3

# macOS
brew install postgresql@16 postgis
```

### 3. Run Migrations

```bash
# SQLx CLI
cargo install sqlx-cli
sqlx migrate run
```

### 4. Build and Run

```bash
# Admin Service
cargo run -p admin-service

# Driver Service
cargo run -p driver-service

# Auth Service
cargo run -p auth-service
```

## 5. Verify

```bash
# Health check
curl http://localhost:8080/api/v1/health

# Create a partner (Admin Service)
curl -X POST http://localhost:8081/api/v1/partners \
  -H "Content-Type: application/json" \
  -d '{"name": "Tesla Tunisia"}'
```

## 6. Project Structure

```
BorneMap/
├── services/
│   ├── admin-service/       # EV domain write API
│   ├── driver-service/      # Public read-only GIS API
│   └── auth-service/        # Identity & JWT
├── migrations/              # SQLx DB migrations
├── docs/                    # Sprint documentation
└── Cargo.toml               # Workspace manifest
```

## 7. Useful Commands

```bash
# Run all tests
cargo test --workspace

# Run linting
cargo clippy --workspace

# Check formatting
cargo fmt --check

# Run specific service
cargo run -p admin-service
```
