# BorneMap - Quick Start Guide

## Overview

BorneMap is an electric vehicle charging station management platform built with Rust and Actix-web. This quick start guide will help you get the authentication service running quickly.

## Prerequisites

- Rust 1.70+
- PostgreSQL 14+
- Redis 7+ (optional, for OAuth state & rate limiting)
- Docker (optional)
- `gcc`, `perl`, `make` (for vendored OpenSSL build, if system `libssl-dev` unavailable)

## Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/your-org/BorneMap.git
cd BorneMap
```

### 2. Database Setup

```bash
# Create PostgreSQL database
createdb bornemap_dev

# Run migrations
cd shared/bornemap-db
cargo run --bin migrate
```

### 3. Configure Environment

Create a `.env` file in the project root:

```bash
# JWT Configuration
JWT_SECRET=your-super-secret-jwt-key-change-this-in-production
JWT_ACCESS_TTL_MINUTES=15
JWT_REFRESH_TTL_DAYS=7
JWT_ISSUER=bornemap
JWT_AUDIENCE=bornemap-app

# Database Configuration
DATABASE_URL=postgresql://localhost:5432/bornemap_dev

# Redis Configuration
REDIS_URL=redis://localhost:6379
RATE_LIMIT_REQUESTS=100
RATE_LIMIT_WINDOW_SECONDS=60
OAUTH_STATE_TTL=300
```

### 4. Build and Run Auth Service

```bash
# Build the auth service
cd services/auth-service
cargo build --release

# Run the service (requires PostgreSQL + Redis)
DATABASE_URL=postgresql://localhost:5432/bornemap_dev \
REDIS_URL=redis://localhost:6379 \
JWT_SECRET=your-secret \
cargo run --release
```

The auth service will be available at `http://localhost:8080`

> **Note:** If `libssl-dev` is not installed on your system, OpenSSL is compiled from source using the `vendored` feature. Ensure `gcc`, `perl`, and `make` are available.

### 5. Test the API

```bash
# Register a new user
curl -X POST http://localhost:8080/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "password": "ValidPassword123!"
  }'

# Login with user
curl -X POST http://localhost:8080/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "password": "ValidPassword123!"
  }'

# Refresh token
curl -X POST http://localhost:8080/api/v1/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{
    "refresh_token": "your-refresh-token-here"
  }'

# Logout
curl -X POST http://localhost:8080/api/v1/auth/logout \
  -H "Authorization: Bearer your-access-token"
```

## API Endpoints

### Authentication

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/auth/register` | Register new user |
| POST | `/api/v1/auth/login` | User login |
| POST | `/api/v1/auth/refresh` | Refresh access token |
| POST | `/api/v1/auth/logout` | User logout |

### Health Check

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health/live` | Health check |
| GET | `/health/ready` | Readiness check |

### Observability

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/metrics` | Prometheus metrics (text format) |

**Metrics exposed:**
- `http_requests_total` — counter with labels `method`, `path`, `status`
- `http_request_duration_seconds` — histogram with labels `method`, `path`
- `http_active_requests` — gauge (currently active requests)

### 6. Run Admin Dashboard

> **Prerequisites:** Node.js 18+, npm 9+

```bash
# Navigate to admin dashboard
cd apps/admin-dashboard

# Install dependencies
npm install

# Start development server (runs on :5173, proxies /api to :8080)
npm run dev
```

The admin dashboard will be available at `http://localhost:5173`. It proxies `/api/*` requests to the auth service at `http://localhost:8080`.

> **Note:** The auth service must be running (step 4) before the dashboard can log in.

## Testing

```bash
# Auth service — run all Rust tests
cargo test

# Admin dashboard — run all frontend tests
cd apps/admin-dashboard && npm test

# Run specific Rust test suites
cargo test validation
cargo test use_cases
cargo test integration

# Run with verbose output
cargo test -- --nocapture
```

## Development

### Project Structure

```
BorneMap/
├── apps/
│   └── admin-dashboard/   # Admin Dashboard (React 19 + Vite + Tailwind v4)
├── shared/
│   ├── bornemap-core/     # Domain models and types
│   ├── bornemap-auth/     # JWT utilities
│   └── bornemap-db/       # Database migrations and pool
├── services/
│   └── auth-service/      # Authentication service (Actix-web)
├── docs/                 # Documentation
└── tests/                # Integration tests
```

### Running in Development

```bash
# Run with auto-reload
cargo install cargo-watch
cargo watch -x "run"

# Run with debug logging
RUST_LOG=debug cargo run
```

### Code Quality

```bash
# Format code
cargo fmt

# Lint code
cargo clippy --all-targets --all-features -- -D warnings

# Check all
cargo check --all-targets --all-features
```

## Docker Support (Optional)

The Docker Compose stack includes PostgreSQL and Redis services:

```bash
# Build and run with Docker Compose
docker-compose up -d

# View logs
docker-compose logs -f auth-service

# The stack includes:
#   - auth-service (port 8080)
#   - postgres (port 5432)
#   - redis (port 6379)
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `JWT_SECRET` | Yes | - | JWT signing secret |
| `JWT_ACCESS_TTL_MINUTES` | No | 15 | Access token TTL |
| `JWT_REFRESH_TTL_DAYS` | No | 7 | Refresh token TTL |
| `JWT_ISSUER` | No | bornemap | JWT issuer |
| `JWT_AUDIENCE` | No | bornemap-app | JWT audience |
| `DATABASE_URL` | Yes | - | PostgreSQL connection string |
| `REDIS_URL` | No | `redis://localhost:6379` | Redis connection string |
| `RATE_LIMIT_REQUESTS` | No | 100 | Max requests per window |
| `RATE_LIMIT_WINDOW_SECONDS` | No | 60 | Rate limit window duration |
| `OAUTH_STATE_TTL` | No | 300 | OAuth state TTL in seconds |

## Troubleshooting

### Database Connection Issues

```bash
# Check PostgreSQL is running
sudo systemctl status postgresql

# Verify database exists
psql -U postgres -c "\l bornemap_dev"
```

### JWT Configuration

```bash
# Test JWT generation
cd services/auth-service
cargo run --example jwt_test
```

### Port Already in Use

```bash
# Find and kill process using port 8080
lsof -ti:8080 | xargs kill -9
```

## Getting Help

- Documentation: `docs/`
- API Contract: `docs/API_CONTRACT.md`
- Sprint Reports: `docs/sprints/`
- Issues: GitHub Issues

## Known Issues

1. **Migration 003 will fail** — `shared/bornemap-db/migrations/202406260003_add_oauth_accounts.sql` tries to `CREATE TABLE oauth_accounts` which was already created by `202406260001_init_auth.sql`. Must be converted to `ALTER TABLE` before deploying.
2. **Redis connections block the async runtime** — `RedisClient` uses synchronous `get_connection()` under all async methods. Works for development but will block tokio under load. Requires the `aio` feature from the `redis` crate.
3. **Rate limiter has a TOCTOU race** — `INCR` + `SETEX` is not atomic. Two concurrent requests near the limit can both pass. Fix planned via Redis Lua scripting.
4. **No Dockerfile for auth-service** — `infra/docker-compose.yml` references `services/auth-service/Dockerfile` which doesn't exist.

## Next Steps

1. Sprint 10 — User Profile & Account Management
2. Implement driver-service backend
3. Implement admin-service backend
4. Add payment integration
5. Deploy to production

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request