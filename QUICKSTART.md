# BorneMap - Quick Start Guide

## Overview

BorneMap is an electric vehicle charging station management platform built with Rust and Actix-web. This quick start guide will help you get the authentication service running quickly.

## Prerequisites

- Rust 1.70+ 
- PostgreSQL 14+
- Docker (optional)

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
```

### 4. Build and Run Auth Service

```bash
# Build the auth service
cd services/auth-service
cargo build --release

# Run the service
cargo run --release
```

The auth service will be available at `http://localhost:8080`

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

## Testing

```bash
# Run all tests
cargo test

# Run specific test suites
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
├── shared/
│   ├── bornemap-core/     # Domain models and types
│   ├── bornemap-auth/     # JWT utilities
│   └── bornemap-db/       # Database migrations and pool
├── services/
│   └── auth-service/      # Authentication service
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

```bash
# Build and run with Docker Compose
docker-compose up -d

# View logs
docker-compose logs -f auth-service
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

## Next Steps

1. Complete remaining sprints (06-08)
2. Implement frontend application
3. Add payment integration
4. Deploy to production

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request