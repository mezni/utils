# Quickstart: Infrastructure Foundation

## Prerequisites

- Docker Engine 24+
- Docker Compose plugin v2+
- 4 GB RAM, 10 GB disk minimum

## Setup

```bash
# 1. Clone the repository
git clone <repo-url> && cd <repo>

# 2. Configure environment
cp .env.example .env
# Edit .env if custom values needed (defaults work for local dev)

# 3. Start the stack
docker compose up -d

# 4. Verify all services are healthy
docker compose ps
# Expected: all 5 services show "Up" status

# 5. Check health endpoints
curl -f http://localhost/api/health

# 6. Access services
# Keycloak admin console: http://localhost/auth/admin/
# RabbitMQ management:   http://localhost:15672
```

## Verification

```bash
# Test PostgreSQL connectivity
docker compose exec postgis pg_isready -U bornemap

# Test MongoDB connectivity
docker compose exec mongodb mongosh --eval "db.runCommand({ping:1})" clickstream

# Test RabbitMQ
docker compose exec rabbitmq rabbitmq-diagnostics check_running

# Test Traefik routing
curl -sI http://localhost/auth/ | head -1
# Expected: HTTP/1.1 302 Found (redirects to Keycloak login)

# View logs
docker compose logs -f
```

## Stop & Restart

```bash
# Stop all services (data persists)
docker compose down

# Restart stack
docker compose up -d

# Full clean reset (destroys volumes)
docker compose down -v && docker compose up -d
```

## Configuration

All configurable parameters are in `.env`. See `.env.example` for the
complete list with defaults. Never commit `.env` to version control.
