# Sprint 02 — Quickstart

## Prerequisites
- PostgreSQL 15+ with PostGIS running (see Sprint 01)
- Migrations from Sprint 01 applied (schemas: users, ev, gis)
- Rust 1.90+

## Setup & Run

```bash
# 1. Ensure PostgreSQL is running
docker compose up -d postgres

# 2. Apply all migrations (Sprint 01 + Sprint 02)
docker compose exec -T postgres psql -U bornemap -d bornemap \
  -f database/migrations/0001_enable_extensions.sql
docker compose exec -T postgres psql -U bornemap -d bornemap \
  -f database/migrations/0002_create_schemas.sql
docker compose exec -T postgres psql -U bornemap -d bornemap \
  -f database/migrations/0003_create_users_accounts.sql

# 3. Set environment
export JWT_SECRET="your-secure-secret-here"
export DATABASE_URL="postgres://bornemap:bornemap_dev@localhost:5432/bornemap"

# 4. Run auth-service
cargo run -p auth-service

# 5. Test endpoints
# Register
curl -X POST http://localhost:3001/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"user@example.com","password":"mypassword123","role":"driver"}'

# Login
curl -X POST http://localhost:3001/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"user@example.com","password":"mypassword123"}'

# Health
curl http://localhost:3001/health
```

## Run Tests

```bash
JWT_SECRET=test-secret \
DATABASE_URL=postgres://bornemap:bornemap_dev@localhost:5432/bornemap \
cargo test -p auth-service
```

## Expected Responses

**Register (201):**
```json
{"token":"eyJ...","email":"user@example.com","role":"driver"}
```

**Login (200):**
```json
{"token":"eyJ...","email":"user@example.com","role":"driver"}
```

**Duplicate email (409):**
```json
{"error":"An account with this email already exists"}
```

**Wrong credentials (401):**
```json
{"error":"Invalid email or password"}
```
