# Quickstart: Auth Service

## Prerequisites

- Docker, Docker Compose — for infrastructure (Postgres, Keycloak, Redis, Traefik)
- Rust toolchain (stable) — `rustup` with `cargo`
- `sqlx-cli` — `cargo install sqlx-cli`
- `cargo-watch` (optional) — `cargo install cargo-watch`

## Start Infrastructure

```bash
cd source/infra
docker compose up -d
```

Verify health: `docker ps --format "table {{.Names}}\t{{.Status}}"`

Wait for Keycloak to become healthy (up to 3 minutes on first boot).

## Database Setup

The `users` schema and `auth_service_role` are created by the Docker init scripts. No additional migration needed for Sprint 1.

Verify:
```bash
docker exec bornemap-postgres psql -U postgres -d platform_db -c "\dt users.*"
```

## Environment Configuration

Copy the example env file:

```bash
cp .env.example .env
```

The `.env` file in `source/infra/` contains the Keycloak credentials and Postgres passwords.

## Run Auth Service (development)

```bash
cd source/services/auth-service
cargo run
```

The service listens on `http://localhost:3000` behind Traefik at `http://localhost/api/v1/auth/`.

## Test End-to-End

### Login

```bash
curl -s -X POST http://localhost/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@bornemap.tn","password":"test123"}'
```

### Refresh

```bash
curl -s -X POST http://localhost/api/v1/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{"refresh_token":"<token from login>"}'
```

### Logout

```bash
curl -s -X POST http://localhost/api/v1/auth/logout \
  -H "Content-Type: application/json" \
  -d '{"refresh_token":"<token from login>"}'
```

## Run Tests

```bash
cargo test                # unit + integration
cargo clippy -- -D warnings  # lint
```

Integration tests require the full Docker stack (Keycloak + Postgres) to be running.

## Useful Docker Commands

```bash
# View Keycloak logs
docker logs bornemap-keycloak -f

# Access Postgres directly
docker exec -it bornemap-postgres psql -U postgres -d platform_db

# Reset everything
docker compose -f source/infra/docker-compose.yml down -v
```
