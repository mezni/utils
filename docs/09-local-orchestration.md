# BorneMap — Local Orchestration Stack

## 1. Docker Compose Development Config

File: `docker-compose.dev.yml`

### Services

#### `postgres`

| Property | Value |
|----------|-------|
| Image | `postgis/postgis:16-3.4-alpine` |
| Container | `bornemap_dev_db` |
| Database | `bornemap_dev` |
| User | `bornemap_admin` |
| Password | `development_secret_key` |
| Port | `5432:5432` |
| Volume | `postgres_dev_data` (persistent) |

Healthcheck: `pg_isready -U bornemap_admin -d bornemap_dev` (interval 5s, timeout 5s, retries 5)

#### `backend-api`

| Property | Value |
|----------|-------|
| Build Context | `./sources/backend` |
| Dockerfile | `Dockerfile.dev` |
| Container | `bornemap_dev_api` |
| Port | `8080:8080` |
| Volume Mount | `./sources/backend:/app` (live reload) |
| Cache Volume | `cargo_cache:/usr/local/cargo/registry` |

Environment variables:

| Variable | Value |
|----------|-------|
| `DATABASE_URL` | `postgres://bornemap_admin:development_secret_key@postgres:5432/bornemap_dev` |
| `RUST_LOG` | `actix_web=info,bornemap_backend=debug` |

Dependency: `postgres` must be healthy before `backend-api` starts.

### Volumes

| Volume | Driver |
|--------|--------|
| `postgres_dev_data` | local |
| `cargo_cache` | local |

## 2. Backend Docker Development Image

File: `sources/backend/Dockerfile.dev`

```dockerfile
FROM rust:1.78-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install sqlx-cli --no-default-features --features postgres

EXPOSE 8080
CMD ["cargo", "run"]
```

### Build Stages

1. Base image: `rust:1.78-slim`
2. System dependencies: `pkg-config`, `libssl-dev`, `git`
3. SQLx CLI installed for in-container migration execution
4. Default command: `cargo run` (development server with live reload via volume mount)

### Development Workflow

```bash
# Start the full stack
docker compose -f docker-compose.dev.yml up

# Run migrations inside the backend container
docker compose -f docker-compose.dev.yml exec backend-api sqlx migrate run

# View backend logs
docker compose -f docker-compose.dev.yml logs backend-api -f
```
