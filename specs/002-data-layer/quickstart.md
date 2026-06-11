# Quickstart: Data Layer

## Prerequisites

- Rust 1.80+ (install via rustup)
- Docker (for integration tests via testcontainers)
- Docker Compose (from Sprint 0 — for local platform_db)

## Setup

```bash
# 1. Start infrastructure (platform_db with PostGIS)
cd infra
docker compose up -d

# 2. Verify PostGIS is ready
docker exec bornemap-platform-db psql -U postgres -d platform_db -c "SELECT PostGIS_Version();"

# 3. Run the data layer integration tests
cd source/services
cargo test -p borne-data
```

## Library Usage

```rust
use borne_data::{create_pool, stations};

#[tokio::main]
async fn main() {
    let pool = create_pool().await.expect("DB connection failed");

    // Find stations near Tunis center (10km radius)
    let nearby = stations::find_nearby(&pool, 36.8065, 10.1815, 10_000.0)
        .await
        .expect("Query failed");

    println!("Found {} stations nearby", nearby.len());
}
```

## Development Workflow

1. Make changes to `source/services/libs/borne-data/`
2. Run `cargo test` to verify
3. Run `cargo clippy` for linting
4. Commit and push

