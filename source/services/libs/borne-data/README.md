# borne-data

Shared data layer library for platform_db (PostGIS) access.

## Quickstart

```bash
# Run all tests (spawns disposable PostGIS via testcontainers)
cargo test -p borne-data

# Run only query tests
cargo test -p borne-data --test queries_test

# Run only migration tests
cargo test -p borne-data --test migration_test
```

## API

```rust
use borne_data::*;

// Create a connection pool (reads DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME env vars)
let pool = create_pool().await?;

// Run pending migrations
run_migrations(&pool).await?;

// List all stations
let stations = list_all(&pool).await?;

// Find stations near a location
let nearby = find_nearby(&pool, 36.8065, 10.1815, 50_000.0).await?;

// Find station by ID (includes chargers and partner)
let detail = find_by_id(&pool, "s1").await?;
```

## Configuration

| Env Var | Default | Description |
|---|---|---|
| `DATABASE_URL` | — | Full connection URL (overrides all other vars) |
| `DB_HOST` | `localhost` | Database host |
| `DB_PORT` | `5432` | Database port |
| `DB_USER` | `postgres` | Database user |
| `DB_PASSWORD` | `postgres` | Database password |
| `DB_NAME` | `platform_db` | Database name |
| `DB_MIN_CONNECTIONS` | `2` | Minimum connection pool size |
| `DB_MAX_CONNECTIONS` | `10` | Maximum connection pool size |

## Dependencies

- SQLx 0.8 (PostgreSQL, async, compile-time checked queries)
- Tokio 1.x (async runtime)
- PostGIS 3.4+ (geospatial queries)

## Project Structure

```
borne-data/
├── migrations/       # SQLx migration files
├── src/
│   ├── lib.rs        # Re-exports
│   ├── error.rs      # DataLayerError enum
│   ├── pool.rs       # Connection pool with retry
│   ├── models/       # Partner, Station, Charger
│   ├── queries/      # Spatial query functions
│   └── migration/    # Migration runner
└── tests/
    ├── common/       # Testcontainers setup
    ├── queries_test.rs
    └── migration_test.rs
```
