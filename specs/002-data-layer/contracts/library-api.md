# Library API Contract: `borne-data`

**Version**: 0.1.0 | **Status**: Draft

## Public API Surface

All public types are re-exported from `borne_data::*`.

### Connection

```rust
/// Create a connection pool from environment variables.
/// Reads: DATABASE_URL (or DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
pub async fn create_pool() -> Result<PgPool, DataLayerError>;

/// Create a connection pool with explicit configuration.
pub async fn create_pool_with_config(config: DbConfig) -> Result<PgPool, DataLayerError>;

pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub db_name: String,
    pub min_connections: u32,
    pub max_connections: u32,
}
```

### Error Types

```rust
pub enum DataLayerError {
    Connection(String),       // DB unreachable, auth failure
    Query(String),            // SQL error, constraint violation
    NotFound(String),         // Entity not found by ID
    Migration(String),        // Migration failure
    PoolExhausted,            // All connections in use
}
```

### Models

```rust
pub struct Partner {
    pub id: String,
    pub name: String,
    pub r#type: PartnerType,  // Business | Personal
    pub is_verified: bool,
    pub is_active: bool,
    pub is_live: bool,
    pub created_at: DateTime<Utc>,
    pub created_by: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub updated_by: Option<String>,
}

pub struct Station {
    pub id: String,
    pub partner_id: String,
    pub name: String,
    pub address: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    pub created_at: DateTime<Utc>,
    pub created_by: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub updated_by: Option<String>,
}

pub struct Charger {
    pub id: String,
    pub station_id: String,
    pub connector_type: String,
    pub power_kw: f64,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub created_by: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub updated_by: Option<String>,
}

pub enum PartnerType {
    Business,
    Personal,
}
```

### Queries

```rust
/// Find stations within radius_meters of (lat, lng), ordered by distance ascending.
pub mod stations {
    pub async fn find_nearby(
        pool: &PgPool,
        lat: f64,
        lng: f64,
        radius_meters: f64,
    ) -> Result<Vec<Station>, DataLayerError>;

    /// Get station by ID with charger and partner details.
    pub async fn find_by_id(
        pool: &PgPool,
        id: &str,
    ) -> Result<StationDetail, DataLayerError>;

    /// List all stations (lightweight, no chargers).
    pub async fn list_all(
        pool: &PgPool,
    ) -> Result<Vec<Station>, DataLayerError>;
}

pub struct StationDetail {
    pub station: Station,
    pub chargers: Vec<Charger>,
    pub partner: Partner,
}
```

### Migration

```rust
/// Run all pending migrations from the embedded migrations directory.
pub async fn run_migrations(pool: &PgPool) -> Result<(), DataLayerError>;
```

## Error Handling

- All public functions return `Result<T, DataLayerError>`
- Connection errors: retry up to 3 times with exponential backoff (1s, 2s, 4s)
- Query errors: returned immediately with SQL error context
- Not found: `DataLayerError::NotFound` with the entity type and ID
- Pool exhaustion: `DataLayerError::PoolExhausted` — caller should retry with backoff

## Configuration

Environment variables:
- `DATABASE_URL` — full connection string (overrides individual vars)
- `DB_HOST` — default: `localhost`
- `DB_PORT` — default: `5432`
- `DB_USER` — default: `postgres`
- `DB_PASSWORD` — default: `postgres`
- `DB_NAME` — default: `platform_db`
- `DB_MIN_CONNECTIONS` — default: `2`
- `DB_MAX_CONNECTIONS` — default: `10`

