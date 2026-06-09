# ev-db — Public API Contract

**Crate**: `ev-db` (shared library)

**Location**: `source/crates/ev-db/src/lib.rs`

## Public Functions

### `init_pool`

```rust
pub async fn init_pool(connection_string: &str) -> Result<PgPool, PoolError>
```

Initializes a `sqlx::PgPool` from a PostgreSQL connection URI.

**Errors**:
- `PoolError::InvalidConnectionString(reason)` — connection string is missing or malformed
- `PoolError::ConnectionFailed(source)` — pool could not connect to the database

**Example**: `init_pool("postgres://user:pass@localhost:5432/borne_map").await`

---

### `init_pool_with_config`

```rust
pub async fn init_pool_with_config(config: PoolConfig) -> Result<PgPool, PoolError>
```

Initializes a PgPool from a config struct with `max_connections` and `connection_timeout` options.

**Errors**: Same as `init_pool`

## Public Struct Types

### `PoolConfig`

```rust
pub struct PoolConfig {
    pub connection_string: String,
    pub max_connections: u32,
    pub connection_timeout: Duration,
}
```

Default: `max_connections: 10`, `connection_timeout: 30s`

### `Paginated<T>`

```rust
pub struct Paginated<T> {
    pub data: Vec<T>,
    pub total: u64,
    pub page: u32,
    pub page_size: u32,
    pub total_pages: u32,
}
```

#### Constructor

```rust
impl<T> Paginated<T> {
    pub fn new(data: Vec<T>, total: u64, page: u32, page_size: u32) -> Self
}
```

**Panics**:
- If `page` is 0
- If `page_size` is 0

**Behavior**:
- `total_pages` = `total.div_ceil(page_size)` when `total > 0`
- `total_pages` = 0 when `total == 0`

## Public Error Types

### `PoolError`

```rust
#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("invalid connection string: {0}")]
    InvalidConnectionString(String),
    #[error("database connection failed: {0}")]
    ConnectionFailed(#[from] sqlx::Error),
}
```
