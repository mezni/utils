# Guardrail — PostgreSQL

Applies to: all migrations in `infra/migrations/`, all sqlx queries in `services/` and `crates/`

---

## Schema ownership (hard rules)

| Schema | Owner | Who can write | DB role |
|--------|-------|--------------|---------|
| `gis` | OSM importer pipeline | `osm-importer` script only | — |
| `inventory` | Driver Service (reads) / Admin Service (writes to partners, stations, chargers) | Driver Service, Admin Service | `driver_service_role` (read-only), `admin_service_role` (read/write) |
| `users` | Auth Service | Auth Service only | `auth_service_role` |
| `keycloak_db` | Keycloak runtime | Keycloak only — no application code | — |
| `analytics_db` | Admin Service | Admin Service only — event logs, audit trails, partner modification history | `admin_analytics_role` |

Any code that writes to a schema it does not own is a blocking violation.

---

## Migration rules

Every schema change ships as a numbered migration file. No exceptions.

```
infra/migrations/
  0001_init_schemas.sql
  0002_create_users_table.sql
  0003_create_stations_table.sql
  0004_add_station_geom_index.sql
```

Rules:
- File names: `NNNN_descriptive_name.sql` (zero-padded to 4 digits).
- Every migration is idempotent where possible (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`).
- Destructive migrations (DROP, ALTER with data loss) require an ADR comment at the top of the file.
- Never edit a migration that has already been applied to any environment. Write a new one.
- Down migrations are optional but encouraged for development convenience.
- Run `sqlx migrate run` in CI — never apply migrations manually in production.

---

## Table design rules

```sql
-- Every table follows this baseline pattern
CREATE TABLE inventory.stations (
    -- NanoID primary key with service-specific prefix (STA-, CHG-, OPR-, USR-)
    id          TEXT        PRIMARY KEY CHECK (id ~ '^(STA|CHG|OPR|USR)-.+'),

    -- Foreign keys reference by ID string, not serial int
    partner_id  TEXT        NOT NULL REFERENCES inventory.partners(id),

    -- Required audit columns on every table
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Soft delete: NULL means active, timestamp means deleted
    -- Only on infrastructure entities (stations, chargers, partners)
    deleted_at  TIMESTAMPTZ,

    -- Geometry: always SRID 4326 (WGS 84)
    location    GEOMETRY(POINT, 4326) NOT NULL
);

-- updated_at trigger (required on every table with this column)
CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON inventory.stations
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();
```

Rules:
- Primary keys are `TEXT` NanoID strings with the entity prefix (STA-, CHG-, OPR-, USR-). The CHECK constraint uses a regex (`~`) to match the literal prefix: `id ~ '^(STA|CHG|OPR|USR)-.+'`.
- Every table has `created_at` and `updated_at`. The trigger is mandatory.
- Soft delete (`deleted_at`) is used on infrastructure entities only. User tables use hard delete.
- All geometry columns use SRID 4326. Never store raw lat/lng floats alongside a geometry column — the geometry is the source of truth.
- Enum-like values use PostgreSQL `CHECK` constraints or a lookup table. Never magic strings without a constraint.

---

## Index rules

```sql
-- Spatial index: required on every geometry column
CREATE INDEX idx_stations_location ON inventory.stations USING GIST (location);

-- Partial index: always filter soft-deleted rows
CREATE INDEX idx_stations_active ON inventory.stations (partner_id)
    WHERE deleted_at IS NULL;

-- Composite index: column order matches the most common query filter order
CREATE INDEX idx_chargers_station_status ON inventory.chargers (station_id, status)
    WHERE deleted_at IS NULL;
```

Rules:
- Every `GEOMETRY` column MUST have a GIST index.
- Every foreign key column MUST have a btree index.
- Partial indexes with `WHERE deleted_at IS NULL` are required on tables with soft delete.
- Never add an index without a corresponding query in the codebase that uses it.
- `EXPLAIN ANALYZE` output must be included as a comment in the migration when adding an index for a specific slow query.

---

## Query rules

```rust
// ✅ Correct: compile-time checked macro
sqlx::query_as!(
    StationSummary,
    r#"
    SELECT
        s.id,
        s.name,
        ST_X(s.location::geometry) AS "longitude!",
        ST_Y(s.location::geometry) AS "latitude!",
        ST_Distance(s.location, ST_MakePoint($2, $1)::geography) AS "distance_m!"
    FROM inventory.stations s
    WHERE s.deleted_at IS NULL
      AND ST_DWithin(s.location, ST_MakePoint($2, $1)::geography, $3)
    ORDER BY distance_m ASC
    LIMIT 100
    "#,
    lat,
    lng,
    radius_m as f64
)
.fetch_all(&self.pool)
.await
.map_err(AppError::Database)

// ❌ Wrong: never do this
let query = format!("SELECT * FROM stations WHERE id = '{}'", id);
```

Rules:
- `sqlx::query!` and `sqlx::query_as!` macros only. No string formatting.
- `ST_DWithin` over geography type for distance queries (accurate for Tunisia's lat range, uses spheroid).
- Always filter `WHERE deleted_at IS NULL` on soft-deleted tables — never omit.
- Alias computed columns with the `!` suffix in sqlx macros to assert non-null: `AS "distance_m!"`.
- `LIMIT` on all list queries. Default: 100. Never unbounded queries.
- Use `fetch_optional` for single-row lookups, `fetch_all` for lists, `fetch_one` never.

---

## Transaction rules

```rust
// Any operation that writes to more than one table MUST use an explicit transaction
let mut tx = pool.begin().await?;

sqlx::query!("INSERT INTO inventory.stations ...", ...)
    .execute(&mut *tx)
    .await?;

sqlx::query!("INSERT INTO inventory.chargers ...", ...)
    .execute(&mut *tx)
    .await?;

tx.commit().await?;

// Cache bust AFTER commit — never before
cache.invalidate_nearby(lat, lng).await?;
```

Rules:
- Any write touching more than one table: explicit transaction, no exceptions.
- Cache bust always happens after `tx.commit()` — a failed cache bust is logged but does not roll back the transaction.
- Transaction scope is the repository method, not the service method. Services orchestrate; repositories transact.

---

## Materialized view rules

Three materialized views exist in `inventory`:

- `mv_stations_geo` — spatial summary for map display (location, status, charger count)
- `mv_stations_summary` — list view data (name, address, rating, amenities)
- `mv_stations_reviews` — aggregated review stats

Rules:
- Driver Service reads from materialized views for all GET queries. Never query base tables directly for reads.
- Materialized views are refreshed synchronously by Admin Service after any station or charger write via `REFRESH MATERIALIZED VIEW CONCURRENTLY`.
- `CONCURRENTLY` is required — blocking refresh locks the view and kills Driver Service reads.
- Each view must have a unique index to support `CONCURRENTLY` refresh.

---

## Self-check before submitting

- [ ] Every schema change has a numbered migration file
- [ ] Every new geometry column has a GIST index in the same migration
- [ ] Every new FK column has a btree index
- [ ] No raw SQL strings — `sqlx::query!` macros only
- [ ] All queries filter `WHERE deleted_at IS NULL` on soft-deleted tables
- [ ] Multi-table writes wrapped in explicit transactions
- [ ] Cache bust happens after `tx.commit()`, not before
- [ ] `REFRESH MATERIALIZED VIEW CONCURRENTLY` used (not blocking refresh)
- [ ] No application code connects to `keycloak_db`
