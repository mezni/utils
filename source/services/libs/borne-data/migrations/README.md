# Migrations

Migration files are plain SQL named with a timestamp prefix: `YYYYMMDDHHMMSS_description.sql`.

Applied via `borne_data::run_migrations(pool)` which uses SQLx migrate internally.

## Adding a Migration

1. Create a new file: `YYYYMMDDHHMMSS_description.sql`
2. Write your SQL changes
3. Run `cargo test` to verify

The tracking table `_sqlx_migrations` records all applied migrations.
