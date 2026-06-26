use sqlx::migrate::{MigrateError, Migrator};

static MIGRATOR: Migrator = sqlx::migrate!();

pub async fn run_migrations(pool: &sqlx::PgPool) -> Result<(), MigrateError> {
    MIGRATOR.run(pool).await
}
