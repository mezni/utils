use sqlx::PgPool;

pub async fn run(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("../../database/migrations")
        .run(pool)
        .await
}
