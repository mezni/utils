use crate::error::AppError;
use crate::models::{AvailabilityResponse, CreateAvailabilityRequest};
use sqlx::PgPool;

pub async fn create_availability(
    pool: &PgPool,
    station_id: &str,
    req: CreateAvailabilityRequest,
    actor: &str,
) -> Result<AvailabilityResponse, AppError> {
    let id = ev_core::generate_id("SA", 3);
    let rec = sqlx::query_as::<_, AvailabilityResponse>(
        r#"
        INSERT INTO "ev-platform".station_availability (id, station_id, status, updated_by, updated_at)
        VALUES ($1, $2, $3, $4, NOW())
        RETURNING *
        "#,
    )
    .bind(&id)
    .bind(station_id)
    .bind(&req.status)
    .bind(actor)
    .fetch_one(pool)
    .await
    .map_err(|e| {
        if let sqlx::Error::Database(ref db_err) = e
            && let Some(code) = db_err.code()
            && code.as_ref() == "23503"
        {
            return AppError::NotFound(format!("Station {} not found", station_id));
        }
        AppError::from(e)
    })?;
    Ok(rec)
}
