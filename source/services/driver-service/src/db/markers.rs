use crate::error::AppError;
use crate::models::StationSummary;

pub async fn markers_in_bbox(
    pool: &sqlx::PgPool,
    south: f64,
    west: f64,
    north: f64,
    east: f64,
    limit: i64,
) -> Result<Vec<StationSummary>, AppError> {
    let rows = sqlx::query_as::<_, StationSummary>(
        r#"
        SELECT s.id, s.name, s.address, s.latitude, s.longitude,
               sa.status AS availability_status
        FROM "ev-platform".station s
        LEFT JOIN LATERAL (
            SELECT status FROM "ev-platform".station_availability
            WHERE station_id = s.id
            ORDER BY updated_at DESC
            LIMIT 1
        ) sa ON true
        JOIN "ev-platform".partner p ON s.partner_id = p.id
        WHERE p.is_verified = true AND p.is_live = true AND p.is_active = true
          AND ST_MakeEnvelope($1, $2, $3, $4, 4326) && s.location
        LIMIT $5
        "#,
    )
    .bind(west)
    .bind(south)
    .bind(east)
    .bind(north)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}
