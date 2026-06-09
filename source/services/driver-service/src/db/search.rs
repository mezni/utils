use crate::error::AppError;
use crate::models::StationSummary;

pub async fn search_stations(
    pool: &sqlx::PgPool,
    query: &str,
    connector_type: Option<&str>,
    limit: i64,
    offset: i64,
) -> Result<Vec<StationSummary>, AppError> {
    let like_pattern = format!("%{}%", query);
    let rows = sqlx::query_as::<_, StationSummary>(
        r#"
        SELECT DISTINCT s.id, s.name, s.address, s.latitude, s.longitude,
               sa.status AS availability_status
        FROM "ev-platform".station s
        JOIN "ev-platform".partner p ON s.partner_id = p.id
        LEFT JOIN LATERAL (
            SELECT status FROM "ev-platform".station_availability
            WHERE station_id = s.id
            ORDER BY updated_at DESC
            LIMIT 1
        ) sa ON true
        LEFT JOIN "ev-platform".charger c ON c.station_id = s.id
        WHERE p.is_verified = true AND p.is_live = true AND p.is_active = true
          AND (s.name ILIKE $1 OR s.address ILIKE $1)
          AND ($2::text IS NULL OR c.connector_type = $2)
        ORDER BY s.name
        LIMIT $3
        OFFSET $4
        "#,
    )
    .bind(&like_pattern)
    .bind(connector_type)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}
