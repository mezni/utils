use crate::error::AppError;
use crate::models::StationNearby;

pub async fn nearby_stations(
    pool: &sqlx::PgPool,
    lat: f64,
    lng: f64,
    radius: f64,
    limit: i64,
    offset: i64,
) -> Result<Vec<StationNearby>, AppError> {
    let rows = sqlx::query_as::<_, StationNearby>(
        r#"
        SELECT
            s.id,
            s.name,
            s.address,
            s.latitude,
            s.longitude,
            sa.status AS availability_status,
            ST_Distance(s.location::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography) AS distance_meters
        FROM "ev-platform".station s
        LEFT JOIN LATERAL (
            SELECT status FROM "ev-platform".station_availability
            WHERE station_id = s.id
            ORDER BY updated_at DESC
            LIMIT 1
        ) sa ON true
        JOIN "ev-platform".partner p ON s.partner_id = p.id
        WHERE p.is_verified = true AND p.is_live = true AND p.is_active = true
          AND ST_DWithin(s.location, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, $3)
        ORDER BY distance_meters
        LIMIT $4
        OFFSET $5
        "#,
    )
    .bind(lng)
    .bind(lat)
    .bind(radius)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}
