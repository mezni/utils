use sqlx::{PgPool, Postgres};
use crate::error::Result;
use crate::models::station::Station;
use crate::models::charger::Charger;
use crate::models::nearby_response::NearbyResponse;

/// Get nearby stations within a radius
pub async fn get_nearby(
    pool: &PgPool,
    latitude: f64,
    longitude: f64,
    radius_km: f64,
    max_results: i32,
    status_filter: &str,
) -> Result<NearbyResponse> {
    let result = sqlx::query_as!(
        NearbyResponse,
        r#"
        WITH nearby_stations AS (
            SELECT
                s.id,
                s.name,
                s.visibility,
                s.location,
                s.address,
                s.city,
                ARRAY_AGG(c.connector) FILTER (WHERE c.connector IS NOT NULL) AS connector_types,
                ARRAY_AGG(c.power_kw) FILTER (WHERE c.power_kw IS NOT NULL) AS connector_power,
                ST_Distance(
                    s.location::geography,
                    ST_MakePoint($2, $1)::geography
                ) / 1000.0 AS distance_km
            FROM inventory.station s
            LEFT JOIN inventory.charger c ON c.station_id = s.id AND c.deleted_at IS NULL
            WHERE
                s.deleted_at IS NULL
                AND s.status = $3
                AND ST_DWithin(
                    s.location::geography,
                    ST_MakePoint($2, $1)::geography,
                    $4 * 1000
                )
            GROUP BY
                s.id, s.name, s.visibility, s.location, s.address, s.city
        )
        SELECT
            id,
            name,
            visibility,
            location,
            distance_km,
            address,
            city,
            connector_types,
            connector_power
        FROM nearby_stations
        ORDER BY distance_km
        LIMIT $5
        "#,
        latitude,
        longitude,
        status_filter,
        radius_km,
        max_results
    )
    .fetch_all(pool)
    .await?;

    Ok(NearbyResponse {
        stations: result,
        count: result.len() as i32,
        radius_m: (radius_km * 1000.0) as i32,
    })
}
