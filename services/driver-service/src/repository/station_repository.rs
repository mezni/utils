use sqlx::PgPool;

use crate::models::error::Result;
use crate::models::nearby_response::NearbyResponse;
use crate::models::station::Station;

pub async fn get_nearby(
    pool: &PgPool,
    latitude: f64,
    longitude: f64,
    radius_m: f64,
    max_results: i32,
    status_filter: &str,
) -> Result<NearbyResponse> {
    let rows = sqlx::query_as!(
        Station,
        r#"
        SELECT
            s.id,
            s.name,
            s.visibility as "visibility!",
            ST_X(s.location::geometry) AS lat,
            ST_Y(s.location::geometry) AS lon,
            ST_Distance(
                s.location::geography,
                ST_MakePoint($2, $1)::geography
            ) AS distance_m,
            s.address,
            s.city,
            ARRAY_AGG(c.connector) FILTER (WHERE c.connector IS NOT NULL) AS "connector_types?: Vec<String>",
            ARRAY_AGG(c.power_kw) FILTER (WHERE c.power_kw IS NOT NULL) AS "connector_power?: Vec<f64>"
        FROM inventory.station s
        LEFT JOIN inventory.charger c ON c.station_id = s.id AND c.deleted_at IS NULL
        WHERE
            s.deleted_at IS NULL
            AND s.status = $3::station_status
            AND ST_DWithin(
                s.location::geography,
                ST_MakePoint($2, $1)::geography,
                $4
            )
        GROUP BY
            s.id, s.name, s.visibility, s.location, s.address, s.city
        ORDER BY distance_m
        LIMIT $5
        "#,
        latitude,
        longitude,
        status_filter,
        radius_m,
        max_results
    )
    .fetch_all(pool)
    .await?;

    let count = rows.len() as i32;

    Ok(NearbyResponse {
        stations: rows,
        count,
        radius_m: radius_m as i32,
    })
}
