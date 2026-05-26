use crate::domain::infrastructure::NearbyStationResult;
use sqlx::PgPool;

pub async fn find_nearby_stations_bounded(
    pool: &PgPool,
    user_lng: f64,
    user_lat: f64,
    radius_meters: f64,
    include_test: bool,
) -> Result<Vec<NearbyStationResult>, sqlx::Error> {
    sqlx::query_as::<_, NearbyStationResult>(
        r#"
        SELECT
            s.id as station_id,
            s.name as station_name,
            s.address,
            s.city,
            ST_X(s.coordinates::geometry) as longitude,
            ST_Y(s.coordinates::geometry) as latitude,
            ST_Distance(s.coordinates, ST_MakePoint($1, $2)::geography) as distance_meters,
            COUNT(c.id) FILTER (WHERE c.status = 'available') as available_chargers_count,
            s.is_test
        FROM stations s
        LEFT JOIN chargers c ON c.station_id = s.id
        WHERE s.deleted_at IS NULL
          AND ST_DWithin(s.coordinates, ST_MakePoint($1, $2)::geography, $3)
          AND ($4 = TRUE OR s.is_test = FALSE)
        GROUP BY s.id
        ORDER BY distance_meters ASC
        LIMIT 50
        "#,
    )
    .bind(user_lng)
    .bind(user_lat)
    .bind(radius_meters)
    .bind(include_test)
    .fetch_all(pool)
    .await
}
