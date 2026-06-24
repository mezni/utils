use sqlx::PgPool;
use crate::domain::station::Station;
use crate::domain::errors::NearbyError;

pub struct PgStationRepository {
    pool: PgPool,
}

impl PgStationRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn find_nearby(
        &self,
        lat: f64,
        lon: f64,
        radius: i32,
        limit: i32,
    ) -> Result<Vec<Station>, NearbyError> {
        let rows = sqlx::query_as::<_, StationRow>(
            r#"SELECT station_id, name, lat, lon, distance_km
               FROM gis.find_nearby_stations($1, $2, $3, $4)"#,
        )
        .bind(lat)
        .bind(lon)
        .bind(radius)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| Station {
            station_id: r.station_id,
            name: r.name,
            lat: r.lat,
            lon: r.lon,
            distance_km: r.distance_km,
        }).collect())
    }
}

#[derive(sqlx::FromRow)]
struct StationRow {
    station_id: String,
    name: Option<String>,
    lat: f64,
    lon: f64,
    distance_km: f64,
}
