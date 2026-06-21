use sqlx::PgPool;

use crate::domain::nearby_query::NearbyQuery;
use crate::domain::nearby_result::NearbyResult;

pub struct NearbyRepository {
    pool: PgPool,
}

impl NearbyRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn find_nearby(&self, query: &NearbyQuery) -> Result<Vec<NearbyResult>, sqlx::Error> {
        let rows = sqlx::query_as!(
            NearbyResultRow,
            "SELECT station_id, name, distance_meters FROM inventory.find_nearby_stations($1, $2, $3)",
            query.lat(),
            query.lng(),
            query.radius_meters()
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| NearbyResult::new(r.station_id, r.name, r.distance_meters))
            .collect())
    }
}

#[derive(Debug)]
struct NearbyResultRow {
    station_id: String,
    name: String,
    distance_meters: f64,
}
