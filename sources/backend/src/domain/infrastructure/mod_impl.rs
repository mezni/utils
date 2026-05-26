use crate::domain::infrastructure::repository;
use crate::utils::error::ProblemResponse;
use actix_web::{web, HttpResponse};
use serde::Deserialize;
use sqlx::PgPool;

#[derive(Debug, Deserialize)]
pub struct NearbyQuery {
    pub longitude: f64,
    pub latitude: f64,
    #[serde(default = "default_radius")]
    pub radius_meters: f64,
    #[serde(default)]
    pub include_test: bool,
}

fn default_radius() -> f64 {
    20000.0
}

pub async fn nearby_stations(
    pool: web::Data<PgPool>,
    query: web::Query<NearbyQuery>,
) -> HttpResponse {
    let q = query.into_inner();

    if !(-180.0..=180.0).contains(&q.longitude) {
        return ProblemResponse::validation("Longitude must be between -180 and 180");
    }
    if !(-90.0..=90.0).contains(&q.latitude) {
        return ProblemResponse::validation("Latitude must be between -90 and 90");
    }
    if q.radius_meters <= 0.0 {
        return ProblemResponse::validation("Radius must be greater than 0");
    }

    match repository::find_nearby_stations_bounded(
        &pool,
        q.longitude,
        q.latitude,
        q.radius_meters,
        q.include_test,
    )
    .await
    {
        Ok(stations) => HttpResponse::Ok().json(stations),
        Err(e) => {
            tracing::error!("Nearby query failed: {:?}", e);
            ProblemResponse::internal_error()
        }
    }
}
