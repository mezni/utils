use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use sqlx::PgPool;

use crate::error::ServiceError;
use crate::extractors::PaginationParams;
use crate::models::station::{StationListQuery, StationSearchQuery};
use crate::repository::station_repo;
use common_types::api::{ItemEnvelope, SuccessEnvelope};

use axum::Router;

pub fn routes(pool: PgPool) -> Router {
    Router::new()
        .route("/api/v1/driver/stations", axum::routing::get(list_stations))
        .route("/api/v1/driver/stations/{id}", axum::routing::get(get_station))
        .route("/api/v1/driver/stations/search", axum::routing::get(search_stations))
        .with_state(pool)
}

async fn list_stations(
    State(pool): State<PgPool>,
    Query(params): Query<StationListQuery>,
) -> Result<impl IntoResponse, ServiceError> {
    let pagination = PaginationParams {
        page: params.page,
        size: params.size,
    };

    let (stations, meta) = station_repo::list_visible_stations(&pool, &params, &pagination).await?;
    Ok(SuccessEnvelope::new(stations, meta))
}

async fn get_station(
    State(pool): State<PgPool>,
    Path(id): Path<String>,
    Query(query): Query<StationListQuery>,
) -> Result<impl IntoResponse, ServiceError> {
    let station = station_repo::get_station_detail(&pool, &id, query.lat, query.lng).await?;
    Ok(ItemEnvelope::new(station))
}

async fn search_stations(
    State(pool): State<PgPool>,
    Query(params): Query<StationSearchQuery>,
) -> Result<impl IntoResponse, ServiceError> {
    let pagination = PaginationParams {
        page: params.page,
        size: params.size,
    };

    let (stations, meta) = station_repo::search_stations(&pool, params.q.as_deref(), &pagination).await?;
    Ok(SuccessEnvelope::new(stations, meta))
}
