use actix_web::{web, HttpResponse};
use ev_core::error::AppError;
use crate::AppState;

pub async fn list_stations(
    state: web::Data<AppState>,
    query: web::Query<PageParams>,
) -> Result<HttpResponse, AppError> {
    let page = query.page.unwrap_or(1).max(1);
    let per_page = query.per_page.unwrap_or(20).clamp(1, 100);

    let result = ev_db::queries::stations::list_stations(&state.db_pool, page, per_page).await?;

    Ok(HttpResponse::Ok().json(result))
}

pub async fn nearby_stations(
    state: web::Data<AppState>,
    query: web::Query<NearbyParams>,
) -> Result<HttpResponse, AppError> {
    let lat = query.lat;
    let lng = query.lng;
    let radius_km = query.radius;

    let mut errors: Vec<ev_core::error::FieldError> = Vec::new();
    if !(-90.0..=90.0).contains(&lat) {
        errors.push(ev_core::error::FieldError {
            field: "lat".into(),
            message: "Latitude must be between -90 and 90".into(),
        });
    }
    if !(-180.0..=180.0).contains(&lng) {
        errors.push(ev_core::error::FieldError {
            field: "lng".into(),
            message: "Longitude must be between -180 and 180".into(),
        });
    }
    if radius_km < 0.1 || radius_km > 100.0 {
        errors.push(ev_core::error::FieldError {
            field: "radius".into(),
            message: "Radius must be between 0.1 and 100 km".into(),
        });
    }
    if !errors.is_empty() {
        return Err(AppError::Validation { details: errors });
    }

    let stations = ev_db::queries::stations::find_nearby_stations(
        &state.db_pool, lat, lng, radius_km,
    )
    .await?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "data": stations,
        "total": stations.len()
    })))
}

pub async fn get_station(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let station_id = path.into_inner();

    let mut station = ev_db::queries::stations::find_station_by_id(&state.db_pool, &station_id).await?;
    let chargers = ev_db::queries::stations::find_chargers_by_station_id(&state.db_pool, &station_id).await?;
    station.chargers = Some(chargers);

    Ok(HttpResponse::Ok().json(station))
}

#[derive(serde::Deserialize)]
pub struct PageParams {
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

#[derive(serde::Deserialize)]
pub struct NearbyParams {
    pub lat: f64,
    pub lng: f64,
    pub radius: f64,
}
