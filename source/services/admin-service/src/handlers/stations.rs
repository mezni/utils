use actix_web::{web, HttpResponse};
use sqlx::PgPool;
use crate::domain::{CreateStationRequest, UpdateStationLiveRequest, CreateResponse};
use crate::error::AdminServiceError;
use services_shared::domain::StationDto;

/// Create a new charging station
#[utoipa::path(
    post,
    path = "/admin/stations",
    request_body = CreateStationRequest,
    responses(
        (status = 201, description = "Station created successfully", body = CreateResponse<StationDto>),
        (status = 400, description = "Invalid request or out of bounds"),
        (status = 404, description = "Partner not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[actix_web::post("/admin/stations")]
pub async fn create_station(
    req: web::Json<CreateStationRequest>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AdminServiceError> {
    let station = crate::usecase::create_station(
        pool.get_ref(),
        req.partner_id.clone(),
        req.name.clone(),
        req.address.clone(),
        req.email.clone(),
        req.latitude,
        req.longitude,
    )
    .await?;

    tracing::info!("Station created: {}", station.id);

    Ok(HttpResponse::Created().json(CreateResponse {
        data: station,
        message: "Station created successfully".to_string(),
    }))
}

/// Update station live status (publish/unpublish from map)
#[utoipa::path(
    patch,
    path = "/admin/stations/{station_id}/live",
    request_body = UpdateStationLiveRequest,
    params(
        ("station_id" = String, Path, description = "Station ID")
    ),
    responses(
        (status = 200, description = "Station updated successfully", body = StationDto),
        (status = 404, description = "Station not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[actix_web::patch("/admin/stations/{station_id}/live")]
pub async fn update_station_live_status(
    station_id: web::Path<String>,
    req: web::Json<UpdateStationLiveRequest>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AdminServiceError> {
    let station = crate::usecase::update_station_live_status(
        pool.get_ref(),
        &station_id,
        req.is_live,
    )
    .await?;

    tracing::info!("Station {} live status updated to {}", station_id, req.is_live);

    Ok(HttpResponse::Ok().json(station))
}
