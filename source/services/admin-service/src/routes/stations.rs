use actix_web::{web, HttpResponse};
use ev_core::error::AppError;
use ev_core::station::CreateStationRequest;
use ev_core::id::{EntityPrefix, generate_entity_id};
use crate::AppState;

pub async fn create_station(
    state: web::Data<AppState>,
    body: web::Json<CreateStationRequest>,
) -> Result<HttpResponse, AppError> {
    ev_db::queries::stations::validate_create_request(
        &body.name, &body.address, body.lat, body.lng, &body.chargers,
    )?;

    ev_db::queries::stations::validate_partner_exists(&state.platform_db, &body.partner_id).await?;

    let station_id = generate_entity_id(EntityPrefix::Station);

    let station = ev_db::queries::stations::insert_station_with_chargers(
        &state.platform_db,
        &station_id,
        &body.name,
        &body.address,
        body.lat,
        body.lng,
        &body.partner_id,
        body.opening_hours.as_deref(),
        &body.chargers,
    )
    .await?;

    Ok(HttpResponse::Created().json(station))
}

pub async fn update_station(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<ev_core::station::UpdateStationRequest>,
) -> Result<HttpResponse, AppError> {
    let station_id = path.into_inner();

    let station = ev_db::queries::stations::update_station(
        &state.platform_db,
        &station_id,
        body.name.as_deref(),
        body.address.as_deref(),
        body.lat,
        body.lng,
        body.status.as_deref(),
        body.opening_hours.as_deref().map(Some),
    )
    .await?;

    Ok(HttpResponse::Ok().json(station))
}

pub async fn delete_station(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let station_id = path.into_inner();
    ev_db::queries::stations::soft_delete_station(&state.platform_db, &station_id).await?;
    Ok(HttpResponse::NoContent().finish())
}
