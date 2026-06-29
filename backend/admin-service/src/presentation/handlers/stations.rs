use actix_web::{web, HttpResponse};
use serde::Deserialize;

use crate::application::stations::create_station::{CreateStationInput, CreateStationUseCase};
use crate::application::stations::delete_station::DeleteStationUseCase;
use crate::application::stations::list_stations::ListStationsUseCase;
use crate::application::stations::update_station::{UpdateStationInput, UpdateStationUseCase};
use crate::domain::repositories::partner_repo::PartnerRepository;
use crate::domain::repositories::station_repo::StationRepository;
use crate::shared::errors::ApiResponse;

#[derive(Deserialize)]
pub struct CreateStationRequest {
    pub partner_id: String,
    pub name: String,
    pub address: String,
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Deserialize)]
pub struct UpdateStationRequest {
    pub name: String,
    pub address: String,
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Deserialize)]
pub struct ListStationsQuery {
    pub partner_id: Option<String>,
}

pub async fn create_station<R: StationRepository + Clone + 'static, P: PartnerRepository + Clone + 'static>(
    station_repo: web::Data<R>,
    partner_repo: web::Data<P>,
    body: web::Json<CreateStationRequest>,
) -> HttpResponse {
    let use_case =
        CreateStationUseCase::new((**station_repo).clone(), (**partner_repo).clone());
    match use_case
        .execute(CreateStationInput {
            partner_id: body.partner_id.clone(),
            name: body.name.clone(),
            address: body.address.clone(),
            latitude: body.latitude,
            longitude: body.longitude,
        })
        .await
    {
        Ok(station) => ApiResponse::created(station),
        Err(msg) => ApiResponse::bad_request(msg),
    }
}

pub async fn list_stations<R: StationRepository + Clone + 'static>(
    station_repo: web::Data<R>,
    query: web::Query<ListStationsQuery>,
) -> HttpResponse {
    let use_case = ListStationsUseCase::new((**station_repo).clone());
    match use_case
        .execute(query.partner_id.as_deref())
        .await
    {
        Ok(stations) => ApiResponse::success(stations),
        Err(msg) => ApiResponse::internal_error(msg),
    }
}

pub async fn update_station<R: StationRepository + Clone + 'static>(
    station_repo: web::Data<R>,
    path: web::Path<String>,
    body: web::Json<UpdateStationRequest>,
) -> HttpResponse {
    let use_case = UpdateStationUseCase::new((**station_repo).clone());
    match use_case
        .execute(UpdateStationInput {
            id: path.into_inner(),
            name: body.name.clone(),
            address: body.address.clone(),
            latitude: body.latitude,
            longitude: body.longitude,
        })
        .await
    {
        Ok(station) => ApiResponse::success(station),
        Err(msg) => {
            if msg.contains("not found") {
                ApiResponse::not_found(msg)
            } else {
                ApiResponse::bad_request(msg)
            }
        }
    }
}

pub async fn delete_station<R: StationRepository + Clone + 'static>(
    station_repo: web::Data<R>,
    path: web::Path<String>,
) -> HttpResponse {
    let use_case = DeleteStationUseCase::new((**station_repo).clone());
    match use_case.execute(&path.into_inner()).await {
        Ok(_) => HttpResponse::NoContent().finish(),
        Err(msg) => {
            if msg.contains("not found") {
                ApiResponse::not_found(msg)
            } else {
                ApiResponse::internal_error(msg)
            }
        }
    }
}
