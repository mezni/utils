use actix_web::{web, HttpResponse};
use serde::Deserialize;

use crate::application::connectors::create_connector::{CreateConnectorInput, CreateConnectorUseCase};
use crate::application::connectors::delete_connector::DeleteConnectorUseCase;
use crate::application::connectors::list_connectors::ListConnectorsUseCase;
use crate::domain::repositories::connector_repo::ConnectorRepository;
use crate::domain::repositories::station_repo::StationRepository;
use crate::shared::errors::ApiResponse;

#[derive(Deserialize)]
pub struct CreateConnectorRequest {
    pub station_id: String,
    pub connector_type: String,
    pub power_kw: f64,
}

#[derive(Deserialize)]
pub struct ListConnectorsQuery {
    pub station_id: String,
}

pub async fn create_connector<R: ConnectorRepository + 'static, S: StationRepository + 'static>(
    connector_repo: web::Data<R>,
    station_repo: web::Data<S>,
    body: web::Json<CreateConnectorRequest>,
) -> HttpResponse {
    let use_case = CreateConnectorUseCase::new(
        connector_repo.get_ref().clone(),
        station_repo.get_ref().clone(),
    );
    match use_case
        .execute(CreateConnectorInput {
            station_id: body.station_id.clone(),
            connector_type: body.connector_type.clone(),
            power_kw: body.power_kw,
        })
        .await
    {
        Ok(connector) => ApiResponse::created(connector),
        Err(msg) => ApiResponse::bad_request(&msg),
    }
}

pub async fn list_connectors<R: ConnectorRepository + 'static>(
    connector_repo: web::Data<R>,
    query: web::Query<ListConnectorsQuery>,
) -> HttpResponse {
    let use_case = ListConnectorsUseCase::new(connector_repo.get_ref().clone());
    match use_case.execute(&query.station_id).await {
        Ok(connectors) => ApiResponse::success(connectors),
        Err(msg) => ApiResponse::internal_error(&msg),
    }
}

pub async fn delete_connector<R: ConnectorRepository + 'static>(
    connector_repo: web::Data<R>,
    path: web::Path<String>,
) -> HttpResponse {
    let use_case = DeleteConnectorUseCase::new(connector_repo.get_ref().clone());
    match use_case.execute(&path.into_inner()).await {
        Ok(_) => HttpResponse::NoContent().finish(),
        Err(msg) => {
            if msg.contains("not found") {
                ApiResponse::not_found(&msg)
            } else {
                ApiResponse::internal_error(&msg)
            }
        }
    }
}
