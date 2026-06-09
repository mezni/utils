use crate::config;
use crate::db;
use crate::error::AppError;
use crate::models::{ChargerListParams, CreateChargerRequest, UpdateChargerRequest};
use crate::AppState;
use actix_web::{delete, get, post, put, web, HttpRequest, HttpResponse};

const VALID_CONNECTOR_TYPES: &[&str] = &["type2", "type3", "ccs", "chademo"];
const VALID_CHARGER_STATUSES: &[&str] = &["available", "in_use", "maintenance", "offline"];

#[post("/api/chargers")]
pub async fn create_charger(
    req: HttpRequest,
    state: web::Data<AppState>,
    body: web::Json<CreateChargerRequest>,
) -> Result<HttpResponse, AppError> {
    let actor = config::x_partner_id(&req);
    if !VALID_CONNECTOR_TYPES.contains(&body.connector_type.as_str()) {
        return Err(AppError::ValidationError(format!(
            "invalid connector_type '{}', must be one of {:?}",
            body.connector_type, VALID_CONNECTOR_TYPES
        )));
    }
    if body.power_kw <= 0.0 {
        return Err(AppError::ValidationError(
            "power_kw must be greater than 0".to_string(),
        ));
    }
    if let Some(ref status) = body.status {
        if !VALID_CHARGER_STATUSES.contains(&status.as_str()) {
            return Err(AppError::ValidationError(format!(
                "invalid status '{}', must be one of {:?}",
                status, VALID_CHARGER_STATUSES
            )));
        }
    }
    let charger = db::chargers::create_charger(&state.pool, body.into_inner(), &actor).await?;
    Ok(HttpResponse::Created().json(charger))
}

#[get("/api/chargers")]
pub async fn list_chargers(
    state: web::Data<AppState>,
    query: web::Query<ChargerListParams>,
) -> Result<HttpResponse, AppError> {
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(20).min(100).max(1);
    let station_id = query.station_id.as_deref();
    let result = db::chargers::list_chargers(&state.pool, station_id, page, page_size).await?;
    Ok(HttpResponse::Ok().json(result))
}

#[get("/api/chargers/{id}")]
pub async fn get_charger(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let charger = db::chargers::get_charger(&state.pool, &id).await?;
    Ok(HttpResponse::Ok().json(charger))
}

#[put("/api/chargers/{id}")]
pub async fn update_charger(
    req: HttpRequest,
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<UpdateChargerRequest>,
) -> Result<HttpResponse, AppError> {
    let actor = config::x_partner_id(&req);
    let id = path.into_inner();
    if let Some(ref ct) = body.connector_type {
        if !VALID_CONNECTOR_TYPES.contains(&ct.as_str()) {
            return Err(AppError::ValidationError(format!(
                "invalid connector_type '{}', must be one of {:?}",
                ct, VALID_CONNECTOR_TYPES
            )));
        }
    }
    if let Some(pk) = body.power_kw {
        if pk <= 0.0 {
            return Err(AppError::ValidationError(
                "power_kw must be greater than 0".to_string(),
            ));
        }
    }
    if let Some(ref status) = body.status {
        if !VALID_CHARGER_STATUSES.contains(&status.as_str()) {
            return Err(AppError::ValidationError(format!(
                "invalid status '{}', must be one of {:?}",
                status, VALID_CHARGER_STATUSES
            )));
        }
    }
    let charger = db::chargers::update_charger(&state.pool, &id, body.into_inner(), &actor).await?;
    Ok(HttpResponse::Ok().json(charger))
}

#[delete("/api/chargers/{id}")]
pub async fn delete_charger(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    db::chargers::delete_charger(&state.pool, &id).await?;
    Ok(HttpResponse::Ok().json(serde_json::json!({"deleted": true})))
}
