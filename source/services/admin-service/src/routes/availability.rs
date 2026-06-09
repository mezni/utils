use crate::config;
use crate::db;
use crate::error::AppError;
use crate::models::CreateAvailabilityRequest;
use crate::AppState;
use actix_web::{post, web, HttpRequest, HttpResponse};

const VALID_AVAILABILITY_STATUSES: &[&str] = &["available", "partial", "unavailable"];

#[post("/api/stations/{id}/availability")]
pub async fn create_availability(
    req: HttpRequest,
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<CreateAvailabilityRequest>,
) -> Result<HttpResponse, AppError> {
    let actor = config::x_partner_id(&req);
    let station_id = path.into_inner();
    if !VALID_AVAILABILITY_STATUSES.contains(&body.status.as_str()) {
        return Err(AppError::ValidationError(format!(
            "invalid status '{}', must be one of {:?}",
            body.status, VALID_AVAILABILITY_STATUSES
        )));
    }
    let record =
        db::availability::create_availability(&state.pool, &station_id, body.into_inner(), &actor)
            .await?;
    Ok(HttpResponse::Created().json(record))
}
