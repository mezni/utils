use actix_web::{web, HttpResponse};
use ev_core::error::{AppError, FieldError};
use ev_core::event::{CreateBatchEventsRequest, CreateEventRequest};
use crate::AppState;

pub async fn ingest_event(
    state: web::Data<AppState>,
    body: web::Json<CreateEventRequest>,
) -> Result<HttpResponse, AppError> {
    let errors = ev_db::queries::events::validate_event(&body, None);
    if !errors.is_empty() {
        return Err(AppError::Validation { details: errors });
    }

    let result = ev_db::queries::events::insert_single_event(&state.analytics_db, &body).await?;
    Ok(HttpResponse::Created().json(result))
}

pub async fn ingest_batch(
    state: web::Data<AppState>,
    body: web::Json<CreateBatchEventsRequest>,
) -> Result<HttpResponse, AppError> {
    if body.events.len() > 100 {
        return Err(AppError::BadRequest(
            "Batch size exceeds maximum of 100 events".into(),
        ));
    }

    if body.events.is_empty() {
        return Err(AppError::Validation {
            details: vec![FieldError {
                field: "events".into(),
                message: "At least one event is required".into(),
            }],
        });
    }

    let all_errors: Vec<FieldError> = body
        .events
        .iter()
        .enumerate()
        .flat_map(|(i, e)| {
            ev_db::queries::events::validate_event(e, Some(i))
        })
        .collect();

    if !all_errors.is_empty() {
        return Err(AppError::Validation {
            details: all_errors,
        });
    }

    let result = ev_db::queries::events::insert_batch_events(&state.analytics_db, &body.events).await?;
    Ok(HttpResponse::Created().json(result))
}
