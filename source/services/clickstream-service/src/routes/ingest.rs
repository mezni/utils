use actix_web::{web, HttpRequest, HttpResponse, ResponseError};
use nanoid::nanoid;
use tracing::{info, warn};

use crate::db::repository::AnalyticsDbRepo;
use crate::errors::AppError;
use crate::models::event::Event;
use crate::response::ApiResponse;

pub async fn ingest_event(
    repo: web::Data<AnalyticsDbRepo>,
    req: HttpRequest,
    body: web::Json<Event>,
) -> HttpResponse {
    let event = body.into_inner();

    if let Err(e) = event.validate() {
        warn!("Event validation failed: {}", e);
        return HttpResponse::build(e.status_code()).json(ApiResponse::<()>::error(e));
    }

    let batch_id = nanoid!();
    let ip = req.peer_addr().map(|a| a.to_string());

    let repo_clone = repo.get_ref().clone();
    let event_clone = event.clone();
    let bid = batch_id.clone();

    tokio::spawn(async move {
        if let Err(e) = repo_clone.insert_event(&event_clone, &bid, ip.as_deref()).await {
            warn!(error = %e, "Fire-and-forget insert failed");
        } else {
            info!(
                event_name = %event_clone.event_name,
                batch_id = %bid,
                result = "accepted",
                "Event ingested"
            );
        }
    });

    HttpResponse::Accepted().json(ApiResponse::success(serde_json::json!({
        "batch_id": batch_id
    })))
}

pub async fn ingest_batch(
    repo: web::Data<AnalyticsDbRepo>,
    req: HttpRequest,
    body: web::Json<Vec<Event>>,
) -> HttpResponse {
    let events = body.into_inner();

    if events.is_empty() || events.len() > 100 {
        return HttpResponse::build(actix_web::http::StatusCode::UNPROCESSABLE_ENTITY)
            .json(ApiResponse::<()>::error(AppError::batch_size_exceeded()));
    }

    let total_size = serde_json::to_vec(&events)
        .map(|v| v.len())
        .unwrap_or(0);
    if total_size > 512 * 1024 {
        return HttpResponse::build(actix_web::http::StatusCode::PAYLOAD_TOO_LARGE)
            .json(ApiResponse::<()>::error(AppError::batch_too_large()));
    }

    let mut valid_events = Vec::new();
    let mut failed = Vec::new();

    for (i, event) in events.into_iter().enumerate() {
        match event.validate() {
            Ok(()) => valid_events.push(event),
            Err(e) => {
                warn!("Batch event {} validation failed: {}", i, e);
                failed.push(serde_json::json!({
                    "index": i,
                    "error": e.message,
                }));
            }
        }
    }

    let accepted = valid_events.len() as u64;
    let batch_id = nanoid!();
    let failed_for_response = failed.clone();
    let batch_id_for_spawn = batch_id.clone();
    let ip = req.peer_addr().map(|a| a.to_string());
    let repo_clone = repo.get_ref().clone();

    tokio::spawn(async move {
        for event in &valid_events {
            if let Err(e) = repo_clone
                .insert_event(event, &batch_id_for_spawn, ip.as_deref())
                .await
            {
                warn!(error = %e, "Fire-and-forget batch insert failed");
            }
        }
        info!(
            batch_id = %batch_id_for_spawn,
            accepted = accepted,
            failed_count = failed.len() as u64,
            result = "accepted",
            "Batch ingested"
        );
    });

    HttpResponse::Accepted().json(ApiResponse::success(serde_json::json!({
        "batch_id": batch_id,
        "accepted": accepted,
        "failed": failed_for_response,
    })))
}
