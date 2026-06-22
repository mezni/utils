use actix_web::{web, HttpResponse};
use domain_types::audit::AuditEvent;
use serde::Serialize;

#[derive(Serialize)]
pub struct EventResponse {
    pub status: String,
    pub event_id: String,
}

pub async fn handle_event(
    event: web::Json<AuditEvent>,
) -> HttpResponse {
    if event.event_type.is_empty() || event.idempotency_key.is_empty() {
        return HttpResponse::UnprocessableEntity().json(
            serde_json::json!({ "error": "Invalid event schema: event_type and idempotency_key are required" }),
        );
    }

    HttpResponse::Created().json(EventResponse {
        status: "accepted".to_string(),
        event_id: format!("EVT-{}", event.idempotency_key),
    })
}
