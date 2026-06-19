use actix_web::{web, HttpResponse, Result};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct CreatePartnerRequest {
    pub name: String,
    pub network_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub support_phone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub support_email: Option<String>,
}

pub async fn create_partner(
    _pool: web::Data<sqlx::PgPool>,
    _req: web::Json<CreatePartnerRequest>,
) -> Result<HttpResponse> {
    // TODO: Implement T024 - Create partner endpoint with transaction and audit
    Ok(HttpResponse::NotImplemented().json(serde_json::json!({
        "error": "Not implemented",
        "message": "T024: Implement create_partner endpoint (POST /api/v1/admin/partner) with transaction and audit"
    })))
}

pub async fn get_partner(
    _pool: web::Data<sqlx::PgPool>,
    _partner_id: web::Path<String>,
) -> Result<HttpResponse> {
    // TODO: Implement T026 - Implement get_partner endpoint (GET /api/v1/admin/partner/:id)
    Ok(HttpResponse::NotImplemented().json(serde_json::json!({
        "error": "Not implemented",
        "message": "T026: Implement get_partner endpoint (GET /api/v1/admin/partner/:id)"
    })))
}

pub async fn update_partner(
    _pool: web::Data<sqlx::PgPool>,
    _partner_id: web::Path<String>,
    _req: web::Json<crate::db_models::UpdatePartnerRequest>,
) -> Result<HttpResponse> {
    // TODO: Implement T025 - Implement update_partner endpoint (PUT /api/v1/admin/partner/:id) with BEFORE snapshot capture
    Ok(HttpResponse::NotImplemented().json(serde_json::json!({
        "error": "Not implemented",
        "message": "T025: Implement update_partner endpoint (PUT /api/v1/admin/partner/:id) with BEFORE snapshot capture"
    })))
}

pub async fn delete_partner_soft(
    _pool: web::Data<sqlx::PgPool>,
    _partner_id: web::Path<String>,
) -> Result<HttpResponse> {
    // TODO: Implement T027 - Implement delete_partner_soft endpoint (DELETE /api/v1/admin/partner/:id) with audit logging
    Ok(HttpResponse::NotImplemented().json(serde_json::json!({
        "error": "Not implemented",
        "message": "T027: Implement delete_partner_soft endpoint (DELETE /api/v1/admin/partner/:id) with audit logging"
    })))
}
