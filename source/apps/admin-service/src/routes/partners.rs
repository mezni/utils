use crate::config;
use crate::db;
use crate::error::AppError;
use crate::models::{CreatePartnerRequest, PaginationParams, UpdatePartnerRequest};
use crate::AppState;
use actix_web::{delete, get, post, put, web, HttpRequest, HttpResponse};

#[post("/api/partners")]
pub async fn create_partner(
    req: HttpRequest,
    state: web::Data<AppState>,
    body: web::Json<CreatePartnerRequest>,
) -> Result<HttpResponse, AppError> {
    let actor = config::x_partner_id(&req);
    validate_partner_type(&body.partner_type)?;
    if body.name.trim().is_empty() || body.name.len() > 255 {
        return Err(AppError::ValidationError(
            "name must be 1-255 characters".to_string(),
        ));
    }
    let partner = db::partners::create_partner(&state.pool, body.into_inner(), &actor).await?;
    Ok(HttpResponse::Created().json(partner))
}

#[get("/api/partners")]
pub async fn list_partners(
    state: web::Data<AppState>,
    query: web::Query<PaginationParams>,
) -> Result<HttpResponse, AppError> {
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(20).min(100).max(1);
    let result = db::partners::list_partners(&state.pool, page, page_size).await?;
    Ok(HttpResponse::Ok().json(result))
}

#[get("/api/partners/{id}")]
pub async fn get_partner(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = path.into_inner();
    let partner = db::partners::get_partner(&state.pool, &id).await?;
    Ok(HttpResponse::Ok().json(partner))
}

#[put("/api/partners/{id}")]
pub async fn update_partner(
    req: HttpRequest,
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<UpdatePartnerRequest>,
) -> Result<HttpResponse, AppError> {
    let actor = config::x_partner_id(&req);
    let id = path.into_inner();
    if let Some(ref name) = body.name {
        if name.trim().is_empty() || name.len() > 255 {
            return Err(AppError::ValidationError(
                "name must be 1-255 characters".to_string(),
            ));
        }
    }
    if let Some(ref t) = body.partner_type {
        validate_partner_type(t)?;
    }
    let partner = db::partners::update_partner(&state.pool, &id, body.into_inner(), &actor).await?;
    Ok(HttpResponse::Ok().json(partner))
}

#[delete("/api/partners/{id}")]
pub async fn delete_partner(
    req: HttpRequest,
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let actor = config::x_partner_id(&req);
    let id = path.into_inner();
    db::partners::delete_partner(&state.pool, &id, &actor).await?;
    Ok(HttpResponse::Ok().json(serde_json::json!({"deleted": true})))
}

fn validate_partner_type(t: &str) -> Result<(), AppError> {
    match t {
        "business" | "personal" => Ok(()),
        _ => Err(AppError::ValidationError(format!(
            "invalid partner type '{}', must be 'business' or 'personal'",
            t
        ))),
    }
}
