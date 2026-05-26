use crate::domain::partners::models::{CreatePartnerRequest, PartnerProfile, UpdatePartnerRequest};
use crate::domain::partners::repository;
use crate::utils::error::ProblemResponse;
use crate::utils::id_validator;
use crate::utils::pagination::Cursor;
use crate::utils::pagination::ListQuery;
use actix_web::{web, HttpResponse};
use serde::Serialize;
use sqlx::PgPool;

#[derive(Serialize)]
pub struct PartnerResponse {
    pub id: String,
    pub user_id: String,
    pub classification: String,
    pub display_name: String,
    pub tax_id: Option<String>,
    pub contact_phone: Option<String>,
    pub is_test: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<PartnerProfile> for PartnerResponse {
    fn from(p: PartnerProfile) -> Self {
        Self {
            id: p.id,
            user_id: p.user_id,
            classification: p.classification,
            display_name: p.display_name,
            tax_id: p.tax_id,
            contact_phone: p.contact_phone,
            is_test: p.is_test,
            created_at: p.created_at,
            updated_at: p.updated_at,
        }
    }
}

pub async fn create_partner(
    pool: web::Data<PgPool>,
    body: web::Json<CreatePartnerRequest>,
) -> HttpResponse {
    let req = body.into_inner();

    if !["business", "private"].contains(&req.classification.as_str()) {
        return ProblemResponse::validation("Classification must be business or private");
    }
    if req.display_name.len() < 2 || req.display_name.len() > 100 {
        return ProblemResponse::validation("Display name must be between 2 and 100 characters");
    }

    match repository::exists_by_user_id(&pool, &req.user_id).await {
        Ok(true) => return ProblemResponse::conflict("User already has a partner profile"),
        Err(_) => return ProblemResponse::internal_error(),
        _ => {}
    }

    let id = crate::utils::id_generator::generate_id("PRT");

    match repository::create(&pool, &id, &req, false).await {
        Ok(profile) => HttpResponse::Created().json(PartnerResponse::from(profile)),
        Err(e) => {
            tracing::error!("Failed to create partner profile: {:?}", e);
            ProblemResponse::internal_error()
        }
    }
}

pub async fn list_partners(
    pool: web::Data<PgPool>,
    query: web::Query<ListQuery>,
) -> HttpResponse {
    let q = query.into_inner();
    let limit = q.limit();

    let cursor = match q.cursor.as_ref() {
        Some(c) => match Cursor::decode(c) {
            Ok(c) => Some(c),
            Err(_) => return ProblemResponse::validation("Invalid cursor format"),
        },
        None => None,
    };

    match repository::list(&pool, cursor, limit, q.include_test()).await {
        Ok((profiles, next_cursor, has_more)) => {
            let data: Vec<PartnerResponse> = profiles.into_iter().map(PartnerResponse::from).collect();
            HttpResponse::Ok().json(serde_json::json!({
                "data": data,
                "pagination": {
                    "next_cursor": next_cursor,
                    "has_more": has_more
                }
            }))
        }
        Err(_) => ProblemResponse::internal_error(),
    }
}

pub async fn get_partner(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
) -> HttpResponse {
    let id = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&id, "PRT") {
        return ProblemResponse::not_found(e);
    }

    match repository::get_by_id(&pool, &id).await {
        Ok(Some(profile)) => HttpResponse::Ok().json(PartnerResponse::from(profile)),
        Ok(None) => ProblemResponse::not_found(format!("Partner profile '{}' not found", &id)),
        Err(_) => ProblemResponse::internal_error(),
    }
}

pub async fn update_partner(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
    body: web::Json<UpdatePartnerRequest>,
) -> HttpResponse {
    let id = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&id, "PRT") {
        return ProblemResponse::not_found(e);
    }

    match repository::update(&pool, &id, &body).await {
        Ok(Some(profile)) => HttpResponse::Ok().json(PartnerResponse::from(profile)),
        Ok(None) => {
            let exists = repository::get_by_id(&pool, &id).await.unwrap_or(None);
            if exists.is_some() {
                ProblemResponse::conflict("Concurrent modification detected — re-read and retry")
            } else {
                ProblemResponse::not_found(format!("Partner profile '{}' not found", &id))
            }
        }
        Err(_) => ProblemResponse::internal_error(),
    }
}

pub async fn delete_partner(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
) -> HttpResponse {
    let id = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&id, "PRT") {
        return ProblemResponse::not_found(e);
    }

    match repository::soft_delete(&pool, &id).await {
        Ok(Some(_)) => HttpResponse::NoContent().finish(),
        Ok(None) => ProblemResponse::not_found(format!("Partner profile '{}' not found", &id)),
        Err(_) => ProblemResponse::internal_error(),
    }
}
