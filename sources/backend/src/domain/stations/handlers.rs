use crate::auth::jwt::{validate_token, jwt_secret};
use crate::domain::stations::models::{CreateStationRequest, Station, UpdateStationRequest};
use crate::domain::stations::repository;
use crate::utils::error::ProblemResponse;
use crate::utils::id_validator;
use crate::utils::pagination::Cursor;
use crate::utils::pagination::ListQuery;
use actix_web::{web, HttpRequest, HttpResponse};
use serde::Serialize;
use sqlx::PgPool;

pub fn try_extract_auth_user(req: &HttpRequest) -> Option<crate::auth::middleware::AuthUser> {
    let auth_header = req.headers().get("Authorization")?.to_str().ok()?;
    let token = auth_header.strip_prefix("Bearer ")?;
    validate_token(token, &jwt_secret())
        .ok()
        .map(crate::auth::middleware::AuthUser)
}

fn check_admin_or_partner(auth: &crate::auth::middleware::AuthUser) -> Result<(), HttpResponse> {
    if auth.0.role == "admin" || auth.0.role == "partner" {
        Ok(())
    } else {
        Err(ProblemResponse::forbidden("Only admins and partners can manage stations"))
    }
}

#[derive(Serialize)]
pub struct StationResponse {
    pub id: String,
    pub owner_id: String,
    pub name: String,
    pub address: String,
    pub city: String,
    pub longitude: f64,
    pub latitude: f64,
    pub is_operational: bool,
    pub is_test: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<Station> for StationResponse {
    fn from(s: Station) -> Self {
        Self {
            id: s.id,
            owner_id: s.owner_id,
            name: s.name,
            address: s.address,
            city: s.city,
            longitude: s.longitude,
            latitude: s.latitude,
            is_operational: s.is_operational,
            is_test: s.is_test,
            created_at: s.created_at,
            updated_at: s.updated_at,
        }
    }
}

pub async fn create_station(
    pool: web::Data<PgPool>,
    auth: crate::auth::middleware::AuthUser,
    body: web::Json<CreateStationRequest>,
) -> HttpResponse {
    if let Err(resp) = check_admin_or_partner(&auth) {
        return resp;
    }

    let mut req = body.into_inner();

    if auth.0.role == "partner" {
        req.owner_id = auth.0.sub.clone();
    }

    if req.longitude < -180.0 || req.longitude > 180.0 {
        return ProblemResponse::validation("Longitude must be between -180 and 180");
    }
    if req.latitude < -90.0 || req.latitude > 90.0 {
        return ProblemResponse::validation("Latitude must be between -90 and 90");
    }
    if req.name.len() < 2 || req.name.len() > 150 {
        return ProblemResponse::validation("Name must be between 2 and 150 characters");
    }
    if req.address.len() < 2 || req.address.len() > 250 {
        return ProblemResponse::validation("Address must be between 2 and 250 characters");
    }
    if req.city.len() < 2 || req.city.len() > 100 {
        return ProblemResponse::validation("City must be between 2 and 100 characters");
    }

    if let Err(e) = id_validator::validate_id_prefix(&req.owner_id, "USR") {
        return ProblemResponse::validation(format!("Invalid owner_id: {}", e));
    }

    match crate::domain::users::repository::get_by_id(&pool, &req.owner_id).await {
        Ok(Some(user)) => {
            if user.role != "partner" && user.role != "admin" {
                return ProblemResponse::validation("Station owner must have role 'partner' or 'admin'");
            }
        }
        Ok(None) => return ProblemResponse::not_found(format!("Owner user '{}' not found", &req.owner_id)),
        Err(_) => return ProblemResponse::internal_error(),
    }

    let id = crate::utils::id_generator::generate_id("STN");

    match repository::create(&pool, &id, &req, false).await {
        Ok(station) => HttpResponse::Created().json(StationResponse::from(station)),
        Err(e) => {
            tracing::error!("Failed to create station: {:?}", e);
            ProblemResponse::internal_error()
        }
    }
}

pub async fn list_stations(
    pool: web::Data<PgPool>,
    req: HttpRequest,
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

    let owner_filter = try_extract_auth_user(&req)
        .filter(|a| a.0.role == "partner")
        .map(|a| a.0.sub);

    match repository::list(&pool, cursor, limit, q.include_test(), owner_filter.as_deref()).await {
        Ok((stations, next_cursor, has_more)) => {
            let data: Vec<StationResponse> = stations.into_iter().map(StationResponse::from).collect();
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

pub async fn get_station(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
) -> HttpResponse {
    let id = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&id, "STN") {
        return ProblemResponse::not_found(e);
    }

    match repository::get_by_id(&pool, &id).await {
        Ok(Some(station)) => HttpResponse::Ok().json(StationResponse::from(station)),
        Ok(None) => ProblemResponse::not_found(format!("Station '{}' not found", &id)),
        Err(_) => ProblemResponse::internal_error(),
    }
}

pub async fn update_station(
    pool: web::Data<PgPool>,
    auth: crate::auth::middleware::AuthUser,
    path: web::Path<String>,
    body: web::Json<UpdateStationRequest>,
) -> HttpResponse {
    if let Err(resp) = check_admin_or_partner(&auth) {
        return resp;
    }

    let id = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&id, "STN") {
        return ProblemResponse::not_found(e);
    }

    if auth.0.role == "partner" {
        match repository::get_owner_id(&pool, &id).await {
            Ok(Some(owner_id)) if owner_id == auth.0.sub => {}
            Ok(Some(_)) => return ProblemResponse::forbidden("You can only update your own stations"),
            Ok(None) => return ProblemResponse::not_found(format!("Station '{}' not found", &id)),
            Err(_) => return ProblemResponse::internal_error(),
        }
    }

    if let Some(lng) = body.longitude {
        if !(-180.0..=180.0).contains(&lng) {
            return ProblemResponse::validation("Longitude must be between -180 and 180");
        }
    }
    if let Some(lat) = body.latitude {
        if !(-90.0..=90.0).contains(&lat) {
            return ProblemResponse::validation("Latitude must be between -90 and 90");
        }
    }

    match repository::update(&pool, &id, &body).await {
        Ok(Some(station)) => HttpResponse::Ok().json(StationResponse::from(station)),
        Ok(None) => {
            let exists = repository::get_by_id(&pool, &id).await.unwrap_or(None);
            if exists.is_some() {
                ProblemResponse::conflict("Concurrent modification detected — re-read and retry")
            } else {
                ProblemResponse::not_found(format!("Station '{}' not found", &id))
            }
        }
        Err(_) => ProblemResponse::internal_error(),
    }
}

pub async fn delete_station(
    pool: web::Data<PgPool>,
    auth: crate::auth::middleware::AuthUser,
    path: web::Path<String>,
) -> HttpResponse {
    if let Err(resp) = check_admin_or_partner(&auth) {
        return resp;
    }

    let id = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&id, "STN") {
        return ProblemResponse::not_found(e);
    }

    if auth.0.role == "partner" {
        match repository::get_owner_id(&pool, &id).await {
            Ok(Some(owner_id)) if owner_id == auth.0.sub => {}
            Ok(Some(_)) => return ProblemResponse::forbidden("You can only delete your own stations"),
            Ok(None) => return ProblemResponse::not_found(format!("Station '{}' not found", &id)),
            Err(_) => return ProblemResponse::internal_error(),
        }
    }

    match repository::soft_delete(&pool, &id).await {
        Ok(Some(_)) => HttpResponse::NoContent().finish(),
        Ok(None) => ProblemResponse::not_found(format!("Station '{}' not found", &id)),
        Err(_) => ProblemResponse::internal_error(),
    }
}
