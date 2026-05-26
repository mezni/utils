use crate::domain::chargers::models::{Charger, CreateChargerRequest, UpdateChargerRequest};
use crate::domain::chargers::repository;
use crate::utils::error::ProblemResponse;
use crate::utils::id_validator;
use crate::utils::pagination::Cursor;
use crate::utils::pagination::ListQuery;
use actix_web::{web, HttpResponse};
use serde::Serialize;
use sqlx::PgPool;

#[derive(Serialize)]
pub struct ChargerResponse {
    pub id: String,
    pub station_id: String,
    pub connector_type_id: String,
    pub power_kw: f64,
    pub current_type: String,
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<Charger> for ChargerResponse {
    fn from(c: Charger) -> Self {
        Self {
            id: c.id,
            station_id: c.station_id,
            connector_type_id: c.connector_type_id,
            power_kw: c.power_kw,
            current_type: c.current_type,
            status: c.status,
            created_at: c.created_at,
            updated_at: c.updated_at,
        }
    }
}

pub async fn create_charger(
    pool: web::Data<PgPool>,
    _auth: crate::auth::middleware::AuthUser,
    path: web::Path<String>,
    body: web::Json<CreateChargerRequest>,
) -> HttpResponse {
    let station_id = path.into_inner();
    let req = body.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&station_id, "STN") {
        return ProblemResponse::not_found(e);
    }

    if req.power_kw <= 0.0 || req.power_kw > 1000.0 {
        return ProblemResponse::validation("power_kw must be between 0 and 1000");
    }
    if req.current_type.is_empty() || req.current_type.len() > 20 {
        return ProblemResponse::validation("current_type must be between 1 and 20 characters");
    }
    if let Err(e) = id_validator::validate_id_prefix(&req.connector_type_id, "CNT") {
        return ProblemResponse::validation(format!("Invalid connector_type_id: {}", e));
    }

    let station_exists = crate::domain::stations::repository::get_by_id(&pool, &station_id).await;
    match station_exists {
        Ok(None) => return ProblemResponse::not_found(format!("Station '{}' not found", &station_id)),
        Err(_) => return ProblemResponse::internal_error(),
        _ => {}
    }

    let id = crate::utils::id_generator::generate_id("CHG");

    match repository::create(&pool, &id, &station_id, &req).await {
        Ok(charger) => HttpResponse::Created().json(ChargerResponse::from(charger)),
        Err(_) => ProblemResponse::internal_error(),
    }
}

pub async fn list_chargers_for_station(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
    query: web::Query<ListQuery>,
) -> HttpResponse {
    let station_id = path.into_inner();
    let q = query.into_inner();
    let limit = q.limit();

    if let Err(e) = id_validator::validate_id_prefix(&station_id, "STN") {
        return ProblemResponse::not_found(e);
    }

    let cursor = match q.cursor.as_ref() {
        Some(c) => match Cursor::decode(c) {
            Ok(c) => Some(c),
            Err(_) => return ProblemResponse::validation("Invalid cursor format"),
        },
        None => None,
    };

    match repository::list_by_station(&pool, &station_id, cursor, limit).await {
        Ok((chargers, next_cursor, has_more)) => {
            let data: Vec<ChargerResponse> = chargers.into_iter().map(ChargerResponse::from).collect();
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

pub async fn get_charger(
    pool: web::Data<PgPool>,
    path: web::Path<String>,
) -> HttpResponse {
    let id = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&id, "CHG") {
        return ProblemResponse::not_found(e);
    }

    match repository::get_by_id(&pool, &id).await {
        Ok(Some(charger)) => HttpResponse::Ok().json(ChargerResponse::from(charger)),
        Ok(None) => ProblemResponse::not_found(format!("Charger '{}' not found", &id)),
        Err(_) => ProblemResponse::internal_error(),
    }
}

pub async fn update_charger(
    pool: web::Data<PgPool>,
    _auth: crate::auth::middleware::AuthUser,
    path: web::Path<String>,
    body: web::Json<UpdateChargerRequest>,
) -> HttpResponse {
    let id = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&id, "CHG") {
        return ProblemResponse::not_found(e);
    }

    let body = body.into_inner();

    if let Some(power) = body.power_kw {
        if power <= 0.0 || power > 1000.0 {
            return ProblemResponse::validation("power_kw must be between 0 and 1000");
        }
    }

    match repository::update(&pool, &id, &body).await {
        Ok(Some(charger)) => HttpResponse::Ok().json(ChargerResponse::from(charger)),
        Ok(None) => {
            let exists = repository::get_by_id(&pool, &id).await.unwrap_or(None);
            if exists.is_some() {
                ProblemResponse::conflict("Concurrent modification detected — re-read and retry")
            } else {
                ProblemResponse::not_found(format!("Charger '{}' not found", &id))
            }
        }
        Err(_) => ProblemResponse::internal_error(),
    }
}

pub async fn delete_charger(
    pool: web::Data<PgPool>,
    _auth: crate::auth::middleware::AuthUser,
    path: web::Path<String>,
) -> HttpResponse {
    let id = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&id, "CHG") {
        return ProblemResponse::not_found(e);
    }

    match repository::permanently_delete(&pool, &id).await {
        Ok(true) => HttpResponse::NoContent().finish(),
        Ok(false) => ProblemResponse::not_found(format!("Charger '{}' not found", &id)),
        Err(_) => ProblemResponse::internal_error(),
    }
}
