use crate::domain::chargers::models::{Charger, CreateChargerRequest, UpdateChargerRequest};
use crate::domain::chargers::repository;
use crate::utils::error::ProblemResponse;
use crate::utils::id_validator;
use crate::utils::pagination::Cursor;
use crate::utils::pagination::ListQuery;
use actix_web::{web, HttpResponse};
use serde::Serialize;
use sqlx::PgPool;

fn check_admin_or_partner(auth: &crate::auth::middleware::AuthUser) -> Result<(), HttpResponse> {
    if auth.0.role == "admin" || auth.0.role == "partner" {
        Ok(())
    } else {
        Err(ProblemResponse::forbidden("Only admins and partners can manage chargers"))
    }
}

async fn check_partner_owns_station(
    pool: &PgPool,
    auth: &crate::auth::middleware::AuthUser,
    station_id: &str,
) -> Result<(), HttpResponse> {
    if auth.0.role == "admin" {
        return Ok(());
    }
    match crate::domain::stations::repository::get_owner_id(pool, station_id).await {
        Ok(Some(owner_id)) if owner_id == auth.0.sub => Ok(()),
        Ok(Some(_)) => Err(ProblemResponse::forbidden("You can only manage chargers in your own stations")),
        Ok(None) => Err(ProblemResponse::not_found(format!("Station '{}' not found", station_id))),
        Err(_) => Err(ProblemResponse::internal_error()),
    }
}

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
    auth: crate::auth::middleware::AuthUser,
    path: web::Path<String>,
    body: web::Json<CreateChargerRequest>,
) -> HttpResponse {
    if let Err(resp) = check_admin_or_partner(&auth) {
        return resp;
    }

    let station_id = path.into_inner();
    let req = body.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&station_id, "STN") {
        return ProblemResponse::not_found(e);
    }

    if let Err(resp) = check_partner_owns_station(&pool, &auth, &station_id).await {
        return resp;
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

    let station = crate::domain::stations::repository::get_by_id(&pool, &station_id).await;
    match station {
        Ok(None) => return ProblemResponse::not_found(format!("Station '{}' not found", &station_id)),
        Err(_) => return ProblemResponse::internal_error(),
        _ => {}
    }

    let ct = crate::domain::connector_types::repository::get_by_id(&pool, &req.connector_type_id).await;
    match ct {
        Ok(None) => return ProblemResponse::not_found(format!("Connector type '{}' not found", &req.connector_type_id)),
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

    let station = crate::domain::stations::repository::get_by_id(&pool, &station_id).await;
    match station {
        Ok(None) => return ProblemResponse::not_found(format!("Station '{}' not found", &station_id)),
        Err(e) => {
            tracing::error!("Station check failed: {:?}", e);
            return ProblemResponse::internal_error();
        }
        _ => {}
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
        Err(e) => {
            tracing::error!("Failed to list chargers: {:?}", e);
            ProblemResponse::internal_error()
        }
    }
}

async fn check_charger_belongs_to_station(
    pool: &PgPool,
    station_id: &str,
    charger_id: &str,
) -> Result<Option<Charger>, HttpResponse> {
    let charger = repository::get_by_id(pool, charger_id).await.map_err(|_| ProblemResponse::internal_error())?;
    match charger {
        Some(c) if c.station_id == station_id => Ok(Some(c)),
        Some(_) => Ok(None),
        None => Ok(None),
    }
}

pub async fn get_charger(
    pool: web::Data<PgPool>,
    path: web::Path<(String, String)>,
) -> HttpResponse {
    let (station_id, id) = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&station_id, "STN") {
        return ProblemResponse::not_found(e);
    }
    if let Err(e) = id_validator::validate_id_prefix(&id, "CHG") {
        return ProblemResponse::not_found(e);
    }

    match check_charger_belongs_to_station(&pool, &station_id, &id).await {
        Ok(Some(charger)) => HttpResponse::Ok().json(ChargerResponse::from(charger)),
        Ok(None) => ProblemResponse::not_found(format!("Charger '{}' not found in station '{}'", &id, &station_id)),
        Err(resp) => resp,
    }
}

pub async fn update_charger(
    pool: web::Data<PgPool>,
    auth: crate::auth::middleware::AuthUser,
    path: web::Path<(String, String)>,
    body: web::Json<UpdateChargerRequest>,
) -> HttpResponse {
    if let Err(resp) = check_admin_or_partner(&auth) {
        return resp;
    }

    let (station_id, id) = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&station_id, "STN") {
        return ProblemResponse::not_found(e);
    }
    if let Err(e) = id_validator::validate_id_prefix(&id, "CHG") {
        return ProblemResponse::not_found(e);
    }

    if let Err(resp) = check_partner_owns_station(&pool, &auth, &station_id).await {
        return resp;
    }

    let body = body.into_inner();

    if let Some(power) = body.power_kw {
        if power <= 0.0 || power > 1000.0 {
            return ProblemResponse::validation("power_kw must be between 0 and 1000");
        }
    }

    match check_charger_belongs_to_station(&pool, &station_id, &id).await {
        Ok(Some(_)) => {}
        Ok(None) => return ProblemResponse::not_found(format!("Charger '{}' not found in station '{}'", &id, &station_id)),
        Err(resp) => return resp,
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
    auth: crate::auth::middleware::AuthUser,
    path: web::Path<(String, String)>,
) -> HttpResponse {
    if let Err(resp) = check_admin_or_partner(&auth) {
        return resp;
    }

    let (station_id, id) = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&station_id, "STN") {
        return ProblemResponse::not_found(e);
    }
    if let Err(e) = id_validator::validate_id_prefix(&id, "CHG") {
        return ProblemResponse::not_found(e);
    }

    if let Err(resp) = check_partner_owns_station(&pool, &auth, &station_id).await {
        return resp;
    }

    match check_charger_belongs_to_station(&pool, &station_id, &id).await {
        Ok(Some(_)) => {}
        Ok(None) => return ProblemResponse::not_found(format!("Charger '{}' not found in station '{}'", &id, &station_id)),
        Err(resp) => return resp,
    }

    match repository::permanently_delete(&pool, &id).await {
        Ok(true) => HttpResponse::NoContent().finish(),
        Ok(false) => ProblemResponse::not_found(format!("Charger '{}' not found", &id)),
        Err(_) => ProblemResponse::internal_error(),
    }
}
