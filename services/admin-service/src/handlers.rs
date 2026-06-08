// Handlers module
use std::sync::Arc;
use actix_web::{web, HttpResponse};
use ev_db::PgPool;

use crate::{
    config::PostgresUrl,
    db::create_pool,
    error::{AppError, EntityNotFoundError},
    models::{
        HealthCheckRequest, HealthCheckResponse, PartnerRequest, PartnerResponse,
        PartnerListResponse, StationRequest, StationResponse, StationListResponse,
        ChargerRequest, ChargerResponse, ChargerListResponse,
    },
};

/// Health check handler
pub async fn health_check_handler(
    postgres_url: web::Data<Arc<PostgresUrl>>,
) -> Result<HttpResponse, AppError> {
    let pool = create_pool(postgres_url.as_ref())
        .await
        .map_err(|e| {
            tracing::error!("Database connection failed: {}", e);
            AppError::HealthCheckError(format!("Database error: {}", e))
        })?;

    sqlx::query("SELECT 1")
        .fetch_one(&pool)
        .await
        .map_err(|e| {
            tracing::error!("Database query failed during health check: {}", e);
            AppError::HealthCheckError(format!("Database error: {}", e))
        })?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "service": "admin-service",
        "db": "ok"
    })))
}

/// Partner CRUD handlers
pub async fn partner_create_handler(
    pool: web::Data<PgPool>,
    partner: web::Json<PartnerRequest>,
) -> Result<HttpResponse, AppError> {
    // Generate unique ID using ev-core NanoID
    let id = uuid::Uuid::new_v4().to_string();

    sqlx::query!(
        r#"
        INSERT INTO inventory.partner (id, name, email, phone, address, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
        "#,
        id,
        partner.name,
        partner.email,
        partner.phone,
        partner.address
    )
    .execute(pool.get_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to create partner: {}", e);
        AppError::DatabaseError(format!("Failed to create partner: {}", e))
    })?;

    Ok(HttpResponse::Created().json(serde_json::json!({
        "id": id,
        "name": partner.name,
        "email": partner.email,
        "phone": partner.phone,
        "address": partner.address,
    })))
}

pub async fn partner_get_handler(
    id: web::Path<String>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AppError> {
    let result = sqlx::query_as!(
        PartnerResponse,
        r#"
        SELECT id, name, email, phone, address
        FROM inventory.partner
        WHERE id = $1
        "#,
        id.to_string()
    )
    .fetch_optional(pool.get_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to get partner: {}", e);
        AppError::DatabaseError(format!("Failed to get partner: {}", e))
    })?;

    match result {
        Some(partner) => Ok(HttpResponse::Ok().json(partner)),
        None => Err(AppError::EntityNotFoundError(format!(
            "Partner with ID '{}' not found",
            id
        ))),
    }
}

pub async fn partner_list_handler(pool: web::Data<PgPool>) -> Result<HttpResponse, AppError> {
    let partners = sqlx::query_as!(
        PartnerResponse,
        r#"
        SELECT id, name, email, phone, address
        FROM inventory.partner
        ORDER BY name ASC
        "#
    )
    .fetch_all(pool.get_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to list partners: {}", e);
        AppError::DatabaseError(format!("Failed to list partners: {}", e))
    })?;

    Ok(HttpResponse::Ok().json(PartnerListResponse {
        partners,
        pagination: None,
    }))
}

pub async fn partner_update_handler(
    id: web::Path<String>,
    partner: web::Json<PartnerRequest>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AppError> {
    sqlx::query!(
        r#"
        UPDATE inventory.partner
        SET name = $1, email = $2, phone = $3, address = $4, updated_at = NOW()
        WHERE id = $5
        "#,
        partner.name,
        partner.email,
        partner.phone,
        partner.address,
        id.to_string()
    )
    .execute(pool.get_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to update partner: {}", e);
        AppError::DatabaseError(format!("Failed to update partner: {}", e))
    })?;

    partner_get_handler(id, pool).await
}

pub async fn partner_delete_handler(
    id: web::Path<String>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AppError> {
    let result = sqlx::query!("DELETE FROM inventory.partner WHERE id = $1 RETURNING id", id.to_string())
        .fetch_optional(pool.get_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to delete partner: {}", e);
            AppError::DatabaseError(format!("Failed to delete partner: {}", e))
        })?;

    match result {
        Some(_) => Ok(HttpResponse::NoContent().finish()),
        None => Err(AppError::EntityNotFoundError(format!(
            "Partner with ID '{}' not found",
            id
        ))),
    }
}

/// Station CRUD handlers
pub async fn station_create_handler(
    pool: web::Data<PgPool>,
    station: web::Json<StationRequest>,
) -> Result<HttpResponse, AppError> {
    // Generate unique ID
    let id = uuid::Uuid::new_v4().to_string();

    // Validate partner_id exists
    sqlx::query!("SELECT id FROM inventory.partner WHERE id = $1", station.partner_id.clone())
        .fetch_optional(pool.get_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to validate partner: {}", e);
            AppError::DatabaseError(format!("Failed to validate partner: {}", e))
        })?;

    sqlx::query!(
        r#"
        INSERT INTO inventory.station (id, partner_id, name, latitude, longitude, address, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
        "#,
        id,
        station.partner_id,
        station.name,
        station.latitude,
        station.longitude,
        station.address
    )
    .execute(pool.get_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to create station: {}", e);
        AppError::DatabaseError(format!("Failed to create station: {}", e))
    })?;

    Ok(HttpResponse::Created().json(serde_json::json!({
        "id": id,
        "partner_id": station.partner_id,
        "name": station.name,
        "latitude": station.latitude,
        "longitude": station.longitude,
        "address": station.address,
    })))
}

pub async fn station_get_handler(
    id: web::Path<String>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AppError> {
    let result = sqlx::query_as!(
        StationResponse,
        r#"
        SELECT id, partner_id, name, latitude, longitude, address
        FROM inventory.station
        WHERE id = $1
        "#,
        id.to_string()
    )
    .fetch_optional(pool.get_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to get station: {}", e);
        AppError::DatabaseError(format!("Failed to get station: {}", e))
    })?;

    match result {
        Some(station) => Ok(HttpResponse::Ok().json(station)),
        None => Err(AppError::EntityNotFoundError(format!(
            "Station with ID '{}' not found",
            id
        ))),
    }
}

pub async fn station_list_handler(pool: web::Data<PgPool>) -> Result<HttpResponse, AppError> {
    let stations = sqlx::query_as!(
        StationResponse,
        r#"
        SELECT id, partner_id, name, latitude, longitude, address
        FROM inventory.station
        ORDER BY name ASC
        "#
    )
    .fetch_all(pool.get_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to list stations: {}", e);
        AppError::DatabaseError(format!("Failed to list stations: {}", e))
    })?;

    Ok(HttpResponse::Ok().json(StationListResponse {
        stations,
        pagination: None,
    }))
}

pub async fn station_update_handler(
    id: web::Path<String>,
    station: web::Json<StationRequest>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AppError> {
    // Validate partner_id exists if being updated
    if let Some(ref partner_id) = station.partner_id.clone() {
        sqlx::query!("SELECT id FROM inventory.partner WHERE id = $1", partner_id.clone())
            .fetch_optional(pool.get_ref())
            .await
            .map_err(|e| {
                tracing::error!("Failed to validate partner: {}", e);
                AppError::DatabaseError(format!("Failed to validate partner: {}", e))
            })?;
    }

    sqlx::query!(
        r#"
        UPDATE inventory.station
        SET partner_id = COALESCE($1, partner_id), name = $2, latitude = $3, longitude = $4, address = $5, updated_at = NOW()
        WHERE id = $6
        "#,
        station.partner_id,
        station.name,
        station.latitude,
        station.longitude,
        station.address,
        id.to_string()
    )
    .execute(pool.get_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to update station: {}", e);
        AppError::DatabaseError(format!("Failed to update station: {}", e))
    })?;

    station_get_handler(id, pool).await
}

pub async fn station_delete_handler(
    id: web::Path<String>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AppError> {
    let result = sqlx::query!("DELETE FROM inventory.station WHERE id = $1 RETURNING id", id.to_string())
        .fetch_optional(pool.get_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to delete station: {}", e);
            AppError::DatabaseError(format!("Failed to delete station: {}", e))
        })?;

    match result {
        Some(_) => Ok(HttpResponse::NoContent().finish()),
        None => Err(AppError::EntityNotFoundError(format!(
            "Station with ID '{}' not found",
            id
        ))),
    }
}

/// Charger CRUD handlers
pub async fn charger_create_handler(
    pool: web::Data<PgPool>,
    charger: web::Json<ChargerRequest>,
) -> Result<HttpResponse, AppError> {
    // Generate unique ID
    let id = uuid::Uuid::new_v4().to_string();

    // Validate station_id exists
    sqlx::query!("SELECT id FROM inventory.station WHERE id = $1", charger.station_id.clone())
        .fetch_optional(pool.get_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to validate station: {}", e);
            AppError::DatabaseError(format!("Failed to validate station: {}", e))
        })?;

    sqlx::query!(
        r#"
        INSERT INTO inventory.charger (id, station_id, connector_type, power_kw, status, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
        "#,
        id,
        charger.station_id,
        charger.connector_type,
        charger.power_kw,
        charger.status
    )
    .execute(pool.get_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to create charger: {}", e);
        AppError::DatabaseError(format!("Failed to create charger: {}", e))
    })?;

    Ok(HttpResponse::Created().json(serde_json::json!({
        "id": id,
        "station_id": charger.station_id,
        "connector_type": charger.connector_type,
        "power_kw": charger.power_kw,
        "status": charger.status,
    })))
}

pub async fn charger_get_handler(
    id: web::Path<String>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AppError> {
    let result = sqlx::query_as!(
        ChargerResponse,
        r#"
        SELECT id, station_id, connector_type, power_kw, status
        FROM inventory.charger
        WHERE id = $1
        "#,
        id.to_string()
    )
    .fetch_optional(pool.get_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to get charger: {}", e);
        AppError::DatabaseError(format!("Failed to get charger: {}", e))
    })?;

    match result {
        Some(charger) => Ok(HttpResponse::Ok().json(charger)),
        None => Err(AppError::EntityNotFoundError(format!(
            "Charger with ID '{}' not found",
            id
        ))),
    }
}

pub async fn charger_list_handler(pool: web::Data<PgPool>) -> Result<HttpResponse, AppError> {
    let chargers = sqlx::query_as!(
        ChargerResponse,
        r#"
        SELECT id, station_id, connector_type, power_kw, status
        FROM inventory.charger
        ORDER BY station_id ASC, connector_type ASC
        "#
    )
    .fetch_all(pool.get_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to list chargers: {}", e);
        AppError::DatabaseError(format!("Failed to list chargers: {}", e))
    })?;

    Ok(HttpResponse::Ok().json(ChargerListResponse {
        chargers,
        pagination: None,
    }))
}

pub async fn charger_update_handler(
    id: web::Path<String>,
    charger: web::Json<ChargerRequest>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AppError> {
    // Validate station_id exists if being updated
    if let Some(ref station_id) = charger.station_id.clone() {
        sqlx::query!("SELECT id FROM inventory.station WHERE id = $1", station_id.clone())
            .fetch_optional(pool.get_ref())
            .await
            .map_err(|e| {
                tracing::error!("Failed to validate station: {}", e);
                AppError::DatabaseError(format!("Failed to validate station: {}", e))
            })?;
    }

    sqlx::query!(
        r#"
        UPDATE inventory.charger
        SET station_id = COALESCE($1, station_id), connector_type = $2, power_kw = $3, status = $4, updated_at = NOW()
        WHERE id = $5
        "#,
        charger.station_id,
        charger.connector_type,
        charger.power_kw,
        charger.status,
        id.to_string()
    )
    .execute(pool.get_ref())
    .await
    .map_err(|e| {
        tracing::error!("Failed to update charger: {}", e);
        AppError::DatabaseError(format!("Failed to update charger: {}", e))
    })?;

    charger_get_handler(id, pool).await
}

pub async fn charger_delete_handler(
    id: web::Path<String>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AppError> {
    let result = sqlx::query!("DELETE FROM inventory.charger WHERE id = $1 RETURNING id", id.to_string())
        .fetch_optional(pool.get_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to delete charger: {}", e);
            AppError::DatabaseError(format!("Failed to delete charger: {}", e))
        })?;

    match result {
        Some(_) => Ok(HttpResponse::NoContent().finish()),
        None => Err(AppError::EntityNotFoundError(format!(
            "Charger with ID '{}' not found",
            id
        ))),
    }
}
