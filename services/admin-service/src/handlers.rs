use actix_web::{web, HttpResponse};
use sqlx::PgPool;

use crate::{
    error::AppError,
    models::{
        ChargerListResponse, ChargerRequest, ChargerResponse, PartnerListResponse,
        PartnerRequest, PartnerResponse, StationListResponse, StationRequest, StationResponse,
    },
};

pub async fn health_check_handler(
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AppError> {
    sqlx::query("SELECT 1")
        .fetch_one(pool.get_ref())
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

pub async fn partner_create_handler(
    pool: web::Data<PgPool>,
    partner: web::Json<PartnerRequest>,
) -> Result<HttpResponse, AppError> {
    let id = uuid::Uuid::new_v4().to_string();

    sqlx::query(
        r#"
        INSERT INTO inventory.partner (id, name, email, phone, address, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
        "#,
    )
    .bind(&id)
    .bind(&partner.name)
    .bind(&partner.email)
    .bind(&partner.phone)
    .bind(&partner.address)
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
    let result = sqlx::query_as::<_, PartnerResponse>(
        r#"
        SELECT id::text AS id, name, email, phone, address
        FROM inventory.partner
        WHERE id = $1
        "#,
    )
    .bind(id.to_string())
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
    let partners = sqlx::query_as::<_, PartnerResponse>(
        r#"
        SELECT id::text AS id, name, email, phone, address
        FROM inventory.partner
        ORDER BY name ASC
        "#,
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
    sqlx::query(
        r#"
        UPDATE inventory.partner
        SET name = $1, email = $2, phone = $3, address = $4, updated_at = NOW()
        WHERE id = $5
        "#,
    )
    .bind(&partner.name)
    .bind(&partner.email)
    .bind(&partner.phone)
    .bind(&partner.address)
    .bind(id.to_string())
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
    let result = sqlx::query(
        "DELETE FROM inventory.partner WHERE id = $1 RETURNING id",
    )
    .bind(id.to_string())
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

pub async fn station_create_handler(
    pool: web::Data<PgPool>,
    station: web::Json<StationRequest>,
) -> Result<HttpResponse, AppError> {
    let id = uuid::Uuid::new_v4().to_string();

    sqlx::query("SELECT id FROM inventory.partner WHERE id = $1")
        .bind(&station.partner_id)
        .fetch_optional(pool.get_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to validate partner: {}", e);
            AppError::DatabaseError(format!("Failed to validate partner: {}", e))
        })?;

    sqlx::query(
        r#"
        INSERT INTO inventory.station (id, partner_id, name, latitude, longitude, address, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
        "#,
    )
    .bind(&id)
    .bind(&station.partner_id)
    .bind(&station.name)
    .bind(station.latitude)
    .bind(station.longitude)
    .bind(&station.address)
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
    let result = sqlx::query_as::<_, StationResponse>(
        r#"
        SELECT id::text AS id, partner_id::text AS partner_id, name, latitude, longitude, address
        FROM inventory.station
        WHERE id = $1
        "#,
    )
    .bind(id.to_string())
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
    let stations = sqlx::query_as::<_, StationResponse>(
        r#"
        SELECT id::text AS id, partner_id::text AS partner_id, name, latitude, longitude, address
        FROM inventory.station
        ORDER BY name ASC
        "#,
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
    sqlx::query("SELECT id FROM inventory.partner WHERE id = $1")
        .bind(&station.partner_id)
        .fetch_optional(pool.get_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to validate partner: {}", e);
            AppError::DatabaseError(format!("Failed to validate partner: {}", e))
        })?;

    sqlx::query(
        r#"
        UPDATE inventory.station
        SET partner_id = COALESCE($1, partner_id), name = $2, latitude = $3, longitude = $4, address = $5, updated_at = NOW()
        WHERE id = $6
        "#,
    )
    .bind(&station.partner_id)
    .bind(&station.name)
    .bind(station.latitude)
    .bind(station.longitude)
    .bind(&station.address)
    .bind(id.to_string())
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
    let result = sqlx::query(
        "DELETE FROM inventory.station WHERE id = $1 RETURNING id",
    )
    .bind(id.to_string())
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

pub async fn charger_create_handler(
    pool: web::Data<PgPool>,
    charger: web::Json<ChargerRequest>,
) -> Result<HttpResponse, AppError> {
    let id = uuid::Uuid::new_v4().to_string();

    sqlx::query("SELECT id FROM inventory.station WHERE id = $1")
        .bind(&charger.station_id)
        .fetch_optional(pool.get_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to validate station: {}", e);
            AppError::DatabaseError(format!("Failed to validate station: {}", e))
        })?;

    sqlx::query(
        r#"
        INSERT INTO inventory.charger (id, station_id, connector_type, power_kw, status, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
        "#,
    )
    .bind(&id)
    .bind(&charger.station_id)
    .bind(&charger.connector_type)
    .bind(charger.power_kw)
    .bind(&charger.status)
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
    let result = sqlx::query_as::<_, ChargerResponse>(
        r#"
        SELECT id::text AS id, station_id::text AS station_id, connector_type, power_kw, status
        FROM inventory.charger
        WHERE id = $1
        "#,
    )
    .bind(id.to_string())
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
    let chargers = sqlx::query_as::<_, ChargerResponse>(
        r#"
        SELECT id::text AS id, station_id::text AS station_id, connector_type, power_kw, status
        FROM inventory.charger
        ORDER BY station_id ASC, connector_type ASC
        "#,
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
    sqlx::query("SELECT id FROM inventory.station WHERE id = $1")
        .bind(&charger.station_id)
        .fetch_optional(pool.get_ref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to validate station: {}", e);
            AppError::DatabaseError(format!("Failed to validate station: {}", e))
        })?;

    sqlx::query(
        r#"
        UPDATE inventory.charger
        SET station_id = COALESCE($1, station_id), connector_type = $2, power_kw = $3, status = $4, updated_at = NOW()
        WHERE id = $5
        "#,
    )
    .bind(&charger.station_id)
    .bind(&charger.connector_type)
    .bind(charger.power_kw)
    .bind(&charger.status)
    .bind(id.to_string())
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
    let result = sqlx::query(
        "DELETE FROM inventory.charger WHERE id = $1 RETURNING id",
    )
    .bind(id.to_string())
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
