use sqlx::PgPool;
use uuid::Uuid;
use services_shared::domain::{PartnerDto, StationDto, ChargerDetailDto};
use services_shared::auth::MVP1_FALLBACK_OPERATOR;
use crate::error::AdminServiceError;
use chrono::Utc;

/// Generate a deterministic UUID-based ID with a prefix
fn generate_prefixed_id(prefix: &str) -> String {
    format!("{}-{}", prefix, Uuid::new_v4().to_string().replace("-", "").chars().take(16).collect::<String>())
}

pub async fn create_partner(
    pool: &PgPool,
    name: String,
    partner_type: String,
    email: String,
    phone: String,
) -> Result<PartnerDto, AdminServiceError> {
    let id = generate_prefixed_id("par");
    let now = Utc::now();

    let partner = sqlx::query_as::<_, PartnerDto>(
        r#"
        INSERT INTO inventory.partners (id, name, type, email, phone, created_by, updated_by, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id, name as name, type as partner_type, email, phone, verified, created_at, updated_at
        "#
    )
    .bind(&id)
    .bind(&name)
    .bind(&partner_type)
    .bind(&email)
    .bind(&phone)
    .bind(MVP1_FALLBACK_OPERATOR)
    .bind(MVP1_FALLBACK_OPERATOR)
    .bind(now)
    .bind(now)
    .fetch_one(pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to create partner: {}", e);
        AdminServiceError::DatabaseError(format!("Failed to create partner: {}", e))
    })?;

    Ok(partner)
}

pub async fn get_partner(
    pool: &PgPool,
    partner_id: &str,
) -> Result<PartnerDto, AdminServiceError> {
    let partner = sqlx::query_as::<_, PartnerDto>(
        "SELECT id, name, type as partner_type, email, phone, verified, created_at, updated_at FROM inventory.partners WHERE id = $1"
    )
    .bind(partner_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| {
        tracing::error!("Database error: {}", e);
        AdminServiceError::DatabaseError(format!("Failed to fetch partner: {}", e))
    })?
    .ok_or_else(|| AdminServiceError::NotFound(format!("Partner {} not found", partner_id)))?;

    Ok(partner)
}

pub async fn create_station(
    pool: &PgPool,
    partner_id: String,
    name: String,
    address: String,
    email: String,
    latitude: f64,
    longitude: f64,
) -> Result<StationDto, AdminServiceError> {
    // Verify partner exists
    let _ = get_partner(pool, &partner_id).await?;

    // Validate coordinates
    if !geo_core::is_within_tunisia(longitude, latitude) {
        return Err(AdminServiceError::InvalidRequest(
            "Station location must be within Tunisia bounds".to_string()
        ));
    }

    let id = generate_prefixed_id("stn");
    let now = Utc::now();

    let station = sqlx::query_as::<_, StationDto>(
        r#"
        INSERT INTO inventory.stations (id, partner_id, name, address, email, latitude, longitude, created_by, updated_by, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        RETURNING id, partner_id, name, address, email, latitude, longitude, availability, verified, is_live, created_at, updated_at
        "#
    )
    .bind(&id)
    .bind(&partner_id)
    .bind(&name)
    .bind(&address)
    .bind(&email)
    .bind(latitude)
    .bind(longitude)
    .bind(MVP1_FALLBACK_OPERATOR)
    .bind(MVP1_FALLBACK_OPERATOR)
    .bind(now)
    .bind(now)
    .fetch_one(pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to create station: {}", e);
        AdminServiceError::DatabaseError(format!("Failed to create station: {}", e))
    })?;

    tracing::info!("Created station {}", id);
    Ok(station)
}

pub async fn update_station_live_status(
    pool: &PgPool,
    station_id: &str,
    is_live: bool,
) -> Result<StationDto, AdminServiceError> {
    let now = Utc::now();

    let station = sqlx::query_as::<_, StationDto>(
        r#"
        UPDATE inventory.stations 
        SET is_live = $1, updated_by = $2, updated_at = $3
        WHERE id = $4
        RETURNING id, partner_id, name, address, email, latitude, longitude, availability, verified, is_live, created_at, updated_at
        "#
    )
    .bind(is_live)
    .bind(MVP1_FALLBACK_OPERATOR)
    .bind(now)
    .bind(station_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to update station: {}", e);
        AdminServiceError::DatabaseError(format!("Failed to update station: {}", e))
    })?
    .ok_or_else(|| AdminServiceError::NotFound(format!("Station {} not found", station_id)))?;

    tracing::info!("Updated station {} live status to {}", station_id, is_live);
    Ok(station)
}

pub async fn create_charger(
    pool: &PgPool,
    station_id: String,
    identifier_code: String,
    plug_type_code: String,
    max_power_kw: i32,
) -> Result<ChargerDetailDto, AdminServiceError> {
    // Verify station exists
    let _ = sqlx::query("SELECT id FROM inventory.stations WHERE id = $1")
        .bind(&station_id)
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            tracing::error!("Database error: {}", e);
            AdminServiceError::DatabaseError(format!("Failed to check station: {}", e))
        })?
        .ok_or_else(|| AdminServiceError::NotFound(format!("Station {} not found", station_id)))?;

    // Verify plug type exists
    let _ = sqlx::query("SELECT code_key FROM configuration.plug_types WHERE code_key = $1")
        .bind(&plug_type_code)
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            tracing::error!("Database error: {}", e);
            AdminServiceError::DatabaseError(format!("Failed to check plug type: {}", e))
        })?
        .ok_or_else(|| AdminServiceError::InvalidRequest(format!("Plug type {} not found", plug_type_code)))?;

    let id = generate_prefixed_id("chr");
    let now = Utc::now();

    let charger = sqlx::query_as::<_, ChargerDetailDto>(
        r#"
        INSERT INTO inventory.chargers (id, station_id, identifier_code, plug_type_code, max_power_kw, created_by, updated_by, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id, station_id, identifier_code, plug_type_code, max_power_kw, status, created_at, updated_at
        "#
    )
    .bind(&id)
    .bind(&station_id)
    .bind(&identifier_code)
    .bind(&plug_type_code)
    .bind(max_power_kw)
    .bind(MVP1_FALLBACK_OPERATOR)
    .bind(MVP1_FALLBACK_OPERATOR)
    .bind(now)
    .bind(now)
    .fetch_one(pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to create charger: {}", e);
        AdminServiceError::DatabaseError(format!("Failed to create charger: {}", e))
    })?;

    tracing::info!("Created charger {} for station {}", id, station_id);
    Ok(charger)
}
