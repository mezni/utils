use sqlx::{PgPool, Transaction};
use tracing::{error, info, warn};

use crate::db_models::{Partner, Station, Charger};
use crate::error::AuthError;
use crate::middleware::auth::UserContext;
use crate::services::audit_service::audit_diff_service;
use crate::services::cache_service::cache_bust_service;
use crate::services::materialized_view_service::mv_refresh_service;

#[derive(Debug, Clone)]
pub struct AdminOrchestrator {
    pub pool: PgPool,
}

impl AdminOrchestrator {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_partner(
        &self,
        user_context: &UserContext,
        name: String,
        network_type: String,
        support_phone: Option<String>,
        support_email: Option<String>,
    ) -> Result<crate::db_models::Partner, AuthError> {
        // Generate ID
        let partner_id = crate::utils::id_generator::generate_id("OPR");

        let partner = crate::db_models::Partner::new(partner_id.clone(), name, network_type);

        let mut tx = self.pool.begin().await?;

        // TODO: Implement actual repository call here (T023)
        // Execute SQL query to insert partner into inventory.partners table
        info!("Creating partner with id: {}", partner_id);

        // Commit transaction
        tx.commit().await?;

        // Post-commit steps (must happen AFTER commit per constitution)
        let before_snapshot = None; // CREATE operation has no before state

        let after_snapshot = serde_json::json!({
            "id": partner.id,
            "name": partner.name,
            "network_type": partner.network_type,
            "support_phone": partner.support_phone,
            "support_email": partner.support_email,
            "is_verified": partner.is_verified,
            "created_at": partner.created_at.to_rfc3339(),
        });

        // Audit logging (not transactional, failure tolerated)
        if let Err(e) = audit_diff_service(&self.pool, user_context, "partner.created", &partner_id, before_snapshot, after_snapshot, None).await {
            error!("Failed to write audit log: {}", e);
        }

        // MV refresh (must happen after audit)
        if let Err(e) = mv_refresh_service(&self.pool).await {
            warn!("Failed to refresh materialized views: {}", e);
        }

        // Redis cache bust (must happen after MV refresh)
        if let Err(e) = cache_bust_service(&self.pool, "stations:tile:*").await {
            warn!("Failed to invalidate cache: {}", e);
        }

        Ok(partner)
    }

    pub async fn update_partner(
        &self,
        user_context: &UserContext,
        partner_id: &str,
        name: String,
        support_phone: Option<String>,
        support_email: Option<String>,
        is_verified: bool,
        updated_by: Option<String>,
    ) -> Result<crate::db_models::Partner, AuthError> {
        // Fetch current state for BEFORE snapshot
        let before_snapshot = None; // TODO: Fetch from database

        let mut tx = self.pool.begin().await?;

        // TODO: Implement actual repository call here (T023)
        // Execute SQL query to update partner
        info!("Updating partner with id: {}", partner_id);

        // Commit transaction
        tx.commit().await?;

        let after_snapshot = serde_json::json!({
            "id": partner_id,
            "name": name,
            "support_phone": support_phone,
            "support_email": support_email,
            "is_verified": is_verified,
        });

        // Audit logging
        if let Err(e) = audit_diff_service(&self.pool, user_context, "partner.updated", partner_id, before_snapshot, after_snapshot, None).await {
            error!("Failed to write audit log: {}", e);
        }

        // MV refresh
        if let Err(e) = mv_refresh_service(&self.pool).await {
            warn!("Failed to refresh materialized views: {}", e);
        }

        // Redis cache bust
        if let Err(e) = cache_bust_service(&self.pool, "stations:tile:*").await {
            warn!("Failed to invalidate cache: {}", e);
        }

        Ok(crate::db_models::Partner::new(partner_id.to_string(), name, crate::db_models::NetworkType::Individual)) // TODO: Need actual network_type
    }

    pub async fn create_station(
        &self,
        user_context: &UserContext,
        partner_id: String,
        name: String,
        address: Option<String>,
        location: crate::db_models::GeoLocation,
        osm_id: Option<i64>,
    ) -> Result<crate::db_models::Station, AuthError> {
        let station_id = crate::utils::id_generator::generate_id("STA");

        let station = crate::db_models::Station::new(station_id.clone(), partner_id, name, location.clone());

        let mut tx = self.pool.begin().await?;

        // TODO: Implement actual repository call here (T034)
        info!("Creating station with id: {}", station_id);

        tx.commit().await?;

        let before_snapshot = None;

        let after_snapshot = serde_json::json!({
            "id": station_id,
            "partner_id": partner_id,
            "name": name,
            "address": address,
            "location": location,
        });

        if let Err(e) = audit_diff_service(&self.pool, user_context, "station.created", &station_id, before_snapshot, after_snapshot, None).await {
            error!("Failed to write audit log: {}", e);
        }

        if let Err(e) = mv_refresh_service(&self.pool).await {
            warn!("Failed to refresh materialized views: {}", e);
        }

        if let Err(e) = cache_bust_service(&self.pool, "stations:near:*").await {
            warn!("Failed to invalidate cache: {}", e);
        }

        Ok(station)
    }

    pub async fn create_charger(
        &self,
        user_context: &UserContext,
        station_id: String,
        connector_type_id: i32,
        status_id: i32,
        current_type_id: i32,
        power_kw: Option<f64>,
        voltage: Option<i32>,
        amperage: Option<i32>,
        count_available: i32,
        count_total: i32,
    ) -> Result<crate::db_models::Charger, AuthError> {
        let charger_id = crate::utils::id_generator::generate_id("CHG");

        let charger = crate::db_models::Charger::new(
            charger_id.clone(),
            station_id,
            connector_type_id,
            status_id,
            current_type_id,
            power_kw,
            voltage,
            amperage,
            count_available,
            count_total,
        );

        let mut tx = self.pool.begin().await?;

        // TODO: Implement actual repository call here (T045)
        info!("Creating charger with id: {}", charger_id);

        tx.commit().await?;

        let before_snapshot = None;

        let after_snapshot = serde_json::json!({
            "id": charger_id,
            "station_id": station_id,
            "connector_type_id": connector_type_id,
            "status_id": status_id,
            "current_type_id": current_type_id,
            "power_kw": power_kw,
            "voltage": voltage,
            "amperage": amperage,
            "count_available": count_available,
            "count_total": count_total,
        });

        if let Err(e) = audit_diff_service(&self.pool, user_context, "charger.created", &charger_id, before_snapshot, after_snapshot, None).await {
            error!("Failed to write audit log: {}", e);
        }

        if let Err(e) = mv_refresh_service(&self.pool).await {
            warn!("Failed to refresh materialized views: {}", e);
        }

        if let Err(e) = cache_bust_service(&self.pool, "stations:near:*").await {
            warn!("Failed to invalidate cache: {}", e);
        }

        Ok(charger)
    }
}
