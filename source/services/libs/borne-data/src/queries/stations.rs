use sqlx::PgPool;

use crate::error::DataLayerError;
use crate::models::charger::Charger;
use crate::models::partner::Partner;
use crate::models::station::Station;

pub struct StationDetail {
    pub station: Station,
    pub chargers: Vec<Charger>,
    pub partner: Partner,
}

pub async fn list_all(pool: &PgPool) -> Result<Vec<Station>, DataLayerError> {
    let stations = sqlx::query_as::<_, Station>(
        r#"
        SELECT id, partner_id, name, address, latitude::double precision, longitude::double precision,
               created_at, created_by, updated_at, updated_by
        FROM inventory.station
        ORDER BY name
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(|e| DataLayerError::Query(e.to_string()))?;

    Ok(stations)
}

pub async fn find_nearby(
    pool: &PgPool,
    lat: f64,
    lng: f64,
    radius_meters: f64,
) -> Result<Vec<Station>, DataLayerError> {
    let stations = sqlx::query_as::<_, Station>(
        r#"
        SELECT id, partner_id, name, address, latitude::double precision, longitude::double precision,
               created_at, created_by, updated_at, updated_by
        FROM inventory.station
        WHERE ST_DWithin(
            ST_SetSRID(ST_MakePoint(longitude::double precision, latitude::double precision), 4326)::geography,
            ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
            $3
        )
        ORDER BY
            ST_SetSRID(ST_MakePoint(longitude::double precision, latitude::double precision), 4326)::geography
            <->
            ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
        "#,
    )
    .bind(lng)
    .bind(lat)
    .bind(radius_meters)
    .fetch_all(pool)
    .await
    .map_err(|e| DataLayerError::Query(e.to_string()))?;

    Ok(stations)
}

pub async fn find_by_id(pool: &PgPool, id: &str) -> Result<StationDetail, DataLayerError> {
    let station = sqlx::query_as::<_, Station>(
        r#"
        SELECT id, partner_id, name, address, latitude::double precision, longitude::double precision,
               created_at, created_by, updated_at, updated_by
        FROM inventory.station
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|e| DataLayerError::Query(e.to_string()))?
    .ok_or_else(|| DataLayerError::NotFound(format!("Station {}", id)))?;

    let chargers = sqlx::query_as::<_, Charger>(
        r#"
        SELECT id, station_id, connector_type, power_kw::double precision, status,
               created_at, created_by, updated_at, updated_by
        FROM inventory.charger
        WHERE station_id = $1
        ORDER BY connector_type
        "#,
    )
    .bind(id)
    .fetch_all(pool)
    .await
    .map_err(|e| DataLayerError::Query(e.to_string()))?;

    let partner = sqlx::query_as::<_, Partner>(
        r#"
        SELECT id, name, type, is_verified, is_active, is_live,
               created_at, created_by, updated_at, updated_by
        FROM inventory.partner
        WHERE id = $1
        "#,
    )
    .bind(&station.partner_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| DataLayerError::Query(e.to_string()))?
    .ok_or_else(|| DataLayerError::NotFound(format!("Partner {}", station.partner_id)))?;

    Ok(StationDetail {
        station,
        chargers,
        partner,
    })
}
