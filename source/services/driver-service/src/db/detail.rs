use crate::error::AppError;
use crate::models::StationDetail;
use sqlx::Row;

pub async fn get_station(
    pool: &sqlx::PgPool,
    station_id: &str,
) -> Result<StationDetail, AppError> {
    let station = sqlx::query(
        r#"
        SELECT s.id, s.name, s.address, s.latitude, s.longitude
        FROM "ev-platform".station s
        JOIN "ev-platform".partner p ON s.partner_id = p.id
        WHERE p.is_verified = true AND p.is_live = true AND p.is_active = true
          AND s.id = $1
        "#,
    )
    .bind(station_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Station {} not found", station_id)))?;

    let id: String = station.get("id");
    let name: String = station.get("name");
    let address: Option<String> = station.get("address");
    let latitude: f64 = station.get("latitude");
    let longitude: f64 = station.get("longitude");

    let charger_rows = sqlx::query(
        r#"
        SELECT c.id, c.connector_type, c.power_kw, c.status
        FROM "ev-platform".charger c
        WHERE c.station_id = $1
        ORDER BY c.id
        "#,
    )
    .bind(station_id)
    .fetch_all(pool)
    .await?;

    let chargers = charger_rows
        .into_iter()
        .map(|row| crate::models::ChargerInfo {
            id: row.get("id"),
            connector_type: row.get("connector_type"),
            power_kw: row.get("power_kw"),
            status: row.get("status"),
        })
        .collect();

    Ok(StationDetail {
        id,
        name,
        address,
        latitude,
        longitude,
        chargers,
    })
}
