use sqlx::PgPool;
use nanoid::nanoid;

use crate::models::station::{CreateStationRequest, Station, StationResponse, UpdateStationRequest};
use crate::error::AppError;

fn generate_station_id() -> String {
    format!("STA-{}", nanoid!(12))
}

pub async fn insert(pool: &PgPool, req: &CreateStationRequest) -> Result<StationResponse, AppError> {
    let id = generate_station_id();
    let wkt = format!("POINT({} {})", req.longitude, req.latitude);

    let station = sqlx::query_as::<_, Station>(
        r#"
        INSERT INTO inventory.stations (id, partner_id, name, address, location)
        VALUES ($1, $2, $3, $4, ST_GeogFromText($5))
        RETURNING id, partner_id, name, address,
                  ST_AsText(location) AS location,
                  deleted_at, created_at, updated_at
        "#,
    )
    .bind(&id)
    .bind(&req.partner_id)
    .bind(&req.name)
    .bind(&req.address)
    .bind(&wkt)
    .fetch_one(pool)
    .await?;

    Ok(to_response(station))
}

pub async fn select_all(pool: &PgPool) -> Result<Vec<StationResponse>, AppError> {
    let stations = sqlx::query_as::<_, Station>(
        r#"
        SELECT id, partner_id, name, address,
               ST_AsText(location) AS location,
               deleted_at, created_at, updated_at
        FROM inventory.stations
        WHERE deleted_at IS NULL
        ORDER BY created_at DESC
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(stations.into_iter().map(to_response).collect())
}

pub async fn select_by_id(pool: &PgPool, id: &str) -> Result<StationResponse, AppError> {
    let station = sqlx::query_as::<_, Station>(
        r#"
        SELECT id, partner_id, name, address,
               ST_AsText(location) AS location,
               deleted_at, created_at, updated_at
        FROM inventory.stations
        WHERE id = $1 AND deleted_at IS NULL
        "#,
    )
    .bind(id)
    .fetch_one(pool)
    .await?;

    Ok(to_response(station))
}

pub async fn update(pool: &PgPool, id: &str, req: &UpdateStationRequest) -> Result<StationResponse, AppError> {
    if fields_present(req).is_empty() {
        return Err(AppError::BadRequest("No valid fields provided for update".into()));
    }

    let current = select_by_id(pool, id).await?;
    let name = req.name.as_deref().unwrap_or(&current.name).to_string();
    let address = req.address.clone().or(current.address);
    let (latitude, longitude) = match (req.latitude, req.longitude) {
        (Some(lat), Some(lon)) => (lat, lon),
        _ => (current.latitude, current.longitude),
    };
    let wkt = format!("POINT({longitude} {latitude})");

    let station = sqlx::query_as::<_, Station>(
        r#"
        UPDATE inventory.stations
        SET name = $2, address = $3, location = ST_GeogFromText($4), updated_at = NOW()
        WHERE id = $1 AND deleted_at IS NULL
        RETURNING id, partner_id, name, address,
                  ST_AsText(location) AS location,
                  deleted_at, created_at, updated_at
        "#,
    )
    .bind(id)
    .bind(&name)
    .bind(&address)
    .bind(&wkt)
    .fetch_one(pool)
    .await?;

    Ok(to_response(station))
}

pub async fn soft_delete(pool: &PgPool, id: &str) -> Result<(), AppError> {
    let result = sqlx::query(
        r#"UPDATE inventory.stations SET deleted_at = NOW() WHERE id = $1 AND deleted_at IS NULL"#,
    )
    .bind(id)
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Station not found".into()));
    }
    Ok(())
}

fn to_response(s: Station) -> StationResponse {
    let (lat, lon) = parse_wkt_point(&s.location);
    StationResponse {
        id: s.id,
        partner_id: s.partner_id,
        name: s.name,
        address: s.address,
        latitude: lat,
        longitude: lon,
        created_at: s.created_at,
        updated_at: s.updated_at,
    }
}

fn parse_wkt_point(wkt: &str) -> (f64, f64) {
    let cleaned = wkt
        .trim_start_matches("POINT(")
        .trim_end_matches(')');
    let parts: Vec<&str> = cleaned.split_whitespace().collect();
    if parts.len() >= 2 {
        let lon: f64 = parts[0].parse().unwrap_or(0.0);
        let lat: f64 = parts[1].parse().unwrap_or(0.0);
        (lat, lon)
    } else {
        (0.0, 0.0)
    }
}

fn fields_present(req: &UpdateStationRequest) -> Vec<&str> {
    let mut fields = Vec::new();
    if req.name.is_some() { fields.push("name"); }
    if req.address.is_some() { fields.push("address"); }
    if req.latitude.is_some() { fields.push("latitude"); }
    if req.longitude.is_some() { fields.push("longitude"); }
    fields
}
