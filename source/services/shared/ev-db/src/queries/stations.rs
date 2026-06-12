use ev_core::error::{AppError, FieldError};
use ev_core::station::{NearbyStation, PaginatedResponse, Station};
use ev_core::charger::{Charger, CreateChargerRequest};
use sqlx::PgPool;

#[derive(Debug, sqlx::FromRow)]
struct StationRow {
    pub id: String,
    pub name: String,
    pub address: String,
    pub lat: f64,
    pub lng: f64,
    pub status: String,
    pub opening_hours: Option<String>,
    pub partner_id: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub deleted_at: Option<chrono::NaiveDateTime>,
}

impl StationRow {
    fn into_station(self, chargers: Option<Vec<Charger>>) -> Station {
        Station {
            id: self.id,
            name: self.name,
            address: self.address,
            lat: self.lat,
            lng: self.lng,
            status: self.status,
            opening_hours: self.opening_hours,
            partner_id: self.partner_id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            deleted_at: self.deleted_at,
            chargers,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct ChargerRow {
    pub id: String,
    pub station_id: String,
    #[sqlx(rename = "type")]
    pub charger_type: String,
    pub power_kw: f64,
    pub status: String,
    pub price_per_kwh: f64,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub deleted_at: Option<chrono::NaiveDateTime>,
}

impl ChargerRow {
    fn into_charger(self) -> Charger {
        Charger {
            id: self.id,
            station_id: self.station_id,
            charger_type: self.charger_type,
            power_kw: self.power_kw,
            status: self.status,
            price_per_kwh: self.price_per_kwh,
            created_at: self.created_at,
            updated_at: self.updated_at,
            deleted_at: self.deleted_at,
        }
    }
}

pub async fn list_stations(
    pool: &PgPool,
    page: i64,
    per_page: i64,
) -> Result<PaginatedResponse<Station>, AppError> {
    let offset = (page - 1) * per_page;

    let total: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM inventory.station WHERE deleted_at IS NULL"
    )
    .fetch_one(pool)
    .await?;

    let rows: Vec<StationRow> = sqlx::query_as(
        r#"
        SELECT id, name, address, lat, lng, status, opening_hours,
               partner_id, created_at, updated_at, deleted_at
        FROM inventory.station
        WHERE deleted_at IS NULL
        ORDER BY name
        LIMIT $1 OFFSET $2
        "#
    )
    .bind(per_page)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    let stations: Vec<Station> = rows.into_iter().map(|r| r.into_station(None)).collect();

    Ok(PaginatedResponse {
        data: stations,
        total: total.0,
        page,
        per_page,
    })
}

pub async fn find_station_by_id(
    pool: &PgPool,
    id: &str,
) -> Result<Station, AppError> {
    let row: StationRow = sqlx::query_as(
        r#"
        SELECT id, name, address, lat, lng, status, opening_hours,
               partner_id, created_at, updated_at, deleted_at
        FROM inventory.station
        WHERE id = $1 AND deleted_at IS NULL
        "#
    )
    .bind(id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Station not found: {}", id)))?;

    Ok(row.into_station(None))
}

pub async fn find_chargers_by_station_id(
    pool: &PgPool,
    station_id: &str,
) -> Result<Vec<Charger>, AppError> {
    let rows: Vec<ChargerRow> = sqlx::query_as(
        r#"
        SELECT id, station_id, type, power_kw, status,
               price_per_kwh, created_at, updated_at, deleted_at
        FROM inventory.charger
        WHERE station_id = $1 AND deleted_at IS NULL
        ORDER BY type
        "#
    )
    .bind(station_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|r| r.into_charger()).collect())
}

pub async fn find_nearby_stations(
    pool: &PgPool,
    lat: f64,
    lng: f64,
    radius_km: f64,
) -> Result<Vec<NearbyStation>, AppError> {
    let radius_meters = radius_km * 1000.0;

    #[derive(Debug, sqlx::FromRow)]
    struct NearbyRow {
        id: String,
        name: String,
        address: String,
        lat: f64,
        lng: f64,
        status: String,
        opening_hours: Option<String>,
        partner_id: String,
        distance_meters: f64,
    }

    let rows: Vec<NearbyRow> = sqlx::query_as(
        r#"
        SELECT id, name, address, lat, lng, status, opening_hours, partner_id,
               ST_Distance(
                   location::geography,
                   ST_SetSRID(ST_MakePoint($2, $1), 4326)::geography
               ) AS distance_meters
        FROM inventory.station
        WHERE deleted_at IS NULL
          AND ST_DWithin(
              location::geography,
              ST_SetSRID(ST_MakePoint($2, $1), 4326)::geography,
              $3
          )
        ORDER BY distance_meters ASC
        "#
    )
    .bind(lat)
    .bind(lng)
    .bind(radius_meters)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| NearbyStation {
            id: r.id,
            name: r.name,
            address: r.address,
            lat: r.lat,
            lng: r.lng,
            status: r.status,
            opening_hours: r.opening_hours,
            partner_id: r.partner_id,
            distance_km: (r.distance_meters / 1000.0 * 100.0).round() / 100.0,
        })
        .collect())
}

pub async fn insert_station_with_chargers(
    pool: &PgPool,
    station_id: &str,
    name: &str,
    address: &str,
    lat: f64,
    lng: f64,
    partner_id: &str,
    opening_hours: Option<&str>,
    chargers: &[CreateChargerRequest],
) -> Result<Station, AppError> {
    let mut tx = pool.begin().await.map_err(AppError::Database)?;

    sqlx::query(
        r#"
        INSERT INTO inventory.station (id, name, address, lat, lng, status, opening_hours, partner_id)
        VALUES ($1, $2, $3, $4, $5, 'offline', $6, $7)
        "#
    )
    .bind(station_id)
    .bind(name)
    .bind(address)
    .bind(lat)
    .bind(lng)
    .bind(opening_hours)
    .bind(partner_id)
    .execute(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    for charger in chargers {
        let charger_id = ev_core::id::generate_entity_id(ev_core::id::EntityPrefix::Charger);
        sqlx::query(
            r#"
            INSERT INTO inventory.charger (id, station_id, type, power_kw, price_per_kwh, status)
            VALUES ($1, $2, $3, $4, $5, 'offline')
            "#
        )
        .bind(&charger_id)
        .bind(station_id)
        .bind(&charger.charger_type)
        .bind(charger.power_kw)
        .bind(charger.price_per_kwh.unwrap_or(0.0))
        .execute(&mut *tx)
        .await
        .map_err(AppError::Database)?;
    }

    tx.commit().await.map_err(AppError::Database)?;

    let mut station = find_station_by_id(pool, station_id).await?;
    let chargers = find_chargers_by_station_id(pool, station_id).await?;
    station.chargers = Some(chargers);

    Ok(station)
}

pub async fn update_station(
    pool: &PgPool,
    id: &str,
    name: Option<&str>,
    address: Option<&str>,
    lat: Option<f64>,
    lng: Option<f64>,
    status: Option<&str>,
    opening_hours: Option<Option<&str>>,
) -> Result<Station, AppError> {
    let existing = find_station_by_id(pool, id).await?;

    let final_name = name.unwrap_or(&existing.name);
    let final_address = address.unwrap_or(&existing.address);
    let final_lat = lat.unwrap_or(existing.lat);
    let final_lng = lng.unwrap_or(existing.lng);
    let final_status = status.unwrap_or(&existing.status);
    let final_hours: Option<&str> = match opening_hours {
        Some(Some(h)) => Some(h),
        Some(None) => None,
        None => existing.opening_hours.as_deref(),
    };

    sqlx::query(
        r#"
        UPDATE inventory.station
        SET name = $1, address = $2, lat = $3, lng = $4,
            status = $5, opening_hours = $6, updated_at = NOW()
        WHERE id = $7 AND deleted_at IS NULL
        "#
    )
    .bind(final_name)
    .bind(final_address)
    .bind(final_lat)
    .bind(final_lng)
    .bind(final_status)
    .bind(final_hours)
    .bind(id)
    .execute(pool)
    .await
    .map_err(AppError::Database)?;

    find_station_by_id(pool, id).await
}

pub async fn soft_delete_station(
    pool: &PgPool,
    id: &str,
) -> Result<(), AppError> {
    let result = sqlx::query(
        r#"
        UPDATE inventory.station
        SET deleted_at = NOW(), updated_at = NOW()
        WHERE id = $1 AND deleted_at IS NULL
        "#
    )
    .bind(id)
    .execute(pool)
    .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(format!("Station not found: {}", id)));
    }

    Ok(())
}

pub async fn validate_partner_exists(
    pool: &PgPool,
    partner_id: &str,
) -> Result<(), AppError> {
    let exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM inventory.partner WHERE id = $1)"
    )
    .bind(partner_id)
    .fetch_one(pool)
    .await?;

    if !exists.0 {
        return Err(AppError::BadRequest(format!(
            "Partner not found: {}",
            partner_id
        )));
    }

    Ok(())
}

pub fn validate_create_request(
    name: &str,
    address: &str,
    lat: f64,
    lng: f64,
    chargers: &[CreateChargerRequest],
) -> Result<(), AppError> {
    let mut errors: Vec<FieldError> = Vec::new();

    if name.is_empty() {
        errors.push(FieldError {
            field: "name".into(),
            message: "Station name is required".into(),
        });
    }
    if address.is_empty() {
        errors.push(FieldError {
            field: "address".into(),
            message: "Address is required".into(),
        });
    }
    if !(-90.0..=90.0).contains(&lat) {
        errors.push(FieldError {
            field: "lat".into(),
            message: "Latitude must be between -90 and 90".into(),
        });
    }
    if !(-180.0..=180.0).contains(&lng) {
        errors.push(FieldError {
            field: "lng".into(),
            message: "Longitude must be between -180 and 180".into(),
        });
    }
    if chargers.is_empty() {
        errors.push(FieldError {
            field: "chargers".into(),
            message: "At least one charger is required".into(),
        });
    }
    for (i, charger) in chargers.iter().enumerate() {
        if charger.charger_type.is_empty() {
            errors.push(FieldError {
                field: format!("chargers[{}].type", i),
                message: "Charger type is required".into(),
            });
        }
        if charger.power_kw <= 0.0 {
            errors.push(FieldError {
                field: format!("chargers[{}].power_kw", i),
                message: "Power must be greater than 0".into(),
            });
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(AppError::Validation { details: errors })
    }
}
