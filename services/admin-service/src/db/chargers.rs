use sqlx::PgPool;
use nanoid::nanoid;

use crate::models::charger::{Charger, CreateChargerRequest, UpdateChargerRequest};
use crate::error::AppError;

fn generate_charger_id() -> String {
    format!("CHG-{}", nanoid!(12))
}

pub async fn insert(pool: &PgPool, req: &CreateChargerRequest) -> Result<Charger, AppError> {
    let id = generate_charger_id();
    let charger = sqlx::query_as::<_, Charger>(
        r#"
        INSERT INTO inventory.chargers
            (id, station_id, connector_type_id, current_type_id, status_id,
             power_kw, voltage, amperage, count_available, count_total)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING id, station_id, connector_type_id, current_type_id, status_id,
                  power_kw, voltage, amperage, count_available, count_total,
                  deleted_at, created_at, updated_at
        "#,
    )
    .bind(&id)
    .bind(&req.station_id)
    .bind(req.connector_type_id)
    .bind(req.current_type_id)
    .bind(req.status_id)
    .bind(req.power_kw)
    .bind(req.voltage)
    .bind(req.amperage)
    .bind(req.count_available.unwrap_or(1))
    .bind(req.count_total.unwrap_or(1))
    .fetch_one(pool)
    .await?;
    Ok(charger)
}

pub async fn select_all(pool: &PgPool, station_id: Option<&str>) -> Result<Vec<Charger>, AppError> {
    let chargers = match station_id {
        Some(sid) => {
            sqlx::query_as::<_, Charger>(
                r#"
                SELECT id, station_id, connector_type_id, current_type_id, status_id,
                       power_kw, voltage, amperage, count_available, count_total,
                       deleted_at, created_at, updated_at
                FROM inventory.chargers
                WHERE deleted_at IS NULL AND station_id = $1
                ORDER BY created_at DESC
                "#,
            )
            .bind(sid)
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query_as::<_, Charger>(
                r#"
                SELECT id, station_id, connector_type_id, current_type_id, status_id,
                       power_kw, voltage, amperage, count_available, count_total,
                       deleted_at, created_at, updated_at
                FROM inventory.chargers
                WHERE deleted_at IS NULL
                ORDER BY created_at DESC
                "#,
            )
            .fetch_all(pool)
            .await?
        }
    };
    Ok(chargers)
}

pub async fn select_by_id(pool: &PgPool, id: &str) -> Result<Charger, AppError> {
    let charger = sqlx::query_as::<_, Charger>(
        r#"
        SELECT id, station_id, connector_type_id, current_type_id, status_id,
               power_kw, voltage, amperage, count_available, count_total,
               deleted_at, created_at, updated_at
        FROM inventory.chargers
        WHERE id = $1 AND deleted_at IS NULL
        "#,
    )
    .bind(id)
    .fetch_one(pool)
    .await?;
    Ok(charger)
}

pub async fn update(pool: &PgPool, id: &str, req: &UpdateChargerRequest) -> Result<Charger, AppError> {
    if fields_present(req).is_empty() {
        return Err(AppError::BadRequest("No valid fields provided for update".into()));
    }

    let current = select_by_id(pool, id).await?;
    let connector_type_id = req.connector_type_id.unwrap_or(current.connector_type_id);
    let current_type_id = req.current_type_id.unwrap_or(current.current_type_id);
    let status_id = req.status_id.unwrap_or(current.status_id);
    let power_kw = req.power_kw.or(current.power_kw);
    let voltage = req.voltage.or(current.voltage);
    let amperage = req.amperage.or(current.amperage);
    let count_available = req.count_available.unwrap_or(current.count_available);
    let count_total = req.count_total.unwrap_or(current.count_total);

    let charger = sqlx::query_as::<_, Charger>(
        r#"
        UPDATE inventory.chargers
        SET connector_type_id = $2, current_type_id = $3, status_id = $4,
            power_kw = $5, voltage = $6, amperage = $7,
            count_available = $8, count_total = $9, updated_at = NOW()
        WHERE id = $1 AND deleted_at IS NULL
        RETURNING id, station_id, connector_type_id, current_type_id, status_id,
                  power_kw, voltage, amperage, count_available, count_total,
                  deleted_at, created_at, updated_at
        "#,
    )
    .bind(id)
    .bind(connector_type_id)
    .bind(current_type_id)
    .bind(status_id)
    .bind(power_kw)
    .bind(voltage)
    .bind(amperage)
    .bind(count_available)
    .bind(count_total)
    .fetch_one(pool)
    .await?;
    Ok(charger)
}

pub async fn soft_delete(pool: &PgPool, id: &str) -> Result<(), AppError> {
    let result = sqlx::query(
        r#"UPDATE inventory.chargers SET deleted_at = NOW() WHERE id = $1 AND deleted_at IS NULL"#,
    )
    .bind(id)
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Charger not found".into()));
    }
    Ok(())
}

fn fields_present(req: &UpdateChargerRequest) -> Vec<&str> {
    let mut fields = Vec::new();
    if req.connector_type_id.is_some() { fields.push("connector_type_id"); }
    if req.current_type_id.is_some() { fields.push("current_type_id"); }
    if req.status_id.is_some() { fields.push("status_id"); }
    if req.power_kw.is_some() { fields.push("power_kw"); }
    if req.voltage.is_some() { fields.push("voltage"); }
    if req.amperage.is_some() { fields.push("amperage"); }
    if req.count_available.is_some() { fields.push("count_available"); }
    if req.count_total.is_some() { fields.push("count_total"); }
    fields
}
