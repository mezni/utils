use crate::error::AppError;
use crate::models::{ChargerResponse, CreateChargerRequest, UpdateChargerRequest};
use sqlx::PgPool;

pub async fn create_charger(
    pool: &PgPool,
    req: CreateChargerRequest,
    actor: &str,
) -> Result<ChargerResponse, AppError> {
    let id = ev_core::generate_id("CHG", 3);
    let status = req.status.as_deref().unwrap_or("offline");
    let rec = sqlx::query_as::<_, ChargerResponse>(
        r#"
        INSERT INTO "ev-platform".charger (id, station_id, connector_type, power_kw, status, created_by, updated_by)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING *
        "#,
    )
    .bind(&id)
    .bind(&req.station_id)
    .bind(&req.connector_type)
    .bind(req.power_kw)
    .bind(status)
    .bind(actor)
    .bind(actor)
    .fetch_one(pool)
    .await
    .map_err(|e| {
        if let sqlx::Error::Database(ref db_err) = e
            && let Some(code) = db_err.code()
            && code.as_ref() == "23503"
        {
            return AppError::NotFound(format!("Station {} not found", req.station_id));
        }
        AppError::from(e)
    })?;
    Ok(rec)
}

pub async fn get_charger(pool: &PgPool, id: &str) -> Result<ChargerResponse, AppError> {
    let rec = sqlx::query_as::<_, ChargerResponse>(
        r#"SELECT * FROM "ev-platform".charger WHERE id = $1"#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    rec.ok_or_else(|| AppError::NotFound(format!("Charger {} not found", id)))
}

pub async fn list_chargers(
    pool: &PgPool,
    station_id: Option<&str>,
    page: u32,
    page_size: u32,
) -> Result<ev_db::Paginated<ChargerResponse>, AppError> {
    let offset = ((page - 1) * page_size) as i64;
    let limit = page_size as i64;

    let (total, data) = if let Some(sid) = station_id {
        let total: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM "ev-platform".charger WHERE station_id = $1"#,
        )
        .bind(sid)
        .fetch_one(pool)
        .await?;

        let data = sqlx::query_as::<_, ChargerResponse>(
            r#"SELECT * FROM "ev-platform".charger WHERE station_id = $3 ORDER BY created_at DESC LIMIT $1 OFFSET $2"#,
        )
        .bind(limit)
        .bind(offset)
        .bind(sid)
        .fetch_all(pool)
        .await?;

        (total.0, data)
    } else {
        let total: (i64,) = sqlx::query_as(r#"SELECT COUNT(*) FROM "ev-platform".charger"#)
            .fetch_one(pool)
            .await?;

        let data = sqlx::query_as::<_, ChargerResponse>(
            r#"SELECT * FROM "ev-platform".charger ORDER BY created_at DESC LIMIT $1 OFFSET $2"#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?;

        (total.0, data)
    };

    Ok(ev_db::Paginated::new(data, total as u64, page, page_size))
}

pub async fn update_charger(
    pool: &PgPool,
    id: &str,
    req: UpdateChargerRequest,
    actor: &str,
) -> Result<ChargerResponse, AppError> {
    let rec = sqlx::query_as::<_, ChargerResponse>(
        r#"
        UPDATE "ev-platform".charger
        SET connector_type = COALESCE($2, connector_type),
            power_kw = COALESCE($3, power_kw),
            status = COALESCE($4, status),
            updated_by = $5,
            updated_at = NOW()
        WHERE id = $1
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(&req.connector_type)
    .bind(req.power_kw)
    .bind(&req.status)
    .bind(actor)
    .fetch_optional(pool)
    .await?;
    rec.ok_or_else(|| AppError::NotFound(format!("Charger {} not found", id)))
}

pub async fn delete_charger(pool: &PgPool, id: &str) -> Result<(), AppError> {
    let result = sqlx::query(r#"DELETE FROM "ev-platform".charger WHERE id = $1"#)
        .bind(id)
        .execute(pool)
        .await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(format!("Charger {} not found", id)));
    }
    Ok(())
}
