use crate::error::AppError;
use crate::models::{CreateStationRequest, StationResponse, UpdateStationRequest};
use sqlx::PgPool;

pub async fn create_station(
    pool: &PgPool,
    req: CreateStationRequest,
    actor: &str,
) -> Result<StationResponse, AppError> {
    let id = ev_core::generate_id("STN", 3);
    let rec = sqlx::query_as::<_, StationResponse>(
        r#"
        INSERT INTO "ev-platform".station (id, partner_id, name, address, latitude, longitude, created_by, updated_by)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING *
        "#,
    )
    .bind(&id)
    .bind(&req.partner_id)
    .bind(&req.name)
    .bind(&req.address)
    .bind(req.latitude)
    .bind(req.longitude)
    .bind(actor)
    .bind(actor)
    .fetch_one(pool)
    .await
    .map_err(|e| {
        if let sqlx::Error::Database(ref db_err) = e
            && let Some(code) = db_err.code()
            && code.as_ref() == "23503"
        {
            return AppError::NotFound(format!("Partner {} not found", req.partner_id));
        }
        AppError::from(e)
    })?;
    Ok(rec)
}

pub async fn get_station(pool: &PgPool, id: &str) -> Result<StationResponse, AppError> {
    let rec = sqlx::query_as::<_, StationResponse>(
        r#"SELECT * FROM "ev-platform".station WHERE id = $1"#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    rec.ok_or_else(|| AppError::NotFound(format!("Station {} not found", id)))
}

pub async fn list_stations(
    pool: &PgPool,
    partner_id: Option<&str>,
    page: u32,
    page_size: u32,
) -> Result<ev_db::Paginated<StationResponse>, AppError> {
    let offset = ((page - 1) * page_size) as i64;
    let limit = page_size as i64;

    let (total, data) = if let Some(pid) = partner_id {
        let total: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM "ev-platform".station WHERE partner_id = $1"#,
        )
        .bind(pid)
        .fetch_one(pool)
        .await?;

        let data = sqlx::query_as::<_, StationResponse>(
            r#"SELECT * FROM "ev-platform".station WHERE partner_id = $3 ORDER BY created_at DESC LIMIT $1 OFFSET $2"#,
        )
        .bind(limit)
        .bind(offset)
        .bind(pid)
        .fetch_all(pool)
        .await?;

        (total.0, data)
    } else {
        let total: (i64,) = sqlx::query_as(r#"SELECT COUNT(*) FROM "ev-platform".station"#)
            .fetch_one(pool)
            .await?;

        let data = sqlx::query_as::<_, StationResponse>(
            r#"SELECT * FROM "ev-platform".station ORDER BY created_at DESC LIMIT $1 OFFSET $2"#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?;

        (total.0, data)
    };

    Ok(ev_db::Paginated::new(data, total as u64, page, page_size))
}

pub async fn update_station(
    pool: &PgPool,
    id: &str,
    req: UpdateStationRequest,
    actor: &str,
) -> Result<StationResponse, AppError> {
    let rec = sqlx::query_as::<_, StationResponse>(
        r#"
        UPDATE "ev-platform".station
        SET name = COALESCE($2, name),
            address = COALESCE($3, address),
            latitude = COALESCE($4, latitude),
            longitude = COALESCE($5, longitude),
            updated_by = $6,
            updated_at = NOW()
        WHERE id = $1
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(&req.name)
    .bind(&req.address)
    .bind(req.latitude)
    .bind(req.longitude)
    .bind(actor)
    .fetch_optional(pool)
    .await?;
    rec.ok_or_else(|| AppError::NotFound(format!("Station {} not found", id)))
}

pub async fn delete_station(pool: &PgPool, id: &str) -> Result<(), AppError> {
    let result = sqlx::query(r#"DELETE FROM "ev-platform".station WHERE id = $1"#)
        .bind(id)
        .execute(pool)
        .await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(format!("Station {} not found", id)));
    }
    Ok(())
}
