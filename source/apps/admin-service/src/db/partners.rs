use crate::error::AppError;
use crate::models::{CreatePartnerRequest, PartnerResponse, UpdatePartnerRequest};
use sqlx::PgPool;

pub async fn create_partner(
    pool: &PgPool,
    req: CreatePartnerRequest,
    actor: &str,
) -> Result<PartnerResponse, AppError> {
    let id = ev_core::generate_id("PRT", 3);
    let rec = sqlx::query_as::<_, PartnerResponse>(
        r#"
        INSERT INTO "ev-platform".partner (id, name, type, is_verified, is_live, is_active, created_by, updated_by)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING *
        "#,
    )
    .bind(&id)
    .bind(&req.name)
    .bind(&req.partner_type)
    .bind(req.is_verified)
    .bind(req.is_live)
    .bind(req.is_active)
    .bind(actor)
    .bind(actor)
    .fetch_one(pool)
    .await?;
    Ok(rec)
}

pub async fn get_partner(pool: &PgPool, id: &str) -> Result<PartnerResponse, AppError> {
    let rec = sqlx::query_as::<_, PartnerResponse>(
        r#"SELECT * FROM "ev-platform".partner WHERE id = $1"#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    rec.ok_or_else(|| AppError::NotFound(format!("Partner {} not found", id)))
}

pub async fn list_partners(
    pool: &PgPool,
    page: u32,
    page_size: u32,
) -> Result<ev_db::Paginated<PartnerResponse>, AppError> {
    let offset = ((page - 1) * page_size) as i64;
    let limit = page_size as i64;

    let total: (i64,) = sqlx::query_as(r#"SELECT COUNT(*) FROM "ev-platform".partner"#)
        .fetch_one(pool)
        .await?;

    let data = sqlx::query_as::<_, PartnerResponse>(
        r#"SELECT * FROM "ev-platform".partner ORDER BY created_at DESC LIMIT $1 OFFSET $2"#,
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    Ok(ev_db::Paginated::new(data, total.0 as u64, page, page_size))
}

pub async fn update_partner(
    pool: &PgPool,
    id: &str,
    req: UpdatePartnerRequest,
    actor: &str,
) -> Result<PartnerResponse, AppError> {
    let rec = sqlx::query_as::<_, PartnerResponse>(
        r#"
        UPDATE "ev-platform".partner
        SET name = COALESCE($2, name),
            type = COALESCE($3, type),
            is_verified = COALESCE($4, is_verified),
            is_live = COALESCE($5, is_live),
            is_active = COALESCE($6, is_active),
            updated_by = $7,
            updated_at = NOW()
        WHERE id = $1
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(&req.name)
    .bind(&req.partner_type)
    .bind(req.is_verified)
    .bind(req.is_live)
    .bind(req.is_active)
    .bind(actor)
    .fetch_optional(pool)
    .await?;
    rec.ok_or_else(|| AppError::NotFound(format!("Partner {} not found", id)))
}

pub async fn delete_partner(pool: &PgPool, id: &str, actor: &str) -> Result<(), AppError> {
    let result = sqlx::query(
        r#"
        UPDATE "ev-platform".partner
        SET is_active = false, updated_by = $2, updated_at = NOW()
        WHERE id = $1 AND is_active = true
        "#,
    )
    .bind(id)
    .bind(actor)
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        let exists = sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS(SELECT 1 FROM "ev-platform".partner WHERE id = $1)"#,
        )
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap_or(false);
        if !exists {
            return Err(AppError::NotFound(format!("Partner {} not found", id)));
        }
    }
    Ok(())
}
