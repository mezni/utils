use sqlx::PgPool;
use nanoid::nanoid;

use crate::models::partner::{CreatePartnerRequest, Partner, UpdatePartnerRequest};
use crate::error::AppError;

fn generate_partner_id() -> String {
    format!("OPR-{}", nanoid!(12))
}

pub async fn insert(pool: &PgPool, req: &CreatePartnerRequest) -> Result<Partner, AppError> {
    let id = generate_partner_id();
    let partner = sqlx::query_as::<_, Partner>(
        r#"
        INSERT INTO inventory.partners (id, name, network_type, support_phone, support_email)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, name, network_type, support_phone, support_email, is_verified,
                  deleted_at, created_at, updated_at
        "#,
    )
    .bind(&id)
    .bind(&req.name)
    .bind(&req.network_type)
    .bind(&req.support_phone)
    .bind(&req.support_email)
    .fetch_one(pool)
    .await?;
    Ok(partner)
}

pub async fn select_all(pool: &PgPool) -> Result<Vec<Partner>, AppError> {
    let partners = sqlx::query_as::<_, Partner>(
        r#"
        SELECT id, name, network_type, support_phone, support_email, is_verified,
               deleted_at, created_at, updated_at
        FROM inventory.partners
        WHERE deleted_at IS NULL
        ORDER BY created_at DESC
        "#,
    )
    .fetch_all(pool)
    .await?;
    Ok(partners)
}

pub async fn select_by_id(pool: &PgPool, id: &str) -> Result<Partner, AppError> {
    let partner = sqlx::query_as::<_, Partner>(
        r#"
        SELECT id, name, network_type, support_phone, support_email, is_verified,
               deleted_at, created_at, updated_at
        FROM inventory.partners
        WHERE id = $1 AND deleted_at IS NULL
        "#,
    )
    .bind(id)
    .fetch_one(pool)
    .await?;
    Ok(partner)
}

pub async fn update(pool: &PgPool, id: &str, req: &UpdatePartnerRequest) -> Result<Partner, AppError> {
    if fields_present(req).is_empty() {
        return Err(AppError::BadRequest("No valid fields provided for update".into()));
    }

    let current = select_by_id(pool, id).await?;
    let name = req.name.as_deref().unwrap_or(&current.name);
    let network_type = req.network_type.as_deref().unwrap_or(&current.network_type);
    let support_phone = req.support_phone.clone().or(current.support_phone);
    let support_email = req.support_email.clone().or(current.support_email);
    let is_verified = req.is_verified.unwrap_or(current.is_verified);

    let partner = sqlx::query_as::<_, Partner>(
        r#"
        UPDATE inventory.partners
        SET name = $2, network_type = $3, support_phone = $4, support_email = $5,
            is_verified = $6, updated_at = NOW()
        WHERE id = $1 AND deleted_at IS NULL
        RETURNING id, name, network_type, support_phone, support_email, is_verified,
                  deleted_at, created_at, updated_at
        "#,
    )
    .bind(id)
    .bind(name)
    .bind(network_type)
    .bind(support_phone)
    .bind(support_email)
    .bind(is_verified)
    .fetch_one(pool)
    .await?;
    Ok(partner)
}

pub async fn soft_delete(pool: &PgPool, id: &str) -> Result<(), AppError> {
    let result = sqlx::query(
        r#"UPDATE inventory.partners SET deleted_at = NOW() WHERE id = $1 AND deleted_at IS NULL"#,
    )
    .bind(id)
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("Partner not found".into()));
    }
    Ok(())
}

fn fields_present(req: &UpdatePartnerRequest) -> Vec<&str> {
    let mut fields = Vec::new();
    if req.name.is_some() { fields.push("name"); }
    if req.network_type.is_some() { fields.push("network_type"); }
    if req.support_phone.is_some() { fields.push("support_phone"); }
    if req.support_email.is_some() { fields.push("support_email"); }
    if req.is_verified.is_some() { fields.push("is_verified"); }
    fields
}
