use crate::domain::partners::models::{CreatePartnerRequest, PartnerProfile, UpdatePartnerRequest};
use crate::domain::repository::{apply_cursor_pagination, paginate, SoftDeleteFilter, TestFilter};
use crate::utils::pagination::Cursor;
use chrono::Utc;
use sqlx::{PgPool, QueryBuilder, Postgres};

pub async fn create(
    pool: &PgPool,
    id: &str,
    req: &CreatePartnerRequest,
    is_test: bool,
) -> Result<PartnerProfile, sqlx::Error> {
    sqlx::query_as::<_, PartnerProfile>(
        "INSERT INTO partner_profiles (id, user_id, classification, display_name, tax_id, contact_phone, is_test) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *"
    )
    .bind(id)
    .bind(&req.user_id)
    .bind(&req.classification)
    .bind(&req.display_name)
    .bind(&req.tax_id)
    .bind(&req.contact_phone)
    .bind(is_test)
    .fetch_one(pool)
    .await
}

pub async fn list(
    pool: &PgPool,
    cursor: Option<Cursor>,
    limit: i64,
    include_test: bool,
) -> Result<(Vec<PartnerProfile>, Option<String>, bool), sqlx::Error> {
    let fetch_limit = limit + 1;
    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "SELECT id, user_id, classification, display_name, tax_id, contact_phone, is_test, created_at, updated_at, deleted_at FROM partner_profiles"
    );

    qb.push(SoftDeleteFilter::where_not_deleted());
    qb.push(TestFilter::and_include_test(include_test));
    apply_cursor_pagination(&mut qb, cursor, fetch_limit);

    let rows: Vec<PartnerProfile> = qb.build_query_as().fetch_all(pool).await?;
    Ok(paginate(rows, limit, |p: &PartnerProfile| (p.created_at, p.id.clone())))
}

pub async fn get_by_id(pool: &PgPool, id: &str) -> Result<Option<PartnerProfile>, sqlx::Error> {
    sqlx::query_as::<_, PartnerProfile>(
        "SELECT id, user_id, classification, display_name, tax_id, contact_phone, is_test, created_at, updated_at, deleted_at FROM partner_profiles WHERE id = $1 AND deleted_at IS NULL"
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}

pub async fn update(
    pool: &PgPool,
    id: &str,
    req: &UpdatePartnerRequest,
) -> Result<Option<PartnerProfile>, sqlx::Error> {
    let now = Utc::now();
    sqlx::query_as::<_, PartnerProfile>(
        "UPDATE partner_profiles SET classification = COALESCE($2, classification), display_name = COALESCE($3, display_name), tax_id = COALESCE($4, tax_id), contact_phone = COALESCE($5, contact_phone), updated_at = $6 WHERE id = $1 AND updated_at = $7 AND deleted_at IS NULL RETURNING *"
    )
    .bind(id)
    .bind(&req.classification)
    .bind(&req.display_name)
    .bind(&req.tax_id)
    .bind(&req.contact_phone)
    .bind(now)
    .bind(req.updated_at)
    .fetch_optional(pool)
    .await
}

pub async fn soft_delete(pool: &PgPool, id: &str) -> Result<Option<PartnerProfile>, sqlx::Error> {
    let now = Utc::now();
    sqlx::query_as::<_, PartnerProfile>(
        "UPDATE partner_profiles SET deleted_at = $2, updated_at = $2 WHERE id = $1 AND deleted_at IS NULL RETURNING *"
    )
    .bind(id)
    .bind(now)
    .fetch_optional(pool)
    .await
}

pub async fn exists_by_user_id(pool: &PgPool, user_id: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM partner_profiles WHERE user_id = $1 AND deleted_at IS NULL)"
    )
    .bind(user_id)
    .fetch_one(pool)
    .await
}
