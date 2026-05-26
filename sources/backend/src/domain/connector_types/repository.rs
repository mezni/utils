use crate::domain::connector_types::models::{ConnectorType, CreateConnectorTypeRequest, UpdateConnectorTypeRequest};
use crate::domain::repository::{apply_cursor_pagination, paginate, SoftDeleteFilter, TestFilter};
use crate::utils::pagination::Cursor;
use chrono::Utc;
use sqlx::{PgPool, QueryBuilder, Postgres};

pub async fn create(
    pool: &PgPool,
    id: &str,
    req: &CreateConnectorTypeRequest,
    is_test: bool,
) -> Result<ConnectorType, sqlx::Error> {
    sqlx::query_as::<_, ConnectorType>(
        "INSERT INTO connector_types (id, name, description, is_test) VALUES ($1, $2, $3, $4) RETURNING id, name, description, is_test, created_at, updated_at, deleted_at"
    )
    .bind(id)
    .bind(&req.name)
    .bind(&req.description)
    .bind(is_test)
    .fetch_one(pool)
    .await
}

pub async fn list(
    pool: &PgPool,
    cursor: Option<Cursor>,
    limit: i64,
    include_test: bool,
) -> Result<(Vec<ConnectorType>, Option<String>, bool), sqlx::Error> {
    let fetch_limit = limit + 1;
    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "SELECT id, name, description, is_test, created_at, updated_at, deleted_at FROM connector_types"
    );

    qb.push(SoftDeleteFilter::where_not_deleted());
    qb.push(TestFilter::and_include_test(include_test));

    apply_cursor_pagination(&mut qb, cursor, fetch_limit);

    let rows: Vec<ConnectorType> = qb.build_query_as().fetch_all(pool).await?;
    Ok(paginate(rows, limit, |c: &ConnectorType| (c.created_at, c.id.clone())))
}

pub async fn get_by_id(pool: &PgPool, id: &str) -> Result<Option<ConnectorType>, sqlx::Error> {
    sqlx::query_as::<_, ConnectorType>(
        "SELECT id, name, description, is_test, created_at, updated_at, deleted_at FROM connector_types WHERE id = $1 AND deleted_at IS NULL"
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}

pub async fn update(
    pool: &PgPool,
    id: &str,
    req: &UpdateConnectorTypeRequest,
) -> Result<Option<ConnectorType>, sqlx::Error> {
    let now = Utc::now();
    sqlx::query_as::<_, ConnectorType>(
        "UPDATE connector_types SET name = COALESCE($2, name), description = COALESCE($3, description), updated_at = $4 WHERE id = $1 AND updated_at = $5 AND deleted_at IS NULL RETURNING id, name, description, is_test, created_at, updated_at, deleted_at"
    )
    .bind(id)
    .bind(&req.name)
    .bind(&req.description)
    .bind(now)
    .bind(req.updated_at)
    .fetch_optional(pool)
    .await
}

pub async fn soft_delete(pool: &PgPool, id: &str) -> Result<Option<ConnectorType>, sqlx::Error> {
    let now = Utc::now();
    sqlx::query_as::<_, ConnectorType>(
        "UPDATE connector_types SET deleted_at = $2, updated_at = $2 WHERE id = $1 AND deleted_at IS NULL RETURNING id, name, description, is_test, created_at, updated_at, deleted_at"
    )
    .bind(id)
    .bind(now)
    .fetch_optional(pool)
    .await
}

pub async fn exists_by_name(pool: &PgPool, name: &str) -> Result<bool, sqlx::Error> {
    let row: Option<bool> = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM connector_types WHERE name = $1 AND deleted_at IS NULL)"
    )
    .bind(name)
    .fetch_optional(pool)
    .await?;
    Ok(row.unwrap_or(false))
}

pub async fn is_referenced_by_charger(pool: &PgPool, id: &str) -> Result<bool, sqlx::Error> {
    let row: Option<bool> = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM chargers WHERE connector_type_id = $1 LIMIT 1)"
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.unwrap_or(false))
}
