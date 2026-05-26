use crate::domain::repository::{apply_cursor_pagination, paginate, SoftDeleteFilter, TestFilter};
use crate::domain::users::models::{UpdateUserRequest, User};
use crate::utils::pagination::Cursor;
use chrono::Utc;
use sqlx::{PgPool, QueryBuilder, Postgres};

pub async fn create(
    pool: &PgPool,
    id: &str,
    email: &str,
    username: &str,
    password_hash: &str,
    role: &str,
    is_test: bool,
) -> Result<User, sqlx::Error> {
    sqlx::query_as::<_, User>(
        "INSERT INTO users (id, email, username, password_hash, role, is_test) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *"
    )
    .bind(id)
    .bind(email)
    .bind(username)
    .bind(password_hash)
    .bind(role)
    .bind(is_test)
    .fetch_one(pool)
    .await
}

pub async fn list(
    pool: &PgPool,
    cursor: Option<Cursor>,
    limit: i64,
    include_test: bool,
) -> Result<(Vec<User>, Option<String>, bool), sqlx::Error> {
    let fetch_limit = limit + 1;
    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "SELECT id, email, username, password_hash, role, is_test, created_at, updated_at, deleted_at FROM users"
    );

    qb.push(SoftDeleteFilter::where_not_deleted());
    qb.push(TestFilter::and_include_test(include_test));
    apply_cursor_pagination(&mut qb, cursor, fetch_limit);

    let rows: Vec<User> = qb.build_query_as().fetch_all(pool).await?;
    Ok(paginate(rows, limit, |u: &User| (u.created_at, u.id.clone())))
}

pub async fn get_by_id(pool: &PgPool, id: &str) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as::<_, User>(
        "SELECT id, email, username, password_hash, role, is_test, created_at, updated_at, deleted_at FROM users WHERE id = $1 AND deleted_at IS NULL"
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}

pub async fn update(
    pool: &PgPool,
    id: &str,
    req: &UpdateUserRequest,
) -> Result<Option<User>, sqlx::Error> {
    let now = Utc::now();
    sqlx::query_as::<_, User>(
        "UPDATE users SET email = COALESCE($2, email), username = COALESCE($3, username), updated_at = $4 WHERE id = $1 AND updated_at = $5 AND deleted_at IS NULL RETURNING *"
    )
    .bind(id)
    .bind(&req.email)
    .bind(&req.username)
    .bind(now)
    .bind(req.updated_at)
    .fetch_optional(pool)
    .await
}

pub async fn soft_delete(pool: &PgPool, id: &str) -> Result<Option<User>, sqlx::Error> {
    let now = Utc::now();
    sqlx::query_as::<_, User>(
        "UPDATE users SET deleted_at = $2, updated_at = $2 WHERE id = $1 AND deleted_at IS NULL RETURNING *"
    )
    .bind(id)
    .bind(now)
    .fetch_optional(pool)
    .await
}

pub async fn exists_by_email(pool: &PgPool, email: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM users WHERE email = $1 AND deleted_at IS NULL)"
    )
    .bind(email)
    .fetch_one(pool)
    .await
}

pub async fn exists_by_username(pool: &PgPool, username: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM users WHERE username = $1 AND deleted_at IS NULL)"
    )
    .bind(username)
    .fetch_one(pool)
    .await
}

pub async fn get_by_email(pool: &PgPool, email: &str) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as::<_, User>(
        "SELECT id, email, username, password_hash, role, is_test, created_at, updated_at, deleted_at FROM users WHERE email = $1 AND deleted_at IS NULL"
    )
    .bind(email)
    .fetch_optional(pool)
    .await
}
