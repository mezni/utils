use crate::domain::repository::{apply_cursor_pagination, paginate, SoftDeleteFilter, TestFilter};
use crate::domain::stations::models::{CreateStationRequest, Station, UpdateStationRequest};
use crate::utils::pagination::Cursor;
use chrono::Utc;
use sqlx::{PgPool, QueryBuilder, Postgres};

pub async fn create(
    pool: &PgPool,
    id: &str,
    req: &CreateStationRequest,
    is_test: bool,
) -> Result<Station, sqlx::Error> {
    sqlx::query_as::<_, Station>(
        "INSERT INTO stations (id, owner_id, name, address, city, coordinates, is_test) VALUES ($1, $2, $3, $4, $5, ST_SetSRID(ST_MakePoint($6, $7), 4326), $8) RETURNING id, owner_id, name, address, city, ST_X(coordinates::geometry) AS longitude, ST_Y(coordinates::geometry) AS latitude, is_operational, is_test, created_at, updated_at, deleted_at"
    )
    .bind(id)
    .bind(&req.owner_id)
    .bind(&req.name)
    .bind(&req.address)
    .bind(&req.city)
    .bind(req.longitude)
    .bind(req.latitude)
    .bind(is_test)
    .fetch_one(pool)
    .await
}

pub async fn list(
    pool: &PgPool,
    cursor: Option<Cursor>,
    limit: i64,
    include_test: bool,
    owner_filter: Option<&str>,
) -> Result<(Vec<Station>, Option<String>, bool), sqlx::Error> {
    let fetch_limit = limit + 1;
    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "SELECT id, owner_id, name, address, city, ST_X(coordinates::geometry) AS longitude, ST_Y(coordinates::geometry) AS latitude, is_operational, is_test, created_at, updated_at, deleted_at FROM stations"
    );

    qb.push(SoftDeleteFilter::where_not_deleted());
    qb.push(TestFilter::and_include_test(include_test));

    if let Some(owner_id) = owner_filter {
        qb.push(" AND owner_id = ");
        qb.push_bind(owner_id);
    }

    apply_cursor_pagination(&mut qb, cursor, fetch_limit);

    let rows: Vec<Station> = qb.build_query_as().fetch_all(pool).await?;
    Ok(paginate(rows, limit, |s: &Station| (s.created_at, s.id.clone())))
}

pub async fn get_by_id(pool: &PgPool, id: &str) -> Result<Option<Station>, sqlx::Error> {
    sqlx::query_as::<_, Station>(
        "SELECT id, owner_id, name, address, city, ST_X(coordinates::geometry) AS longitude, ST_Y(coordinates::geometry) AS latitude, is_operational, is_test, created_at, updated_at, deleted_at FROM stations WHERE id = $1 AND deleted_at IS NULL"
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}

pub async fn update(
    pool: &PgPool,
    id: &str,
    req: &UpdateStationRequest,
) -> Result<Option<Station>, sqlx::Error> {
    let now = Utc::now();

    let update_coords = req.longitude.is_some() || req.latitude.is_some();
    let lng = req.longitude.unwrap_or(0.0);
    let lat = req.latitude.unwrap_or(0.0);

    let query = if update_coords {
        "UPDATE stations SET name = COALESCE($2, name), address = COALESCE($3, address), city = COALESCE($4, city), coordinates = ST_SetSRID(ST_MakePoint($5, $6), 4326), is_operational = COALESCE($7, is_operational), updated_at = $8 WHERE id = $1 AND updated_at = $9 AND deleted_at IS NULL RETURNING id, owner_id, name, address, city, ST_X(coordinates::geometry) AS longitude, ST_Y(coordinates::geometry) AS latitude, is_operational, is_test, created_at, updated_at, deleted_at"
    } else {
        "UPDATE stations SET name = COALESCE($2, name), address = COALESCE($3, address), city = COALESCE($4, city), is_operational = COALESCE($7, is_operational), updated_at = $8 WHERE id = $1 AND updated_at = $9 AND deleted_at IS NULL RETURNING id, owner_id, name, address, city, ST_X(coordinates::geometry) AS longitude, ST_Y(coordinates::geometry) AS latitude, is_operational, is_test, created_at, updated_at, deleted_at"
    };

    sqlx::query_as::<_, Station>(query)
        .bind(id)
        .bind(&req.name)
        .bind(&req.address)
        .bind(&req.city)
        .bind(lng)
        .bind(lat)
        .bind(req.is_operational)
        .bind(now)
        .bind(req.updated_at)
        .fetch_optional(pool)
        .await
}

pub async fn soft_delete(pool: &PgPool, id: &str) -> Result<Option<Station>, sqlx::Error> {
    let now = Utc::now();
    sqlx::query_as::<_, Station>(
        "UPDATE stations SET deleted_at = $2, updated_at = $2 WHERE id = $1 AND deleted_at IS NULL RETURNING id, owner_id, name, address, city, ST_X(coordinates::geometry) AS longitude, ST_Y(coordinates::geometry) AS latitude, is_operational, is_test, created_at, updated_at, deleted_at"
    )
    .bind(id)
    .bind(now)
    .fetch_optional(pool)
    .await
}

pub async fn get_owner_id(pool: &PgPool, id: &str) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT owner_id FROM stations WHERE id = $1 AND deleted_at IS NULL"
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}
