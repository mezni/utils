use crate::domain::chargers::models::{Charger, CreateChargerRequest, UpdateChargerRequest};
use crate::domain::repository::{apply_cursor_pagination, paginate};
use crate::utils::pagination::Cursor;
use sqlx::{PgPool, Postgres, QueryBuilder};

pub async fn create(
    pool: &PgPool,
    id: &str,
    station_id: &str,
    req: &CreateChargerRequest,
) -> Result<Charger, sqlx::Error> {
    sqlx::query_as::<_, Charger>(
        "INSERT INTO chargers (id, station_id, connector_type_id, power_kw, current_type) VALUES ($1, $2, $3, $4, $5) RETURNING id, station_id, connector_type_id, power_kw, current_type, status, created_at, updated_at"
    )
    .bind(id)
    .bind(station_id)
    .bind(&req.connector_type_id)
    .bind(req.power_kw)
    .bind(&req.current_type)
    .fetch_one(pool)
    .await
}

pub async fn list_by_station(
    pool: &PgPool,
    station_id: &str,
    cursor: Option<Cursor>,
    limit: i64,
) -> Result<(Vec<Charger>, Option<String>, bool), sqlx::Error> {
    let fetch_limit = limit + 1;
    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "SELECT id, station_id, connector_type_id, power_kw, current_type, status, created_at, updated_at FROM chargers"
    );

    qb.push(" WHERE station_id = ");
    qb.push_bind(station_id);

    apply_cursor_pagination(&mut qb, cursor, fetch_limit);

    let rows: Vec<Charger> = qb.build_query_as().fetch_all(pool).await?;
    Ok(paginate(rows, limit, |c: &Charger| (c.created_at, c.id.clone())))
}

pub async fn get_by_id(pool: &PgPool, id: &str) -> Result<Option<Charger>, sqlx::Error> {
    sqlx::query_as::<_, Charger>(
        "SELECT id, station_id, connector_type_id, power_kw, current_type, status, created_at, updated_at FROM chargers WHERE id = $1"
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}

pub async fn update(
    pool: &PgPool,
    id: &str,
    req: &UpdateChargerRequest,
) -> Result<Option<Charger>, sqlx::Error> {
    let now = chrono::Utc::now();
    sqlx::query_as::<_, Charger>(
        "UPDATE chargers SET status = COALESCE($2, status), power_kw = COALESCE($3, power_kw), current_type = COALESCE($4, current_type), updated_at = $5 WHERE id = $1 AND updated_at = $6 RETURNING id, station_id, connector_type_id, power_kw, current_type, status, created_at, updated_at"
    )
    .bind(id)
    .bind(&req.status)
    .bind(req.power_kw)
    .bind(&req.current_type)
    .bind(now)
    .bind(req.updated_at)
    .fetch_optional(pool)
    .await
}

pub async fn permanently_delete(pool: &PgPool, id: &str) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("DELETE FROM chargers WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}




