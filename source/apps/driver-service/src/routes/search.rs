use actix_web::{get, web, HttpResponse};
use crate::error::AppError;
use crate::AppState;

#[get("/api/stations/search")]
pub async fn search(
    state: web::Data<AppState>,
    query: web::Query<SearchQuery>,
) -> Result<HttpResponse, AppError> {
    let params = query.into_inner();

    if params.q.len() < 2 {
        return Err(AppError::BadRequest("query must be at least 2 characters".to_string()));
    }

    if let Some(ref ct) = params.connector_type {
        let valid = ["type2", "type3", "ccs", "chademo"];
        if !valid.contains(&ct.as_str()) {
            return Err(AppError::BadRequest(format!(
                "invalid connector_type: {}. Must be one of {:?}",
                ct, valid
            )));
        }
    }

    if params.limit > 100 {
        return Err(AppError::BadRequest("limit must not exceed 100".to_string()));
    }

    let stations = crate::db::search::search_stations(
        &state.pool,
        &params.q,
        params.connector_type.as_deref(),
        params.limit,
        params.offset,
    )
    .await?;

    Ok(HttpResponse::Ok().json(stations))
}

#[derive(serde::Deserialize)]
pub struct SearchQuery {
    pub q: String,
    pub connector_type: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 {
    20
}
