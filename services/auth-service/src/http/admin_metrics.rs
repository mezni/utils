use actix_web::{get, web, HttpResponse};
use serde::Serialize;

use super::error::map_app_error;
use crate::application::metrics::{GetUsersMetricsRequest, GetUsersMetricsUseCase};
use crate::http::middleware::admin_scope::AdminRequest;
use crate::infrastructure::pg_user_repo::PgUserRepository;
use crate::middleware::RequestId;
use crate::response::ApiResponse;
use bornemap_core::{MetricsRange, UsersGrowthPoint, UsersMetrics};
use bornemap_db::AppState;

fn new_user_repo(state: &web::Data<AppState>) -> PgUserRepository {
    PgUserRepository::new(state.db.clone())
}

#[derive(Serialize)]
pub struct UsersMetricsResponse {
    total: i64,
    growth: Vec<UsersGrowthPointResponse>,
}

#[derive(Serialize)]
pub struct UsersGrowthPointResponse {
    date: String,
    count: i64,
}

impl From<UsersMetrics> for UsersMetricsResponse {
    fn from(m: UsersMetrics) -> Self {
        Self {
            total: m.total,
            growth: m.growth.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<UsersGrowthPoint> for UsersGrowthPointResponse {
    fn from(p: UsersGrowthPoint) -> Self {
        Self {
            date: p.date.to_string(),
            count: p.count,
        }
    }
}

#[get("/api/v1/admin/metrics/users")]
pub async fn users_metrics(
    admin_request: AdminRequest<()>,
    state: web::Data<AppState>,
    query: web::Query<MetricsQuery>,
    request_id: RequestId,
) -> HttpResponse {
    let _current_user = admin_request.current_user; // User is already validated as ADMIN by middleware

    let range = match MetricsRange::from_str(&query.range) {
        Ok(r) => r,
        Err(err) => return map_app_error(err),
    };

    let repo = new_user_repo(&state);
    let use_case = GetUsersMetricsUseCase::new(repo);
    let request = GetUsersMetricsRequest { range };

    match use_case.execute(request).await {
        Ok(metrics) => {
            let response: UsersMetricsResponse = metrics.into();
            HttpResponse::Ok().json(ApiResponse::success(response, request_id.0))
        }
        Err(err) => map_app_error(err),
    }
}

#[derive(serde::Deserialize)]
pub struct MetricsQuery {
    range: String,
}
