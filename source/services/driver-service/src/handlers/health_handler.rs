use actix_web::{web, HttpResponse};
use serde::Serialize;

use crate::dto::error_response::ApiResponse;

#[derive(Serialize)]
pub struct HealthData {
    pub status: String,
    pub database: String,
}

pub async fn health_check(pool: web::Data<sqlx::PgPool>) -> HttpResponse {
    match sqlx::query("SELECT 1").fetch_one(pool.get_ref()).await {
        Ok(_) => {
            let data = HealthData {
                status: "ok".into(),
                database: "connected".into(),
            };
            HttpResponse::Ok().json(ApiResponse {
                data: Some(data),
                error: None,
                meta: None,
            })
        }
        Err(_) => {
            let data = HealthData {
                status: "error".into(),
                database: "disconnected".into(),
            };
            HttpResponse::ServiceUnavailable().json(ApiResponse {
                data: Some(data),
                error: None,
                meta: None,
            })
        }
    }
}
