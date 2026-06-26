use actix_web::{HttpResponse, get, web};
use bornemap_db::AppState;

#[get("/health/live")]
pub async fn live() -> HttpResponse {
    HttpResponse::Ok().finish()
}

#[get("/health/ready")]
pub async fn ready(state: web::Data<AppState>) -> HttpResponse {
    match sqlx::query("SELECT 1").execute(&state.db).await {
        Ok(_) => HttpResponse::Ok().finish(),
        Err(_) => HttpResponse::ServiceUnavailable().finish(),
    }
}
