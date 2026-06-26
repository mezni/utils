use actix_web::{HttpResponse, get};

#[get("/health/live")]
pub async fn live() -> HttpResponse {
    HttpResponse::Ok().finish()
}

#[get("/health/ready")]
pub async fn ready() -> HttpResponse {
    HttpResponse::Ok().finish()
}
