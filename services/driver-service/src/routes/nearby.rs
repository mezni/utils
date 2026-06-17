use actix_web::{web, HttpResponse};
use sqlx::PgPool;

use crate::handler::import::import_handler;
use crate::handler::nearby::nearby_handler;

pub fn setup_routes(cfg: &mut web::ServiceConfig, pool: web::Data<PgPool>) {
    cfg.service(
        web::scope("/api/v1/nearby")
            .app_data(pool.clone())
            .route("", web::get().to(nearby_handler))
    )
    .service(
        web::scope("/api/v1")
            .app_data(pool.clone())
            .route("/import", web::post().to(import_handler))
    );
}
