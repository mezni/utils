pub mod health;
pub mod nearby;
pub mod ready;

use actix_web::web;
use sqlx::PgPool;

use crate::middleware::rate_limit::RateLimiter;
use std::sync::Mutex;

pub fn setup_routes(cfg: &mut web::ServiceConfig, pool: web::Data<PgPool>) {
    let rate_limiter = web::Data::new(Mutex::new(RateLimiter::new(100, 60)));

    cfg.app_data(pool.clone())
       .app_data(rate_limiter.clone())
       .service(health::health)
       .service(ready::ready)
       .configure(|cfg| nearby::setup_routes(cfg, pool));
}
