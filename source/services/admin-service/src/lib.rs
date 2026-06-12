pub mod routes;
pub mod middleware;

use sqlx::PgPool;
use std::time::Instant;

pub struct AppState {
    pub platform_db: PgPool,
    pub analytics_db: PgPool,
    pub startup_time: Instant,
    pub service_name: String,
}
