use actix_web::{web, App, HttpServer, middleware, HttpResponse};
use sqlx::PgPool;
use tracing_actix_web::TracingLogger;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

mod handlers;
mod domain;
mod error;
mod middleware;

use handlers::proximity;
use services_shared::logging;
use middleware::GatewayAwareMiddleware;

#[derive(OpenApi)]
#[openapi(
    paths(
        proximity::get_nearby_stations
    ),
    components(
        schemas(
            services_shared::domain::ChargerDto,
            services_shared::domain::NearbyStationRow,
            domain::ProximityQuery,
            domain::ProximityResponse
        )
    ),
    info(
        title = "BorneMap Driver Service API",
        version = "1.0.0",
        description = "High-performance geospatial proximity lookup service"
    )
)]
struct ApiDoc;

#[actix_web::get("/api/v1/driver/health")]
async fn health_check() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({"status": "healthy"}))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize structured logging
    logging::init_platform_subscriber("driver_service");

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://platform_admin:platform_secure_password_2026@localhost:5432/platform_db".to_string());

    // Create database connection pool
    let pool = db_core::create_platform_pool(&database_url)
        .await
        .expect("Failed to create database pool");

    tracing::info!("Driver service initializing on 0.0.0.0:3001");

    // Start the Actix-web server
    HttpServer::new(move || {
        let openapi = ApiDoc::openapi();

        App::new()
            .app_data(web::Data::new(pool.clone()))
            .wrap(middleware::NormalizePath::trim())
            .wrap(TracingLogger::default())
            .wrap(GatewayAwareMiddleware)
            .service(health_check)
            .service(
                web::scope("/api/v1")
                    .service(proximity::get_nearby_stations)
            )
            .service(
                SwaggerUi::new("/docs/swagger")
                    .url("/docs/openapi.json", openapi)
            )
    })
    .bind("0.0.0.0:3001")?
    .run()
    .await
}
