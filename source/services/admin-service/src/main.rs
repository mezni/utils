use actix_web::{web, App, HttpServer, middleware, HttpResponse};
use sqlx::PgPool;
use tracing_actix_web::TracingLogger;

mod handlers;
mod domain;
mod error;
mod usecase;
mod middleware;

use handlers::{partners, stations, chargers};
use services_shared::logging;
use middleware::GatewayAwareMiddleware;

#[derive(OpenApi)]
#[openapi(
    paths(
        partners::create_partner,
        partners::get_partner,
        stations::create_station,
        stations::update_station_live_status,
        chargers::create_charger
    ),
    components(
        schemas(
            services_shared::domain::PartnerDto,
            services_shared::domain::StationDto,
            services_shared::domain::ChargerDetailDto,
            domain::CreatePartnerRequest,
            domain::CreateStationRequest,
            domain::CreateChargerRequest,
            domain::UpdateStationLiveRequest,
            domain::CreateResponse
        )
    ),
    info(
        title = "BorneMap Admin Service API",
        version = "1.0.0",
        description = "Administrative asset management and configuration service"
    )
)]
struct ApiDoc;

#[actix_web::get("/api/v1/admin/health")]
async fn health_check() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({"status": "healthy"}))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize structured logging
    logging::init_platform_subscriber("admin_service");

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://platform_admin:platform_secure_password_2026@localhost:5432/platform_db".to_string());

    // Create database connection pool
    let pool = db_core::create_platform_pool(&database_url)
        .await
        .expect("Failed to create database pool");

    tracing::info!("Admin service initializing on 0.0.0.0:3002");

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
                    .service(partners::create_partner)
                    .service(partners::get_partner)
                    .service(stations::create_station)
                    .service(stations::update_station_live_status)
                    .service(chargers::create_charger)
            )
            .service(
                SwaggerUi::new("/docs/swagger")
                    .url("/docs/openapi.json", openapi)
            )
    })
    .bind("0.0.0.0:3002")?
    .run()
    .await
}
