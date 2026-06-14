use actix_web::{web, App, HttpServer, middleware, HttpResponse};
use sqlx::PgPool;
use tracing_actix_web::TracingLogger;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

mod handlers;
mod domain;
mod error;
mod usecase;
mod jwt;
mod middleware_auth;

use handlers::{auth};
use services_shared::logging;

#[derive(OpenApi)]
#[openapi(
    paths(
        auth::register,
        auth::login,
        auth::verify,
        auth::profile
    ),
    components(
        schemas(
            domain::RegisterRequest,
            domain::LoginRequest,
            domain::AuthResponse,
            domain::UserProfile,
            domain::JwtClaims
        )
    ),
    info(
        title = "BorneMap Auth Service API",
        version = "1.0.0",
        description = "Centralized identity and access management service with JWT token generation"
    )
)]
struct ApiDoc;

#[actix_web::get("/api/v1/auth/health")]
async fn health_check() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({"status": "healthy"}))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize structured logging
    logging::init_platform_subscriber("auth_service");

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://platform_admin:platform_secure_password_2026@localhost:5432/platform_db".to_string());

    // Create database connection pool
    let pool = db_core::create_platform_pool(&database_url)
        .await
        .expect("Failed to create database pool");

    // Load JWT secret from environment
    let jwt_secret = std::env::var("AUTH_JWT_SECRET")
        .unwrap_or_else(|_| "dev-secret-key-change-in-production".to_string());

    tracing::info!("Auth service initializing on 0.0.0.0:3000");

    // Start the Actix-web server
    HttpServer::new(move || {
        let openapi = ApiDoc::openapi();

        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(web::Data::new(jwt_secret.clone()))
            .wrap(middleware::NormalizePath::trim())
            .wrap(TracingLogger::default())
            .service(health_check)
            .service(
                web::scope("/api/v1")
                    .service(auth::register)
                    .service(auth::login)
                    .service(auth::verify)
                    .service(auth::profile)
            )
            .service(
                SwaggerUi::new("/docs/swagger")
                    .url("/docs/openapi.json", openapi)
            )
    })
    .bind("0.0.0.0:3000")?
    .run()
    .await
}
