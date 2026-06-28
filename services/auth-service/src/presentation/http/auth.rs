use actix_web::{web, HttpResponse};
use sqlx::PgPool;

use common_errors::AppError;

use crate::application::auth::{AuthUseCases, LoginRequest as AppLoginRequest, RegisterRequest as AppRegisterRequest};
use crate::infrastructure::jwt_service::JwtService;
use crate::infrastructure::postgres_repo::PostgresAccountRepository;
use crate::presentation::http::dto::{AuthResponse, LoginRequest, RegisterRequest};

pub async fn register(
    pool: web::Data<PgPool>,
    body: web::Json<RegisterRequest>,
) -> Result<HttpResponse, AppError> {
    let repo = PostgresAccountRepository::new(pool.get_ref().clone());
    let jwt_secret = std::env::var("JWT_SECRET")
        .unwrap_or_else(|_| "dev-secret-change-in-production".to_string());
    let jwt = JwtService::new(jwt_secret);

    let uc = AuthUseCases::new(repo, jwt);

    let result = uc
        .register(AppRegisterRequest {
            email: body.email.clone(),
            password: body.password.clone(),
            role: body.role.clone().unwrap_or_else(|| "driver".to_string()),
        })
        .await?;

    Ok(HttpResponse::Created().json(AuthResponse {
        token: result.token,
        email: result.email,
        role: result.role,
    }))
}

pub async fn login(
    pool: web::Data<PgPool>,
    body: web::Json<LoginRequest>,
) -> Result<HttpResponse, AppError> {
    let repo = PostgresAccountRepository::new(pool.get_ref().clone());
    let jwt_secret = std::env::var("JWT_SECRET")
        .unwrap_or_else(|_| "dev-secret-change-in-production".to_string());
    let jwt = JwtService::new(jwt_secret);

    let uc = AuthUseCases::new(repo, jwt);

    let result = uc
        .login(AppLoginRequest {
            email: body.email.clone(),
            password: body.password.clone(),
        })
        .await?;

    Ok(HttpResponse::Ok().json(AuthResponse {
        token: result.token,
        email: result.email,
        role: result.role,
    }))
}
