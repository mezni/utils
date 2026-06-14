use actix_web::{web, HttpRequest, HttpResponse};
use sqlx::PgPool;
use crate::domain::{RegisterRequest, LoginRequest};
use crate::error::AuthServiceError;
use crate::middleware_auth::extract_token_from_header;

/// Register a new user account
#[utoipa::path(
    post,
    path = "/auth/register",
    request_body = RegisterRequest,
    responses(
        (status = 201, description = "User registered successfully"),
        (status = 409, description = "User already exists"),
        (status = 500, description = "Internal server error")
    )
)]
#[actix_web::post("/auth/register")]
pub async fn register(
    req: web::Json<RegisterRequest>,
    pool: web::Data<PgPool>,
) -> Result<HttpResponse, AuthServiceError> {
    let profile = crate::usecase::register(
        pool.get_ref(),
        req.into_inner(),
    )
    .await?;

    tracing::info!("User registered: {}", profile.id);

    Ok(HttpResponse::Created().json(profile))
}

/// Authenticate user and receive JWT token
#[utoipa::path(
    post,
    path = "/auth/login",
    request_body = LoginRequest,
    responses(
        (status = 200, description = "Authentication successful"),
        (status = 401, description = "Invalid credentials"),
        (status = 500, description = "Internal server error")
    )
)]
#[actix_web::post("/auth/login")]
pub async fn login(
    req: web::Json<LoginRequest>,
    pool: web::Data<PgPool>,
    jwt_secret: web::Data<String>,
) -> Result<HttpResponse, AuthServiceError> {
    let response = crate::usecase::login(
        pool.get_ref(),
        req.into_inner(),
        jwt_secret.as_ref(),
    )
    .await?;

    tracing::info!("User logged in: {}", response.user.id);

    Ok(HttpResponse::Ok().json(response))
}

/// Verify JWT token validity
#[utoipa::path(
    post,
    path = "/auth/verify",
    responses(
        (status = 200, description = "Token is valid"),
        (status = 401, description = "Token is invalid or expired"),
        (status = 500, description = "Internal server error")
    )
)]
#[actix_web::post("/auth/verify")]
pub async fn verify(
    req: HttpRequest,
    jwt_secret: web::Data<String>,
) -> Result<HttpResponse, AuthServiceError> {
    let token = extract_token_from_header(&req)?;
    let claims = crate::usecase::verify_token(&token, jwt_secret.as_ref()).await?;

    tracing::debug!("Token verified for user: {}", claims.sub);

    Ok(HttpResponse::Ok().json(claims))
}

/// Get authenticated user profile
#[utoipa::path(
    get,
    path = "/auth/profile",
    responses(
        (status = 200, description = "User profile retrieved"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "User not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[actix_web::get("/auth/profile")]
pub async fn profile(
    req: HttpRequest,
    pool: web::Data<PgPool>,
    jwt_secret: web::Data<String>,
) -> Result<HttpResponse, AuthServiceError> {
    let token = extract_token_from_header(&req)?;
    let claims = crate::usecase::verify_token(&token, jwt_secret.as_ref()).await?;

    let profile = crate::usecase::get_user_profile(pool.get_ref(), &claims.sub).await?;

    tracing::debug!("Profile retrieved for user: {}", profile.id);

    Ok(HttpResponse::Ok().json(profile))
}
