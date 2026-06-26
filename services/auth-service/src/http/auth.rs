use actix_web::{HttpResponse, post, web};
use tracing::{error, info};

use super::dto::{
    AuthResponse as AuthDto, LoginRequest, RefreshTokenRequest, RegisterRequest, RegisterResponse,
};
use super::error::{map_app_error, map_validation_errors};
use crate::application::login::{LoginRequest as LoginReq, LoginUseCase};
use crate::application::refresh::{RefreshRequest as RefreshReq, RefreshUseCase};
use crate::application::register::{RegisterRequest as RegisterReq, RegisterUseCase};
use crate::infrastructure::jwt::JwtService;
use crate::infrastructure::pg_session_repo::PgSessionRepository;
use crate::infrastructure::pg_user_repo::PgUserRepository;
use crate::middleware::RequestId;
use crate::response::ApiResponse;
use crate::validation::Validator;
use crate::validation::register::RegisterRequest as RegisterValidator;
use bornemap_db::AppState;

fn new_session_repo(state: &web::Data<AppState>) -> PgSessionRepository {
    PgSessionRepository::new(state.db.clone())
}

fn new_user_repo(state: &web::Data<AppState>) -> PgUserRepository {
    PgUserRepository::new(state.db.clone())
}

#[post("/api/v1/auth/register")]
pub async fn register(
    state: web::Data<AppState>,
    body: web::Json<RegisterRequest>,
    _jwt_service: web::Data<JwtService>,
    request_id: RequestId,
) -> HttpResponse {
    // Validate request
    let validator = RegisterValidator {
        email: body.email.clone(),
        password: body.password.clone(),
    };

    match validator.validate() {
        Ok(_) => {
            info!("Registration request validated for email: {}", body.email);
        }
        Err(_) => {
            let validation_errors = vec!["Invalid email or password format".to_string()];
            return map_validation_errors(validation_errors);
        }
    }

    let repo = new_user_repo(&state);
    let use_case = RegisterUseCase::new(repo);

    let req = RegisterReq {
        email: body.email.clone(),
        password: body.password.clone(),
    };

    match use_case.execute(req).await {
        Ok(resp) => {
            info!("User registered successfully: {}", body.email);
            HttpResponse::Created().json(ApiResponse::success(
                RegisterResponse {
                    user_id: resp.user_id,
                },
                request_id.0,
            ))
        }
        Err(err) => {
            error!("Registration failed for email: {} - {:?}", body.email, err);
            map_app_error(err.into())
        }
    }
}

#[post("/api/v1/auth/login")]
pub async fn login(
    state: web::Data<AppState>,
    body: web::Json<LoginRequest>,
    jwt_service: web::Data<JwtService>,
    request_id: RequestId,
) -> HttpResponse {
    // Validate request
    let validator = crate::validation::login::LoginRequest {
        email: body.email.clone(),
        password: body.password.clone(),
    };

    match validator.validate() {
        Ok(_) => {
            info!("Login request validated for email: {}", body.email);
        }
        Err(_) => {
            let validation_errors = vec!["Email and password are required".to_string()];
            return map_validation_errors(validation_errors);
        }
    }

    let user_repo = new_user_repo(&state);
    let session_repo = new_session_repo(&state);
    let refresh_ttl: i64 = std::env::var("JWT_REFRESH_TTL_DAYS")
        .unwrap_or_else(|_| "7".into())
        .parse()
        .unwrap_or(7)
        * 86400;

    let use_case = LoginUseCase::new(
        user_repo,
        session_repo,
        jwt_service.get_ref().clone(),
        refresh_ttl,
    );

    let req = LoginReq {
        email: body.email.clone(),
        password: body.password.clone(),
    };

    match use_case.execute(req).await {
        Ok(resp) => {
            info!("User logged in successfully: {}", body.email);
            HttpResponse::Ok().json(ApiResponse::success(
                AuthDto {
                    access_token: resp.access_token,
                    refresh_token: Some(resp.refresh_token),
                    token_type: resp.token_type,
                    expires_in: resp.expires_in,
                },
                request_id.0,
            ))
        }
        Err(err) => {
            error!("Login failed for email: {} - {:?}", body.email, err);
            map_app_error(err)
        }
    }
}

#[post("/api/v1/auth/refresh")]
pub async fn refresh(
    state: web::Data<AppState>,
    body: web::Json<RefreshTokenRequest>,
    jwt_service: web::Data<JwtService>,
    request_id: RequestId,
) -> HttpResponse {
    let user_repo = new_user_repo(&state);
    let session_repo = new_session_repo(&state);
    let refresh_ttl: i64 = std::env::var("JWT_REFRESH_TTL_DAYS")
        .unwrap_or_else(|_| "7".into())
        .parse()
        .unwrap_or(7)
        * 86400;

    let use_case = RefreshUseCase::new(
        user_repo,
        session_repo,
        jwt_service.get_ref().clone(),
        refresh_ttl,
    );

    let req = RefreshReq {
        refresh_token: body.refresh_token.clone(),
    };

    match use_case.execute(req).await {
        Ok(resp) => {
            info!("Token refresh successful");
            HttpResponse::Ok().json(ApiResponse::success(
                AuthDto {
                    access_token: resp.access_token,
                    refresh_token: Some(resp.refresh_token),
                    token_type: resp.token_type,
                    expires_in: resp.expires_in,
                },
                request_id.0,
            ))
        }
        Err(err) => {
            error!("Token refresh failed - {:?}", err);
            map_app_error(err)
        }
    }
}

#[post("/api/v1/auth/logout")]
pub async fn logout(
    _state: web::Data<AppState>,
    _jwt_service: web::Data<JwtService>,
    request_id: RequestId,
) -> HttpResponse {
    // In a real implementation, we would validate the JWT and invalidate the session
    // For now, we'll just log the logout request
    info!("Logout request received");

    // Return 204 No Content as per API contract
    HttpResponse::NoContent().json(ApiResponse::success(
        (), // Empty data for 204 response
        request_id.0,
    ))
}
