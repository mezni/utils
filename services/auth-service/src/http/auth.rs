use actix_web::{HttpResponse, post, web};

use super::dto::{AuthResponse as AuthDto, LoginRequest, RefreshTokenRequest, RegisterRequest};
use super::error::{map_app_error, map_auth_error};
use crate::application::login::{LoginRequest as LoginReq, LoginUseCase};
use crate::application::refresh::{RefreshRequest as RefreshReq, RefreshUseCase};
use crate::application::register::{RegisterRequest as RegisterReq, RegisterUseCase};
use crate::infrastructure::jwt::JwtService;
use crate::infrastructure::pg_session_repo::PgSessionRepository;
use crate::infrastructure::pg_user_repo::PgUserRepository;
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
    jwt_service: web::Data<JwtService>,
) -> HttpResponse {
    let repo = new_user_repo(&state);
    let use_case = RegisterUseCase::new(repo, jwt_service.get_ref().clone());

    let req = RegisterReq {
        email: body.email.clone(),
        password: body.password.clone(),
    };

    match use_case.execute(req).await {
        Ok(resp) => HttpResponse::Created().json(AuthDto {
            access_token: resp.access_token,
            refresh_token: None,
            token_type: resp.token_type,
            expires_in: resp.expires_in,
        }),
        Err(err) => map_auth_error(err),
    }
}

#[post("/api/v1/auth/login")]
pub async fn login(
    state: web::Data<AppState>,
    body: web::Json<LoginRequest>,
    jwt_service: web::Data<JwtService>,
) -> HttpResponse {
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
        Ok(resp) => HttpResponse::Ok().json(AuthDto {
            access_token: resp.access_token,
            refresh_token: Some(resp.refresh_token),
            token_type: resp.token_type,
            expires_in: resp.expires_in,
        }),
        Err(err) => map_app_error(err),
    }
}

#[post("/api/v1/auth/refresh")]
pub async fn refresh(
    state: web::Data<AppState>,
    body: web::Json<RefreshTokenRequest>,
    jwt_service: web::Data<JwtService>,
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
        Ok(resp) => HttpResponse::Ok().json(AuthDto {
            access_token: resp.access_token,
            refresh_token: Some(resp.refresh_token),
            token_type: resp.token_type,
            expires_in: resp.expires_in,
        }),
        Err(err) => map_app_error(err),
    }
}
