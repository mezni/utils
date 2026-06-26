use actix_web::{HttpResponse, post, web};

use super::dto::{AuthResponse as AuthDto, LoginRequest, RegisterRequest};
use super::error::map_auth_error;
use crate::application::login::{LoginRequest as LoginReq, LoginUseCase};
use crate::application::register::{RegisterRequest as RegisterReq, RegisterUseCase};
use crate::infrastructure::jwt::JwtService;
use crate::infrastructure::pg_user_repo::PgUserRepository;
use bornemap_db::AppState;

#[post("/api/v1/auth/register")]
pub async fn register(
    state: web::Data<AppState>,
    body: web::Json<RegisterRequest>,
    jwt_service: web::Data<JwtService>,
) -> HttpResponse {
    let repo = PgUserRepository::new(state.db.clone());
    let use_case = RegisterUseCase::new(repo, jwt_service.get_ref().clone());

    let req = RegisterReq {
        email: body.email.clone(),
        password: body.password.clone(),
    };

    match use_case.execute(req).await {
        Ok(resp) => HttpResponse::Created().json(AuthDto {
            access_token: resp.access_token,
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
    let repo = PgUserRepository::new(state.db.clone());
    let use_case = LoginUseCase::new(repo, jwt_service.get_ref().clone());

    let req = LoginReq {
        email: body.email.clone(),
        password: body.password.clone(),
    };

    match use_case.execute(req).await {
        Ok(resp) => HttpResponse::Ok().json(AuthDto {
            access_token: resp.access_token,
            token_type: resp.token_type,
            expires_in: resp.expires_in,
        }),
        Err(err) => map_auth_error(err),
    }
}
