use crate::auth::jwt::{create_token, jwt_secret};
use crate::domain::users::models::CreateUserRequest;
use crate::domain::users::{handlers as user_handlers, repository as user_repo};
use crate::utils::error::ProblemResponse;
use actix_web::{web, HttpResponse};
use serde::Deserialize;
use sqlx::PgPool;

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

pub async fn register(
    pool: web::Data<PgPool>,
    body: web::Json<CreateUserRequest>,
) -> HttpResponse {
    let req = body.into_inner();

    if req.password.len() < 8 {
        return ProblemResponse::validation("Password must be at least 8 characters");
    }
    if !req.email.contains('@') || !req.email.contains('.') {
        return ProblemResponse::validation("Invalid email format");
    }
    if req.username.len() < 2 || req.username.len() > 50 {
        return ProblemResponse::validation("Username must be between 2 and 50 characters");
    }

    let role = "driver";
    let id = crate::utils::id_generator::generate_id("USR");
    let password_hash = "[placeholder]";

    match user_repo::exists_by_email(&pool, &req.email).await {
        Ok(true) => return ProblemResponse::conflict("Email already exists"),
        Err(_) => return ProblemResponse::internal_error(),
        _ => {}
    }

    match user_repo::exists_by_username(&pool, &req.username).await {
        Ok(true) => return ProblemResponse::conflict("Username already exists"),
        Err(_) => return ProblemResponse::internal_error(),
        _ => {}
    }

    match user_repo::create(&pool, &id, &req.email, &req.username, password_hash, role, false).await {
        Ok(user) => {
            match create_token(&user.id, role, &jwt_secret()) {
                Ok(token) => HttpResponse::Created().json(serde_json::json!({
                    "user": {
                        "id": user.id,
                        "email": user.email,
                        "username": user.username,
                        "role": user.role,
                        "is_test": user.is_test,
                        "created_at": user.created_at,
                        "updated_at": user.updated_at,
                    },
                    "token": token,
                })),
                Err(_) => ProblemResponse::internal_error(),
            }
        }
        Err(_) => ProblemResponse::internal_error(),
    }
}

pub async fn login(
    pool: web::Data<PgPool>,
    body: web::Json<LoginRequest>,
) -> HttpResponse {
    let req = body.into_inner();

    let _users = user_repo::list(&pool, None, 1, true).await;

    HttpResponse::Ok().json(serde_json::json!({
        "message": "Login endpoint — password verification via argon2 deferred",
    }))
}
