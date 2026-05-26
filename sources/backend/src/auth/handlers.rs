use crate::auth::jwt::create_token;
use crate::domain::users::models::CreateUserRequest;
use crate::domain::users::repository as user_repo;
use crate::utils::error::ProblemResponse;
use actix_web::{web, HttpResponse};
use argon2::Argon2;
use password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use rand_core::OsRng;
use serde::Deserialize;
use sqlx::PgPool;

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

fn hash_password(password: &str) -> Result<String, String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| format!("Password hashing failed: {}", e))
}

fn verify_password(password: &str, hash: &str) -> Result<bool, String> {
    let parsed_hash = PasswordHash::new(hash)
        .map_err(|e| format!("Invalid password hash format: {}", e))?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok())
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

    let password_hash = match tokio::task::spawn_blocking(move || hash_password(&req.password)).await {
        Ok(Ok(hash)) => hash,
        Ok(Err(e)) => return ProblemResponse::internal_error_with(e),
        Err(_) => return ProblemResponse::internal_error(),
    };

    match user_repo::create(&pool, &id, &req.email, &req.username, &password_hash, role, false).await {
        Ok(user) => {
            let jwt_secret = crate::auth::jwt::jwt_secret();
            match create_token(&user.id, role, &jwt_secret) {
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

    let user = match user_repo::get_by_email(&pool, &req.email).await {
        Ok(Some(u)) => u,
        Ok(None) => return ProblemResponse::unauthorized("Invalid email or password"),
        Err(_) => return ProblemResponse::internal_error(),
    };

    let password = req.password;
    let stored_hash = user.password_hash.clone();

    let valid = match tokio::task::spawn_blocking(move || verify_password(&password, &stored_hash)).await {
        Ok(Ok(true)) => true,
        Ok(Ok(false)) => false,
        Ok(Err(e)) => {
            tracing::error!("Password verification error: {}", e);
            return ProblemResponse::internal_error();
        }
        Err(_) => return ProblemResponse::internal_error(),
    };

    if !valid {
        return ProblemResponse::unauthorized("Invalid email or password");
    }

    let jwt_secret = crate::auth::jwt::jwt_secret();
    match create_token(&user.id, &user.role, &jwt_secret) {
        Ok(token) => HttpResponse::Ok().json(serde_json::json!({
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
