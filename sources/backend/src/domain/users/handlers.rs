use crate::domain::users::models::{CreateUserRequest, UpdateUserRequest, User};
use crate::domain::users::repository;
use crate::utils::error::ProblemResponse;
use crate::utils::id_validator;
use crate::utils::pagination::Cursor;
use crate::utils::pagination::ListQuery;
use actix_web::{web, HttpResponse};
use serde::Serialize;
use sqlx::PgPool;

fn hash_password(password: &str) -> Result<String, String> {
    use argon2::Argon2;
    use password_hash::{PasswordHasher, SaltString};
    use rand_core::OsRng;
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| format!("Password hashing failed: {}", e))
}

#[derive(Serialize)]
pub struct UserResponse {
    pub id: String,
    pub email: String,
    pub username: String,
    pub role: String,
    pub is_test: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<User> for UserResponse {
    fn from(u: User) -> Self {
        Self {
            id: u.id,
            email: u.email,
            username: u.username,
            role: u.role,
            is_test: u.is_test,
            created_at: u.created_at,
            updated_at: u.updated_at,
        }
    }
}

#[derive(Serialize)]
pub struct UserListResponse {
    pub data: Vec<UserResponse>,
    pub pagination: crate::utils::pagination::Pagination,
}

pub async fn create_user(
    pool: web::Data<PgPool>,
    auth: crate::auth::middleware::AuthUser,
    body: web::Json<CreateUserRequest>,
) -> HttpResponse {
    if auth.0.role != "admin" {
        return ProblemResponse::forbidden("Only admins can create users");
    }

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

    if !["admin", "partner", "driver"].contains(&req.role.as_str()) {
        return ProblemResponse::validation("Role must be admin, partner, or driver");
    }

    match repository::exists_by_email(&pool, &req.email).await {
        Ok(true) => return ProblemResponse::conflict("Email already exists"),
        Err(_) => return ProblemResponse::internal_error(),
        _ => {}
    }

    match repository::exists_by_username(&pool, &req.username).await {
        Ok(true) => return ProblemResponse::conflict("Username already exists"),
        Err(_) => return ProblemResponse::internal_error(),
        _ => {}
    }

    let id = crate::utils::id_generator::generate_id("USR");

    let password = req.password.clone();
    let password_hash = match tokio::task::spawn_blocking(move || hash_password(&password)).await {
        Ok(Ok(hash)) => hash,
        Ok(Err(e)) => return ProblemResponse::internal_error_with(e),
        Err(_) => return ProblemResponse::internal_error(),
    };

    match repository::create(&pool, &id, &req.email, &req.username, &password_hash, &req.role, false).await {
        Ok(user) => HttpResponse::Created().json(UserResponse::from(user)),
        Err(_) => ProblemResponse::internal_error(),
    }
}

pub async fn list_users(
    pool: web::Data<PgPool>,
    auth: crate::auth::middleware::AuthUser,
    query: web::Query<ListQuery>,
) -> HttpResponse {
    if auth.0.role != "admin" && auth.0.role != "partner" {
        return ProblemResponse::forbidden("Only admins and partners can list users");
    }

    let q = query.into_inner();
    let limit = q.limit();

    let cursor = match q.cursor.as_ref() {
        Some(c) => match Cursor::decode(c) {
            Ok(c) => Some(c),
            Err(_) => return ProblemResponse::validation("Invalid cursor format"),
        },
        None => None,
    };

    match repository::list(&pool, cursor, limit, q.include_test()).await {
        Ok((users, next_cursor, has_more)) => {
            let data: Vec<UserResponse> = users.into_iter().map(UserResponse::from).collect();
            HttpResponse::Ok().json(UserListResponse {
                pagination: crate::utils::pagination::Pagination {
                    next_cursor,
                    has_more,
                },
                data,
            })
        }
        Err(_) => ProblemResponse::internal_error(),
    }
}

pub async fn get_user(
    pool: web::Data<PgPool>,
    auth: crate::auth::middleware::AuthUser,
    path: web::Path<String>,
) -> HttpResponse {
    if auth.0.role != "admin" && auth.0.role != "partner" {
        return ProblemResponse::forbidden("Only admins and partners can view users");
    }

    let id = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&id, "USR") {
        return ProblemResponse::not_found(e);
    }

    match repository::get_by_id(&pool, &id).await {
        Ok(Some(user)) => HttpResponse::Ok().json(UserResponse::from(user)),
        Ok(None) => ProblemResponse::not_found(format!("User '{}' not found", &id)),
        Err(_) => ProblemResponse::internal_error(),
    }
}

pub async fn update_user(
    pool: web::Data<PgPool>,
    auth: crate::auth::middleware::AuthUser,
    path: web::Path<String>,
    body: web::Json<UpdateUserRequest>,
) -> HttpResponse {
    if auth.0.role != "admin" {
        return ProblemResponse::forbidden("Only admins can update users");
    }

    let id = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&id, "USR") {
        return ProblemResponse::not_found(e);
    }

    match repository::update(&pool, &id, &body).await {
        Ok(Some(user)) => HttpResponse::Ok().json(UserResponse::from(user)),
        Ok(None) => {
            let exists = repository::get_by_id(&pool, &id).await.unwrap_or(None);
            if exists.is_some() {
                ProblemResponse::conflict("Concurrent modification detected — re-read and retry")
            } else {
                ProblemResponse::not_found(format!("User '{}' not found", &id))
            }
        }
        Err(_) => ProblemResponse::internal_error(),
    }
}

pub async fn delete_user(
    pool: web::Data<PgPool>,
    auth: crate::auth::middleware::AuthUser,
    path: web::Path<String>,
) -> HttpResponse {
    if auth.0.role != "admin" {
        return ProblemResponse::forbidden("Only admins can delete users");
    }

    let id = path.into_inner();

    if let Err(e) = id_validator::validate_id_prefix(&id, "USR") {
        return ProblemResponse::not_found(e);
    }

    match repository::soft_delete(&pool, &id).await {
        Ok(Some(_)) => HttpResponse::NoContent().finish(),
        Ok(None) => ProblemResponse::not_found(format!("User '{}' not found", &id)),
        Err(_) => ProblemResponse::internal_error(),
    }
}
