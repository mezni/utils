use sqlx::PgPool;
use uuid::Uuid;
use chrono::Utc;
use crate::domain::{User, UserProfile, UserRole, RegisterRequest, LoginRequest, AuthResponse};
use crate::error::AuthServiceError;
use crate::jwt;

/// Hash password using bcrypt
fn hash_password(password: &str) -> Result<String, AuthServiceError> {
    bcrypt::hash(password, 4)
        .map_err(|_| AuthServiceError::PasswordError)
}

/// Verify password against hash
fn verify_password(password: &str, hash: &str) -> bool {
    bcrypt::verify(password, hash).unwrap_or(false)
}

pub async fn register(
    pool: &PgPool,
    req: RegisterRequest,
) -> Result<UserProfile, AuthServiceError> {
    // Check if user already exists
    let existing = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM users.user WHERE email = $1"
    )
    .bind(&req.email)
    .fetch_one(pool)
    .await
    .map_err(|e| {
        tracing::error!("Database error: {}", e);
        AuthServiceError::DatabaseError(e.to_string())
    })?;

    if existing > 0 {
        return Err(AuthServiceError::UserAlreadyExists);
    }

    // Hash password
    let password_hash = hash_password(&req.password)?;
    let user_id = format!("usr-{}", Uuid::new_v4().to_string().replace("-", "").chars().take(16).collect::<String>());
    let now = Utc::now();

    // Create user
    let user = sqlx::query_as::<_, User>(
        r#"
        INSERT INTO users.user (id, email, password_hash, full_name, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, email, password_hash, full_name, created_at, updated_at
        "#
    )
    .bind(&user_id)
    .bind(&req.email)
    .bind(&password_hash)
    .bind(&req.full_name)
    .bind(now)
    .bind(now)
    .fetch_one(pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to create user: {}", e);
        AuthServiceError::DatabaseError(e.to_string())
    })?;

    // Assign default role
    sqlx::query(
        r#"
        INSERT INTO users.user_role (user_id, role)
        VALUES ($1, $2)
        "#
    )
    .bind(&user_id)
    .bind("driver")
    .execute(pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to assign role: {}", e);
        AuthServiceError::DatabaseError(e.to_string())
    })?;

    tracing::info!("User registered: {}", user_id);

    Ok(UserProfile {
        id: user.id,
        email: user.email,
        full_name: user.full_name,
        roles: vec!["driver".to_string()],
        created_at: user.created_at,
    })
}

pub async fn login(
    pool: &PgPool,
    req: LoginRequest,
    jwt_secret: &str,
) -> Result<AuthResponse, AuthServiceError> {
    // Find user by email
    let user = sqlx::query_as::<_, User>(
        "SELECT id, email, password_hash, full_name, created_at, updated_at FROM users.user WHERE email = $1"
    )
    .bind(&req.email)
    .fetch_optional(pool)
    .await
    .map_err(|e| {
        tracing::error!("Database error: {}", e);
        AuthServiceError::DatabaseError(e.to_string())
    })?
    .ok_or(AuthServiceError::InvalidCredentials)?;

    // Verify password
    if !verify_password(&req.password, &user.password_hash) {
        return Err(AuthServiceError::InvalidCredentials);
    }

    // Get user roles
    let roles: Vec<String> = sqlx::query_scalar(
        "SELECT role FROM users.user_role WHERE user_id = $1"
    )
    .bind(&user.id)
    .fetch_all(pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch roles: {}", e);
        AuthServiceError::DatabaseError(e.to_string())
    })?;

    // Generate JWT
    let access_token = jwt::create_jwt(
        user.id.clone(),
        user.email.clone(),
        roles.clone(),
        jwt_secret,
    )?;

    tracing::info!("User logged in: {}", user.id);

    Ok(AuthResponse {
        access_token,
        token_type: "Bearer".to_string(),
        expires_in: 86400, // 24 hours in seconds
        user: UserProfile {
            id: user.id,
            email: user.email,
            full_name: user.full_name,
            roles,
            created_at: user.created_at,
        },
    })
}

pub async fn verify_token(
    token: &str,
    jwt_secret: &str,
) -> Result<crate::domain::JwtClaims, AuthServiceError> {
    jwt::verify_jwt(token, jwt_secret)
}

pub async fn get_user_profile(
    pool: &PgPool,
    user_id: &str,
) -> Result<UserProfile, AuthServiceError> {
    let user = sqlx::query_as::<_, User>(
        "SELECT id, email, password_hash, full_name, created_at, updated_at FROM users.user WHERE id = $1"
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| {
        tracing::error!("Database error: {}", e);
        AuthServiceError::DatabaseError(e.to_string())
    })?
    .ok_or(AuthServiceError::UserNotFound)?;

    let roles: Vec<String> = sqlx::query_scalar(
        "SELECT role FROM users.user_role WHERE user_id = $1"
    )
    .bind(user_id)
    .fetch_all(pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch roles: {}", e);
        AuthServiceError::DatabaseError(e.to_string())
    })?;

    Ok(UserProfile {
        id: user.id,
        email: user.email,
        full_name: user.full_name,
        roles,
        created_at: user.created_at,
    })
}
