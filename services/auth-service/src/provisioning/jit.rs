use sqlx::PgPool;
use uuid::Uuid;

use domain_types::role::Role;
use domain_types::user::UserProfile;

#[derive(Debug, thiserror::Error)]
pub enum JitError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("User not found in Keycloak: {0}")]
    UserNotFound(Uuid),
}

pub async fn upsert_user_profile(
    pool: &PgPool,
    user_id: Uuid,
    email: &str,
    role: &Role,
    display_name: Option<&str>,
) -> Result<UserProfile, JitError> {
    let role_str = role.as_str();

    let row = sqlx::query(
        r#"
        INSERT INTO users.user_profiles (user_id, email, role, display_name, last_login_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (user_id) DO UPDATE SET
            email = EXCLUDED.email,
            role = EXCLUDED.role,
            display_name = COALESCE(EXCLUDED.display_name, users.user_profiles.display_name),
            last_login_at = NOW(),
            updated_at = NOW()
        RETURNING user_id, email, role, display_name, phone, locale, is_active, last_login_at
        "#,
    )
    .bind(user_id)
    .bind(email)
    .bind(role_str)
    .bind(display_name)
    .fetch_one(pool)
    .await?;

    use sqlx::Row;
    Ok(UserProfile {
        user_id: row.get("user_id"),
        email: row.get("email"),
        role: Role::from_str(row.get::<String, _>("role").as_str()).unwrap_or_default(),
        display_name: row.get("display_name"),
        phone: row.get("phone"),
        locale: row.get("locale"),
        is_active: row.get("is_active"),
        last_login_at: row.get("last_login_at"),
    })
}

pub async fn get_user_profile(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Option<UserProfile>, JitError> {
    let row = sqlx::query(
        r#"
        SELECT user_id, email, role, display_name, phone, locale, is_active, last_login_at
        FROM users.user_profiles
        WHERE user_id = $1
        "#,
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await?;

    use sqlx::Row;
    Ok(row.map(|r| UserProfile {
        user_id: r.get("user_id"),
        email: r.get("email"),
        role: Role::from_str(r.get::<String, _>("role").as_str()).unwrap_or_default(),
        display_name: r.get("display_name"),
        phone: r.get("phone"),
        locale: r.get("locale"),
        is_active: r.get("is_active"),
        last_login_at: r.get("last_login_at"),
    }))
}
