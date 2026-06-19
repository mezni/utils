use actix_web::{web, HttpResponse};
use actix_web::HttpRequest;

use crate::db::UsersRepository;
use crate::error::AuthError;
use crate::keycloak::KeycloakClient;
use crate::models::auth::ErrorResponse;

/// Handle GET /api/v1/auth/me requests.
///
/// Returns the authenticated user's profile from the database.
pub async fn me(
    claims: Option<crate::keycloak::Claims>,
    repo: web::Data<UsersRepository>,
    client: web::Data<&KeycloakClient>,
) -> Result<web::Json<serde_json::Value>, HttpResponse> {
    let claims = claims.ok_or_else(|| {
        tracing::warn!("GET /me called without authentication");
        AuthError::AuthUnavailable
    })?;

    let user_profile = repo
        .get_user_by_sub(&web::Data::<sqlx::PgPool>(web::Data::clone(&repo as *const _)), &claims.sub)
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch user profile: {}", e);
            AuthError::AuthUnavailable
        })?;

    Ok(web::Json(serde_json::json!({
        "id": user_profile.id,
        "email": user_profile.email,
        "display_name": user_profile.display_name,
        "roles": user_profile.roles,
        "created_at": user_profile.created_at,
        "updated_at": user_profile.updated_at,
    })))
}
