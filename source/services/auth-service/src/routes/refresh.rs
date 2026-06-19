use actix_web::{web, HttpResponse};
use actix_web::HttpRequest;

use crate::db::UsersRepository;
use crate::error::AuthError;
use crate::keycloak::KeycloakClient;
use crate::models::auth::{RefreshRequest, TokenResponse};
use crate::validation::token::validate_token;

/// Handle refresh token requests.
pub async fn refresh(
    req: web::Json<RefreshRequest>,
    claims: Option<crate::keycloak::Claims>,
    repo: web::Data<UsersRepository>,
    client: web::Data<&KeycloakClient>,
) -> Result<TokenResponse, AuthError> {
    let RefreshRequest { refresh_token } = req.into_inner();

    // Validate token format before contacting Keycloak
    validate_token(&refresh_token).map_err(|e| {
        tracing::warn!("Refresh validation error: {}", e);
        AuthError::ValidationError(e.to_string())
    })?;

    // Check if request came from a proxy and extract user info (if available)
    let keycloak_sub = claims.map(|c| c.sub);

    // Refresh the token with Keycloak
    let token_response = client
        .refresh(&refresh_token)
        .await
        .map_err(|e| {
            tracing::error!("Token refresh failed: {}", e);
            e
        })?;

    // Extract claims from the new access token
    let claims = client.extract_claims(&token_response.access_token).map_err(|e| {
        tracing::error!("Failed to extract claims from new token: {}", e);
        AuthError::AuthUnavailable
    })?;

    // Upsert user profile with updated last_login_at
    let user_profile = repo
        .upsert_user(&web::Data::<sqlx::PgPool>(web::Data::clone(&repo as *const _)), &claims)
        .await
        .map_err(|e| {
            tracing::error!("Failed to upsert user: {}", e);
            AuthError::AuthUnavailable
        })?;

    tracing::info!(
        "Token refreshed: user_id={}, email={}",
        user_profile.id,
        user_profile.email
    );

    Ok(token_response)
}
