use actix_web::web;
use actix_web::HttpRequest;
use actix_web::Result;

use crate::db::UsersRepository;
use crate::error::AuthError;
use crate::keycloak::KeycloakClient;
use crate::models::auth::{RefreshRequest, TokenResponse};

/// Handle refresh token requests.
pub async fn refresh(
    claims: Option<Claims>,
    repo: web::Data<UsersRepository>,
    client: web::Data<KeycloakClient>,
    refresh_req: web::Json<RefreshRequest>,
    request: HttpRequest,
) -> Result<TokenResponse> {
    let RefreshRequest { refresh_token } = refresh_req.into_inner();

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
