use actix_web::{web, HttpResponse};
use actix_web::HttpRequest;

use crate::db::UsersRepository;
use crate::error::AuthError;
use crate::keycloak::KeycloakClient;
use crate::models::auth::{LoginRequest, TokenResponse};
use crate::validation::token::validate_token;

/// Handle login requests.
pub async fn login(
    req: web::Json<LoginRequest>,
    claims: Option<crate::keycloak::Claims>,
    repo: web::Data<UsersRepository>,
    client: web::Data<&KeycloakClient>,
) -> Result<TokenResponse, AuthError> {
    let LoginRequest { email, password } = req.into_inner();

    // Validate token format before contacting Keycloak
    validate_token(&email).map_err(|e| {
        tracing::warn!("Login validation error: {}", e);
        AuthError::ValidationError(e.to_string())
    })?;

    validate_token(&password).map_err(|e| {
        tracing::warn!("Login validation error: {}", e);
        AuthError::ValidationError(e.to_string())
    })?;

    // Check if request came from a proxy and extract user info (if available)
    let keycloak_sub = claims.map(|c| c.sub);

    // Call Keycloak to authenticate
    let token_response = client
        .login(&email, &password)
        .await
        .map_err(|e| {
            // Do not log detailed Keycloak error
            tracing::error!("Authentication failed");
            if matches!(e, AuthError::InvalidCredentials) {
                e
            } else {
                AuthError::AuthUnavailable
            }
        })?;

    // Extract claims from the access token
    let claims = client.extract_claims(&token_response.access_token).map_err(|e| {
        tracing::error!("Failed to extract claims: {}", e);
        AuthError::AuthUnavailable
    })?;

    // Upsert user profile in the database
    let user_profile = repo
        .upsert_user(&web::Data::<sqlx::PgPool>(web::Data::clone(&repo as *const _)), &claims)
        .await
        .map_err(|e| {
            tracing::error!("Failed to upsert user: {}", e);
            AuthError::AuthUnavailable
        })?;

    tracing::info!(
        "User logged in: email={}, id={}, roles={:?}",
        user_profile.email,
        user_profile.id,
        user_profile.roles
    );

    Ok(token_response)
}
