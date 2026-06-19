use actix_web::web;
use actix_web::HttpRequest;
use actix_web::Result;

use crate::db::UsersRepository;
use crate::error::AuthError;
use crate::keycloak::KeycloakClient;
use crate::models::auth::{LoginRequest, TokenResponse};

/// Handle login requests.
pub async fn login(
    claims: Option<Claims>,
    repo: web::Data<UsersRepository>,
    client: web::Data<KeycloakClient>,
    login_req: web::Json<LoginRequest>,
    request: HttpRequest,
) -> Result<TokenResponse> {
    let LoginRequest { email, password } = login_req.into_inner();

    // Check if request came from a proxy and extract user info (if available)
    let keycloak_sub = claims.map(|c| c.sub);

    // Call Keycloak to authenticate
    let token_response = client
        .login(&email, &password)
        .await
        .map_err(|e| {
            tracing::error!("Login failed for {}: {}", email, e);
            e
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
