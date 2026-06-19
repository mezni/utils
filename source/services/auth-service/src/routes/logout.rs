use actix_web::{web, HttpResponse};
use actix_web::HttpRequest;

use crate::db::UsersRepository;
use crate::error::AuthError;
use crate::keycloak::KeycloakClient;
use crate::models::auth::{LogoutRequest, LogoutResponse};
use crate::validation::token::validate_token;

/// Handle logout requests.
pub async fn logout(
    req: web::Json<LogoutRequest>,
    claims: Option<crate::keycloak::Claims>,
    repo: web::Data<UsersRepository>,
    client: web::Data<&KeycloakClient>,
) -> Result<LogoutResponse, AuthError> {
    let LogoutRequest { refresh_token } = req.into_inner();

    // Validate token format before contacting Keycloak
    validate_token(&refresh_token).map_err(|e| {
        tracing::warn!("Logout validation error: {}", e);
        AuthError::ValidationError(e.to_string())
    })?;

    // Call Keycloak to revoke the refresh token
    let _ = client
        .logout(&refresh_token)
        .await
        .map_err(|e| {
            tracing::error!("Logout failed: {}", e);
            e
        })?;

    tracing::info!("User logged out: refresh_token_revoked=true");

    Ok(LogoutResponse {
        message: "logged_out".to_string(),
    })
}
