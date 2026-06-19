use actix_web::web;
use actix_web::HttpRequest;
use actix_web::Result;

use crate::db::UsersRepository;
use crate::error::AuthError;
use crate::keycloak::KeycloakClient;
use crate::models::auth::{LogoutRequest, LogoutResponse};

/// Handle logout requests.
pub async fn logout(
    claims: Option<Claims>,
    repo: web::Data<UsersRepository>,
    client: web::Data<KeycloakClient>,
    logout_req: web::Json<LogoutRequest>,
    request: HttpRequest,
) -> Result<LogoutResponse> {
    let LogoutRequest { refresh_token } = logout_req.into_inner();

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
