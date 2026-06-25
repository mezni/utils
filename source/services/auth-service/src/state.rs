use std::sync::Arc;
use sqlx::PgPool;


use crate::infrastructure::keycloak::JwtValidator;
use crate::services::profile_service::ProfileService;
use crate::oidc::client::OidcClient;
use crate::session::manager::SessionManager;

pub struct AppState {
    pub pool: PgPool,
    pub keycloak: JwtValidator,
    pub profile_service: ProfileService,
    pub oidc_client: OidcClient,
    pub session_manager: SessionManager,
}

pub type SharedState = Arc<AppState>;
