use std::sync::Arc;

use sqlx::PgPool;

use crate::infrastructure::keycloak::JwtValidator;
use crate::services::profile_service::ProfileService;

pub struct AppState {
    pub pool: PgPool,
    pub keycloak: JwtValidator,
    pub profile_service: ProfileService,
}

pub type SharedState = Arc<AppState>;
