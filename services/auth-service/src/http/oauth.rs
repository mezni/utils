use actix_web::{web, HttpResponse, Responder};
use serde::Deserialize;

use crate::infrastructure::oauth::google::GoogleOAuthProvider;

#[derive(Clone)]
pub struct OAuthState {
    pub state_store: std::sync::Arc<dyn crate::application::oauth_state::OAuthStateStore>,
    pub google_provider: Option<GoogleOAuthProvider>,
}

#[derive(Deserialize)]
pub struct OAuthStartQuery {
    pub redirect_uri: String,
}

#[derive(Deserialize)]
pub struct OAuthCallbackQuery {
    pub code: String,
    pub state: String,
}

pub async fn google_start(
    _query: web::Query<OAuthStartQuery>,
) -> impl Responder {
    HttpResponse::Found()
        .append_header(("Location", "/api/auth/oauth/google/start"))
        .finish()
}

pub async fn google_callback(
    _query: web::Query<OAuthCallbackQuery>,
) -> impl Responder {
    HttpResponse::Found()
        .append_header(("Location", "/api/auth/oauth/google/callback"))
        .finish()
}

pub fn configure_oauth_routes(
    cfg: &mut web::ServiceConfig,
    _state: OAuthState,
) {
    cfg.service(
        web::scope("/api/v1/auth/oauth/google")
            .route("/start", web::get().to(google_start))
            .route("/callback", web::get().to(google_callback)),
    );
}
