use actix_web::{web, HttpResponse};
use uuid::Uuid;

use domain_types::user::UserProfile;
use domain_types::role::Role;

#[derive(serde::Deserialize)]
pub struct SyncQuery {
    pub user_uuid: Uuid,
    pub email: Option<String>,
    pub role: Option<String>,
}

#[derive(serde::Serialize)]
pub struct SyncResponse {
    pub status: String,
    pub profile: UserProfile,
}

pub async fn handle_sync(
    query: web::Query<SyncQuery>,
) -> HttpResponse {
    let email = query.email.clone().unwrap_or_default();
    let role_str = query.role.clone().unwrap_or_else(|| "driver".to_string());
    let role = Role::from_str(&role_str).unwrap_or_default();

    let profile = UserProfile::new(query.user_uuid, email, role);

    HttpResponse::Ok().json(SyncResponse {
        status: "synced".to_string(),
        profile,
    })
}
