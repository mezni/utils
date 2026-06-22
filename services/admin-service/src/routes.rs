use actix_web::web;

use domain_types::role::Role;

pub struct ProtectedRoute {
    pub path: &'static str,
    pub allowed_roles: &'static [Role],
    pub public: bool,
}

pub const ADMIN_SERVICE_ROUTES: &[ProtectedRoute] = &[
    ProtectedRoute { path: "/health", allowed_roles: &[], public: true },
    ProtectedRoute { path: "/api/v1/stations", allowed_roles: &[Role::Partner, Role::Admin], public: false },
    ProtectedRoute { path: "/api/v1/analytics", allowed_roles: &[Role::Admin], public: false },
    ProtectedRoute { path: "/api/v1/partners", allowed_roles: &[Role::Partner, Role::Admin], public: false },
];

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.route("/health", web::get().to(super::health));
}
