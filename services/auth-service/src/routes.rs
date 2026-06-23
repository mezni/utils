pub mod auth;

use domain_types::role::Role;

pub struct ProtectedRoute {
    pub path: &'static str,
    pub allowed_roles: &'static [Role],
    pub public: bool,
}

pub const AUTH_SERVICE_ROUTES: &[ProtectedRoute] = &[
    ProtectedRoute { path: "/health", allowed_roles: &[], public: true },
    ProtectedRoute { path: "/api/v1/auth/login", allowed_roles: &[], public: true },
    ProtectedRoute { path: "/api/v1/auth/logout", allowed_roles: &[Role::Driver, Role::Partner, Role::Admin], public: false },
    ProtectedRoute { path: "/api/v1/auth/sync", allowed_roles: &[Role::Admin], public: false },
    ProtectedRoute { path: "/api/v1/auth/me", allowed_roles: &[Role::Driver, Role::Partner, Role::Admin], public: false },
    ProtectedRoute { path: "/api/v1/auth/preferences", allowed_roles: &[Role::Driver, Role::Partner, Role::Admin], public: false },
];


