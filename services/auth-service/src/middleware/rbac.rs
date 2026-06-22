use actix_web::body::BoxBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::HttpMessage;
use domain_types::jwt::JwtClaims;
use domain_types::role::Role;

#[derive(Debug, Clone)]
pub struct RouteGuard {
    pub allowed_roles: Vec<Role>,
}

impl RouteGuard {
    pub fn new(roles: Vec<Role>) -> Self {
        Self { allowed_roles: roles }
    }

    pub fn any() -> Self {
        Self {
            allowed_roles: vec![Role::Driver, Role::Partner, Role::Admin],
        }
    }

    pub fn admin() -> Self {
        Self {
            allowed_roles: vec![Role::Admin],
        }
    }

    pub fn partner_admin() -> Self {
        Self {
            allowed_roles: vec![Role::Partner, Role::Admin],
        }
    }
}

pub async fn rbac_middleware(
    req: ServiceRequest,
    next: Next<BoxBody>,
    guard: RouteGuard,
) -> Result<ServiceResponse<BoxBody>, actix_web::Error> {
    let path = req.path();
    if path == "/health" || path == "/api/v1/auth/login" {
        return next.call(req).await;
    }

    let user_role = req
        .extensions()
        .get::<JwtClaims>()
        .map(|c| c.role.clone())
        .ok_or_else(|| actix_web::error::ErrorUnauthorized("No JWT claims found"))?;

    let authorized = guard.allowed_roles.iter().any(|r| user_role.inherits(r));

    if !authorized {
        return Err(actix_web::error::ErrorForbidden(format!(
            "Insufficient role: required {:?}, got {}",
            guard.allowed_roles, user_role
        )));
    }

    next.call(req).await
}
