pub mod auth;
pub mod error;

pub use auth::{AuthInfo, Claims, JwtService, jwt_auth, require_role};
pub use error::{CoreServiceError, ErrorResponse, ErrorDetail, CoreResult, 
    validation_error, not_found, unauthorized, forbidden, conflict, bad_request, 
    internal_error, service_unavailable, configuration_error};