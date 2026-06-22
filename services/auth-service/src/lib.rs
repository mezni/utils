pub mod models;
pub mod services;
pub mod api;
pub mod db;

pub use services::auth_service::AuthService;

// This is the shared library for the auth service
// It can be used by other services that need to interact with auth
