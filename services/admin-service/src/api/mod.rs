//! API module for admin-service
//! Exposes HTTP endpoints for analytics read operations

pub mod analytics;
pub mod health;
pub mod routes;

pub use analytics::*;
pub use health::*;
pub use routes::configure_routes;