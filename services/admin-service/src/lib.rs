pub mod models;
pub mod services;
pub mod api;
pub mod db;

pub use services::admin_service::AdminService;

// This is the shared library for the admin service
// It can be used by other services that need to interact with inventory
