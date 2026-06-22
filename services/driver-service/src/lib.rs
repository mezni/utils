pub mod models;
pub mod services;
pub mod api;
pub mod db;
pub mod telemetry;

pub use services::driver_service::DriverService;

// This is the shared library for the driver service
// It can be used by other services that need to interact with GIS and analytics
