// Library entry point with public API
mod config;
mod db;
mod error;
mod handlers;
mod models;
mod routes;

pub use config::PostgresUrl;
pub use error::{AppError, ApiError, EntityNotFoundError};
pub use models::{HealthCheckRequest, HealthCheckResponse, PartnerRequest, PartnerResponse, PartnerListResponse, StationRequest, StationResponse, StationListResponse, ChargerRequest, ChargerResponse, ChargerListResponse};
