use actix_web::{web, HttpResponse};
use sqlx::PgPool;
use crate::domain::{ProximityQuery, ProximityResponse};
use crate::error::DriverServiceError;
use services_shared::domain::NearbyStationRow;

/// Get nearby charging stations within radius of driver location
///
/// Queries PostGIS spatial index for fast proximity lookups. Returns all
/// available stations with aggregated charger details within the search radius.
