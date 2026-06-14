use actix_web::{web, HttpResponse};
use sqlx::PgPool;
use crate::domain::{CreateChargerRequest, CreateResponse};
use crate::error::AdminServiceError;
use services_shared::domain::ChargerDetailDto;

/// Create a new charger at a station
