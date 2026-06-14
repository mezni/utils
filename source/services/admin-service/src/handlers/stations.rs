use actix_web::{web, HttpResponse};
use sqlx::PgPool;
use crate::domain::{CreateStationRequest, UpdateStationLiveRequest, CreateResponse};
use crate::error::AdminServiceError;
use services_shared::domain::StationDto;

/// Create a new charging station
