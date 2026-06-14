use actix_web::{web, HttpResponse};
use sqlx::PgPool;
use crate::domain::{CreatePartnerRequest, CreateResponse};
use crate::error::AdminServiceError;
use services_shared::domain::PartnerDto;

/// Create a new charging network partner
