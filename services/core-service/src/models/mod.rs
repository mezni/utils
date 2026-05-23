use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

pub mod company;
pub mod station;
pub mod charger;

/// Base model with common fields for all entities
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct BaseModel {
    pub id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Base model with soft delete support
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct SoftDeleteModel {
    #[serde(flatten)]
    pub base: BaseModel,
    pub deleted_at: Option<DateTime<Utc>>,
}

impl SoftDeleteModel {
    /// Check if the entity is deleted
    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    /// Check if the entity is active (not deleted)
    pub fn is_active(&self) -> bool {
        !self.is_deleted()
    }

    /// Get the deletion time if deleted
    pub fn deleted_at(&self) -> Option<&DateTime<Utc>> {
        self.deleted_at.as_ref()
    }
}

/// Base model with version field for optimistic concurrency control
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct VersionedModel {
    #[serde(flatten)]
    pub base: BaseModel,
    pub version: i32,
}

/// Base model with both soft delete and version support
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct FullModel {
    #[serde(flatten)]
    pub base: BaseModel,
    pub deleted_at: Option<DateTime<Utc>>,
    pub version: i32,
}

impl FullModel {
    /// Check if the entity is deleted
    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    /// Check if the entity is active (not deleted)
    pub fn is_active(&self) -> bool {
        !self.is_deleted()
    }

    /// Get the deletion time if deleted
    pub fn deleted_at(&self) -> Option<&DateTime<Utc>> {
        self.deleted_at.as_ref()
    }

    /// Get the current version
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Increment the version for updates
    pub fn next_version(&self) -> i32 {
        self.version + 1
    }
}

/// Trait for generating entity IDs with prefixes
pub trait EntityId {
    const PREFIX: &'static str;
    
    fn generate_id() -> String {
        format!("{}-{}", Self::PREFIX, nanoid::nanoid!(12))
    }
    
    fn validate_id(id: &str) -> bool {
        let prefix = format!("{}-", Self::PREFIX);
        id.starts_with(&prefix) && id.len() == prefix.len() + 12
    }
}

/// Company ID generator
pub struct CompanyId;
impl EntityId for CompanyId {
    const PREFIX: &'static str = "CMP";
}

/// Station ID generator
pub struct StationId;
impl EntityId for StationId {
    const PREFIX: &'static str = "STA";
}

/// Charger ID generator
pub struct ChargerId;
impl EntityId for ChargerId {
    const PREFIX: &'static str = "CHR";
}

/// User ID generator
pub struct UserId;
impl EntityId for UserId {
    const PREFIX: &'static str = "USR";
}

/// Favorite ID generator
pub struct FavoriteId;
impl EntityId for FavoriteId {
    const PREFIX: &'static str = "FAV";
}

/// Review ID generator
pub struct ReviewId;
impl EntityId for ReviewId {
    const PREFIX: &'static str = "REV";
}

/// Event ID generator
pub struct EventId;
impl EntityId for EventId {
    const PREFIX: &'static str = "EVT";
}

/// Generate a UUID for database records
pub fn generate_uuid() -> String {
    Uuid::new_v4().to_string()
}

/// Generate a nanoid for entity IDs
pub fn generate_nanoid() -> String {
    nanoid::nanoid!(12)
}

/// Generate a company ID
pub fn generate_company_id() -> String {
    CompanyId::generate_id()
}

/// Generate a station ID
pub fn generate_station_id() -> String {
    StationId::generate_id()
}

/// Generate a charger ID
pub fn generate_charger_id() -> String {
    ChargerId::generate_id()
}

/// Generate a user ID
pub fn generate_user_id() -> String {
    UserId::generate_id()
}

/// Generate a favorite ID
pub fn generate_favorite_id() -> String {
    FavoriteId::generate_id()
}

/// Generate a review ID
pub fn generate_review_id() -> String {
    ReviewId::generate_id()
}

/// Generate an event ID
pub fn generate_event_id() -> String {
    EventId::generate_id()
}

/// Validate an entity ID by prefix
pub fn validate_entity_id(id: &str, prefix: &str) -> bool {
    let expected_prefix = format!("{}-", prefix);
    id.starts_with(&expected_prefix) && id.len() == expected_prefix.len() + 12
}

/// Validate a company ID
pub fn validate_company_id(id: &str) -> bool {
    CompanyId::validate_id(id)
}

/// Validate a station ID
pub fn validate_station_id(id: &str) -> bool {
    StationId::validate_id(id)
}

/// Validate a charger ID
pub fn validate_charger_id(id: &str) -> bool {
    ChargerId::validate_id(id)
}

/// Validate a user ID
pub fn validate_user_id(id: &str) -> bool {
    UserId::validate_id(id)
}

/// Validate a favorite ID
pub fn validate_favorite_id(id: &str) -> bool {
    FavoriteId::validate_id(id)
}

/// Validate a review ID
pub fn validate_review_id(id: &str) -> bool {
    ReviewId::validate_id(id)
}

/// Validate an event ID
pub fn validate_event_id(id: &str) -> bool {
    EventId::validate_id(id)
}

// Re-export the models for easier access
pub use company::Company;
pub use station::{Station, AccessType};
pub use charger::{Charger, ChargerType, ConnectorType, ChargerStatus};