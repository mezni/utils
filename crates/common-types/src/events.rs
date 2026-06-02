use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum EventName {
    PageViewed,
    MapLoaded,
    MapViewportChanged,
    SearchPerformed,
    StationsNearbyViewed,
    FilterApplied,
    StationMarkerClicked,
    StationOpened,
    ChargerOpened,
    FavoriteStationAdded,
    FavoriteStationRemoved,
    ReviewSubmitted,
    ReviewUpdated,
    AuthStarted,
    AuthSucceeded,
    AuthFailed,
    PartnerStationCreated,
    PartnerStationUpdated,
    PartnerAvailabilityUpdated,
    AdminStationCreated,
    AdminReviewModerated,
    SearchFailed,
    StationLoadFailed,
}

impl EventName {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventName::PageViewed => "page.viewed",
            EventName::MapLoaded => "map.loaded",
            EventName::MapViewportChanged => "map.viewport_changed",
            EventName::SearchPerformed => "search.performed",
            EventName::StationsNearbyViewed => "stations.nearby.viewed",
            EventName::FilterApplied => "filter.applied",
            EventName::StationMarkerClicked => "station.marker_clicked",
            EventName::StationOpened => "station.opened",
            EventName::ChargerOpened => "charger.opened",
            EventName::FavoriteStationAdded => "favorite_station.added",
            EventName::FavoriteStationRemoved => "favorite_station.removed",
            EventName::ReviewSubmitted => "review.submitted",
            EventName::ReviewUpdated => "review.updated",
            EventName::AuthStarted => "auth.started",
            EventName::AuthSucceeded => "auth.succeeded",
            EventName::AuthFailed => "auth.failed",
            EventName::PartnerStationCreated => "partner_station.created",
            EventName::PartnerStationUpdated => "partner_station.updated",
            EventName::PartnerAvailabilityUpdated => "partner_availability.updated",
            EventName::AdminStationCreated => "admin_station.created",
            EventName::AdminReviewModerated => "admin_review.moderated",
            EventName::SearchFailed => "search.failed",
            EventName::StationLoadFailed => "station.load_failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Channel {
    DriverWeb,
    DriverMobile,
    PartnerDashboard,
    AdminDashboard,
}

impl Channel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Channel::DriverWeb => "driver_web",
            Channel::DriverMobile => "driver_mobile",
            Channel::PartnerDashboard => "partner_dashboard",
            Channel::AdminDashboard => "admin_dashboard",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ActorRole {
    RegisteredDriver,
    Partner,
    Admin,
    Anonymous,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: String,
    pub event_version: i32,
    pub schema_namespace: String,
    pub event_name: String,
    pub occurred_at: String,
    pub ingested_at: String,
    pub channel: Channel,
    pub session_id: String,
    pub correlation_id: Option<String>,
    pub anonymous_id: Option<String>,
    pub user_id: Option<String>,
    pub actor_role: Option<ActorRole>,
    pub path: Option<String>,
    pub payload: Option<serde_json::Value>,
    pub metadata: Option<serde_json::Value>,
}
