// Client Core - Transport-only package for API clients, auth, mappers, and offline support
// NO backend framework dependencies (actix-web, sqlx, tokio)
// Only reqwest, serde, serde_json, thiserror, anyhow

pub mod cache;
pub mod session;
pub mod telemetry;

pub use cache::{CacheEntry, CacheStore, PendingWrite, PendingAction, SyncQueue, CacheNamespace};
pub use session::{SessionState, MapRegion, SessionFilters};
pub use telemetry::{
    FavoriteEventPayload, SearchExecutedPayload, SearchSelectedPayload,
    FilterChangedPayload, OfflineModeEnteredPayload,
    favorite_added_payload, favorite_removed_payload,
    search_executed_payload, search_selected_payload,
    filter_changed_payload, offline_mode_payload,
};
