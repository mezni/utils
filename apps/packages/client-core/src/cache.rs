use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Cache entry with timestamp for stale-while-revalidate strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry<T> {
    pub data: T,
    pub cached_at: DateTime<Utc>,
    pub stale_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl<T> CacheEntry<T> {
    pub fn new(data: T, ttl_secs: i64, stale_ttl_secs: i64) -> Self {
        let now = Utc::now();
        Self {
            data,
            cached_at: now,
            stale_at: now + chrono::Duration::seconds(ttl_secs),
            expires_at: now + chrono::Duration::seconds(ttl_secs + stale_ttl_secs),
        }
    }

    pub fn is_stale(&self) -> bool {
        Utc::now() >= self.stale_at
    }

    pub fn is_expired(&self) -> bool {
        Utc::now() >= self.expires_at
    }
}

/// Pending write operation queued for sync
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingWrite {
    pub action: PendingAction,
    pub timestamp: DateTime<Utc>,
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PendingAction {
    AddFavorite { station_id: String },
    RemoveFavorite { station_id: String },
    UpdatePreferences { prefs: serde_json::Value },
}

/// Offline sync queue tracking pending operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncQueue {
    pub pending_writes: Vec<PendingWrite>,
    pub last_sync_at: Option<DateTime<Utc>>,
}

impl SyncQueue {
    pub fn new() -> Self {
        Self {
            pending_writes: vec![],
            last_sync_at: None,
        }
    }

    pub fn enqueue(&mut self, action: PendingAction) {
        let id = uuid::Uuid::new_v4().to_string();
        self.pending_writes.push(PendingWrite {
            action,
            timestamp: Utc::now(),
            id,
        });
    }

    pub fn dequeue(&mut self) -> Option<PendingWrite> {
        if self.pending_writes.is_empty() {
            None
        } else {
            Some(self.pending_writes.remove(0))
        }
    }
}

/// Offline cache store types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CacheNamespace {
    Stations,
    Favorites,
    Preferences,
    MapTiles,
}

/// Cache store key-value pair
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStore {
    pub stations: Option<CacheEntry<Vec<serde_json::Value>>>,
    pub favorites: Option<CacheEntry<Vec<serde_json::Value>>>,
    pub preferences: Option<CacheEntry<serde_json::Value>>,
    pub sync_queue: SyncQueue,
}

impl Default for CacheStore {
    fn default() -> Self {
        Self {
            stations: None,
            favorites: None,
            preferences: None,
            sync_queue: SyncQueue::new(),
        }
    }
}
