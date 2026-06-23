use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    pub q: String,
    pub lat: Option<f64>,
    pub lng: Option<f64>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub station_id: String,
    pub name: String,
    pub address: String,
    pub distance_km: Option<f64>,
    pub relevance: f64,
    pub connector_types: Vec<String>,
    pub available: bool,
    pub lat: f64,
    pub lng: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub data: Vec<SearchResult>,
    pub metadata: SearchMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchMetadata {
    pub query: String,
    pub total: usize,
    pub latency_ms: u64,
}
