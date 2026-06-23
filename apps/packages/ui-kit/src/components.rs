use serde::{Deserialize, Serialize};

/// Defines the shape/layout of a skeleton placeholder
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SkeletonShape {
    Rect { width: u32, height: u32, radius: u32 },
    Circle { size: u32 },
    Text { lines: u32, line_height: u32 },
}

/// Configuration for a skeleton loader component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkeletonConfig {
    pub shapes: Vec<SkeletonShape>,
    pub animation: bool,
}

impl Default for SkeletonConfig {
    fn default() -> Self {
        Self {
            shapes: vec![
                SkeletonShape::Rect { width: 64, height: 64, radius: 8 },
                SkeletonShape::Text { lines: 3, line_height: 12 },
            ],
            animation: true,
        }
    }
}

/// Preview card skeleton for station cards
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkeletonCard {
    pub config: SkeletonConfig,
    pub spacing: u32,
}

impl Default for SkeletonCard {
    fn default() -> Self {
        Self {
            config: SkeletonConfig {
                shapes: vec![
                    SkeletonShape::Rect { width: 48, height: 48, radius: 8 },
                    SkeletonShape::Text { lines: 2, line_height: 12 },
                    SkeletonShape::Rect { width: 80, height: 20, radius: 4 },
                ],
                animation: true,
            },
            spacing: 12,
        }
    }
}

/// Determines which variant of skeleton to render for a given context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SkeletonVariant {
    Card(SkeletonCard),
    List(SkeletonConfig),
    MapMarker,
    TextBlock { lines: u32 },
}

/// Custom map marker definition by connector type and availability
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConnectorType {
    CCS,
    CHAdeMO,
    Type2,
}

/// Availability status color for markers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AvailabilityStatus {
    Available,    // green
    Limited,      // orange
    Unavailable,  // red
}

/// Map marker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapMarker {
    pub station_id: String,
    pub lat: f64,
    pub lng: f64,
    pub connector_type: ConnectorType,
    pub availability: AvailabilityStatus,
}

/// Station preview card data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationPreviewCard {
    pub station_id: String,
    pub name: String,
    pub address: String,
    pub distance_km: Option<f64>,
    pub connector_types: Vec<String>,
    pub available: bool,
    pub lat: f64,
    pub lng: f64,
}

/// Cluster marker for grouped stations at low zoom
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMarker {
    pub count: usize,
    pub lat: f64,
    pub lng: f64,
    pub station_ids: Vec<String>,
}
