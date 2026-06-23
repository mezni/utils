// UI Kit - UI components package (NOT a frontend application)
// This is a contracts-like package for UI component definitions

pub mod components;

pub use components::{
    SkeletonShape, SkeletonConfig, SkeletonCard, SkeletonVariant,
    ConnectorType, AvailabilityStatus, MapMarker, StationPreviewCard, ClusterMarker,
};
