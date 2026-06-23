use domain_types::favorites::FavoriteItem;

/// FavoriteButton component definition
/// Describes the state of a favorite toggle button for a station
#[derive(Debug, Clone)]
pub struct FavoriteButton {
    pub station_id: String,
    pub is_favorited: bool,
    pub animating: bool,
}

impl FavoriteButton {
    pub fn new(station_id: &str, is_favorited: bool) -> Self {
        Self {
            station_id: station_id.to_string(),
            is_favorited,
            animating: false,
        }
    }

    pub fn with_animation(mut self, animating: bool) -> Self {
        self.animating = animating;
        self
    }

    pub fn toggle(&self) -> Self {
        Self {
            station_id: self.station_id.clone(),
            is_favorited: !self.is_favorited,
            animating: true,
        }
    }
}

/// FavoritesList component definition
/// Describes the state of a favorites list view
#[derive(Debug, Clone)]
pub struct FavoritesList {
    pub items: Vec<FavoriteItem>,
    pub loading: bool,
}

impl FavoritesList {
    pub fn new(items: Vec<FavoriteItem>) -> Self {
        Self {
            items,
            loading: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }
}
