#[derive(Debug, Clone)]
pub struct Station {
    pub station_id: String,
    pub name: String,
}

impl Station {
    pub fn new(station_id: String, name: String) -> Self {
        Self { station_id, name }
    }
}
