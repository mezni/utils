use serde::{Serialize, Deserialize};
use chrono::{Utc, DateTime};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum StationStatus {
    Available,
    Occupied,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Charger {
    pub id: String,
    pub plug_type: String,
    pub power_output: u32,
    pub status: StationStatus,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Station {
    pub id: String,
    pub name: String,
    pub provider_id: String,
    pub provider_name: String,
    pub latitude: f64,
    pub longitude: f64,
    pub status: StationStatus,
    pub chargers: Vec<Charger>,
    pub updated_at: DateTime<Utc>,
}

pub fn generate_mock_data() -> Vec<Station> {
    vec![
        Station {
            id: "stn-e3b0c442".to_string(),
            name: "LES BERGES DU LAC 2 HUB".to_string(),
            provider_id: "prv-k9x2m47a".to_string(),
            provider_name: "TotalEnergies Tunisia".to_string(),
            latitude: 36.8324,
            longitude: 10.2321,
            status: StationStatus::Available,
            chargers: vec![
                Charger { id: "chg-7b2a19f4".to_string(), plug_type: "CCS2".to_string(), power_output: 120, status: StationStatus::Available },
            ],
            updated_at: Utc::now(),
        },
        Station {
            id: "stn-f4a1d553".to_string(),
            name: "TUNIS MARINE PLAZA".to_string(),
            provider_id: "prv-m1n8b52c".to_string(),
            provider_name: "Ola Energy".to_string(),
            latitude: 36.8010,
            longitude: 10.1912,
            status: StationStatus::Occupied,
            chargers: vec![
                Charger { id: "chg-3a1b2c3d".to_string(), plug_type: "CCS2".to_string(), power_output: 50, status: StationStatus::Occupied },
            ],
            updated_at: Utc::now(),
        }
    ]
}
