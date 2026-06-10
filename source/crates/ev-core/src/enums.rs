use serde::{Deserialize, Serialize};

/// Type of EV connector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectorType {
    /// Type 2 AC connector (IEC 62196).
    Type2,
    /// Type 3 AC connector.
    Type3,
    /// Combined Charging System (DC).
    CCS,
    /// CHAdeMO DC connector.
    CHAdeMO,
}

impl ConnectorType {
    /// All valid connector type strings, for use in validation.
    pub fn valid_values() -> &'static [&'static str] {
        &["type2", "type3", "ccs", "chademo"]
    }
}

/// Operational status of a charger.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChargerStatus {
    /// Charger is idle and ready for use.
    Available,
    /// Charger is currently occupied.
    InUse,
    /// Charger is under maintenance.
    Maintenance,
    /// Charger is offline or unreachable.
    Offline,
}

impl ChargerStatus {
    /// All valid charger status strings, for use in validation.
    pub fn valid_values() -> &'static [&'static str] {
        &["available", "in_use", "maintenance", "offline"]
    }
}

/// Type of partner entity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PartnerType {
    /// Commercial or business partner.
    Business,
    /// Individual or personal partner.
    Personal,
}

/// Availability status of a station.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StationStatus {
    /// All chargers are operational.
    Available,
    /// Some chargers are unavailable.
    Partial,
    /// No chargers are operational.
    Unavailable,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_type_round_trip() {
        let variants = [
            (ConnectorType::Type2, "\"type2\""),
            (ConnectorType::Type3, "\"type3\""),
            (ConnectorType::CCS, "\"ccs\""),
            (ConnectorType::CHAdeMO, "\"chademo\""),
        ];
        for (variant, expected_json) in &variants {
            let json = serde_json::to_string(variant).unwrap();
            assert_eq!(&json, expected_json);
            let deserialized: ConnectorType = serde_json::from_str(&json).unwrap();
            assert_eq!(&deserialized, variant);
        }
    }

    #[test]
    fn charger_status_round_trip() {
        let variants = [
            (ChargerStatus::Available, "\"available\""),
            (ChargerStatus::InUse, "\"in_use\""),
            (ChargerStatus::Maintenance, "\"maintenance\""),
            (ChargerStatus::Offline, "\"offline\""),
        ];
        for (variant, expected_json) in &variants {
            let json = serde_json::to_string(variant).unwrap();
            assert_eq!(&json, expected_json);
            let deserialized: ChargerStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(&deserialized, variant);
        }
    }

    #[test]
    fn partner_type_round_trip() {
        let variants = [
            (PartnerType::Business, "\"business\""),
            (PartnerType::Personal, "\"personal\""),
        ];
        for (variant, expected_json) in &variants {
            let json = serde_json::to_string(variant).unwrap();
            assert_eq!(&json, expected_json);
            let deserialized: PartnerType = serde_json::from_str(&json).unwrap();
            assert_eq!(&deserialized, variant);
        }
    }

    #[test]
    fn station_status_round_trip() {
        let variants = [
            (StationStatus::Available, "\"available\""),
            (StationStatus::Partial, "\"partial\""),
            (StationStatus::Unavailable, "\"unavailable\""),
        ];
        for (variant, expected_json) in &variants {
            let json = serde_json::to_string(variant).unwrap();
            assert_eq!(&json, expected_json);
            let deserialized: StationStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(&deserialized, variant);
        }
    }

    #[test]
    fn unknown_connector_type_returns_error() {
        let result: Result<ConnectorType, _> = serde_json::from_str("\"invalid\"");
        assert!(result.is_err());
    }
}
