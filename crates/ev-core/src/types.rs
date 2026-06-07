use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectorType {
    Type2,
    Type2Combo,
    Chademo,
    CCS,
    Schuko,
    Wall,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChargerStatus {
    Available,
    Charging,
    Offline,
    Maintenance,
    Reserved,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AvailabilityStatus {
    Public,
    Private,
    Restricted,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_type_serialization() {
        let ct = ConnectorType::Type2;
        let json = serde_json::to_string(&ct).unwrap();
        let deserialized: ConnectorType = serde_json::from_str(&json).unwrap();
        assert_eq!(ct, deserialized);
    }

    #[test]
    fn test_charger_status_serialization() {
        let cs = ChargerStatus::Available;
        let json = serde_json::to_string(&cs).unwrap();
        let deserialized: ChargerStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(cs, deserialized);
    }

    #[test]
    fn test_availability_status_serialization() {
        let as_ = AvailabilityStatus::Public;
        let json = serde_json::to_string(&as_).unwrap();
        let deserialized: AvailabilityStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(as_, deserialized);
    }
}
