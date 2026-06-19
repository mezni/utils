use validator::Validate;

#[derive(Debug, Clone, Validate)]
pub struct ChargerValidation {
    pub station_id: String,
    pub connector_type_id: i32,
    pub status_id: i32,
    pub current_type_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub power_kw: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voltage: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amperage: Option<i32>,
    pub count_available: i32,
    pub count_total: i32,
}

impl ChargerValidation {
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if self.station_id.is_empty() {
            errors.push("station_id is required".to_string());
        }

        if self.connector_type_id < 1 || self.connector_type_id > 10 {
            errors.push("connector_type_id must be between 1 and 10".to_string());
        }

        if self.status_id < 1 || self.status_id > 10 {
            errors.push("status_id must be between 1 and 10".to_string());
        }

        if self.current_type_id < 1 || self.current_type_id > 10 {
            errors.push("current_type_id must be between 1 and 10".to_string());
        }

        if let Some(power) = self.power_kw {
            if power < 0.0 || power > 1000.0 {
                errors.push("power_kw must be between 0 and 1000".to_string());
            }
        }

        if let Some(voltage) = self.voltage {
            if voltage < 0 || voltage > 1000 {
                errors.push("voltage must be between 0 and 1000".to_string());
            }
        }

        if let Some(amperage) = self.amperage {
            if amperage < 0 || amperage > 500 {
                errors.push("amperage must be between 0 and 500".to_string());
            }
        }

        if self.count_available < 0 {
            errors.push("count_available must be non-negative".to_string());
        }

        if self.count_total < 1 {
            errors.push("count_total must be at least 1".to_string());
        }

        if self.count_available > self.count_total {
            errors.push(format!("count_available ({}) cannot exceed count_total ({})", self.count_available, self.count_total));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

pub fn validate_charger(request: &crate::CreateChargerRequest) -> Result<(), Vec<String>> {
    let validation = ChargerValidation {
        station_id: request.station_id.clone(),
        connector_type_id: request.connector_type_id,
        status_id: request.status_id,
        current_type_id: request.current_type_id,
        power_kw: request.power_kw,
        voltage: request.voltage,
        amperage: request.amperage,
        count_available: request.count_available,
        count_total: request.count_total,
    };

    validation.validate()
}
