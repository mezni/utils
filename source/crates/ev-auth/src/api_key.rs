pub struct ApiKey {
    pub prefix: String,
    pub value: String,
}

impl ApiKey {
    pub fn from_header(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.splitn(2, '.').collect();
        if parts.len() != 2 {
            return None;
        }
        Some(ApiKey {
            prefix: parts[0].to_string(),
            value: parts[1].to_string(),
        })
    }
}
