#[derive(Debug)]
pub struct SyslogMessage {
    pub pri: String,
    pub version: u32,
    pub ts: String,
    pub hostname: String,
    pub app_name: String,
    pub sd_type: String,
    pub kv_pairs: Vec<(String, String)>,
}

impl SyslogMessage {
    /// Parses a key-value pair string into a vector of tuples.
    pub fn parse_kv_pairs(message: &str) -> Result<Vec<(String, String)>, String> {
        let kv_pairs: Vec<(String, String)> = message
            .split_whitespace()
            .filter_map(|pair| {
                let mut parts = pair.splitn(2, '=');
                let key = parts.next()?;
                let value = parts.next()?;
                let value = value.trim_matches('"').to_string(); // Remove quotes
                Some((key.to_string(), value))
            })
            .collect();

        Ok(kv_pairs)
    }

    /// Parses a syslog string into a `SyslogMessage` object.
    pub fn parse_syslog(input: &str) -> Result<SyslogMessage, String> {
        let mut parts = input.splitn(8, |c| c == ' ' || c == '>' || c == '[' || c == ']');
        let pri = parts
            .next()
            .unwrap()
            .trim_start_matches('<')
            .trim_end_matches('>')
            .to_string();
        let version: u32 = parts.next().unwrap().parse().unwrap();
        let ts = parts.next().unwrap().to_string();
        let hostname = parts.next().unwrap().to_string();
        let app_name = parts.next().unwrap().to_string();
        parts.next(); // Consume the "-" character
        let sd_type = parts.next().unwrap().to_string();
        let message = parts
            .next()
            .unwrap()
            .to_string()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();

        let kv_pairs = Self::parse_kv_pairs(&message)?;

        Ok(SyslogMessage {
            pri,
            version,
            ts,
            hostname,
            app_name,
            sd_type,
            kv_pairs,
        })
    }
}
