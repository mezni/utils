#[derive(Debug)]
pub struct SyslogMessage {
    pub pri: String,
    pub version: u32,
    pub timestamp: String,
    pub hostname: String,
    pub app_name: String,
    pub sd_type: String,
    pub kv_pairs: Vec<(String, String)>,
}

// Function to parse key-value pairs from a message
pub fn parse_kv_pairs(message: &str) -> Result<Vec<(String, String)>, String> {
    let kv_pairs: Vec<(String, String)> = message
        .split_whitespace()
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            let value = parts.next()?;
            let value = value.trim_matches('"').to_string(); // Remove quotes around the value
            Some((key.to_string(), value))
        })
        .collect();

    Ok(kv_pairs)
}

// Function to parse a full syslog message string
pub fn parse_syslog_message(input: &str) -> Result<SyslogMessage, String> {
    let mut parts = input.splitn(8, |c| c == ' ' || c == '>' || c == '[' || c == ']');
    
    // Parse priority (pri)
    let pri = parts.next().ok_or("Missing priority")?
        .trim_start_matches('<')
        .trim_end_matches('>')
        .to_string();

    // Parse version (version) with error handling
    let version: u32 = parts.next().ok_or("Missing version")?.parse()
        .map_err(|_| "Invalid version format")?;
    
    // Parse timestamp (timestamp)
    let timestamp = parts.next().ok_or("Missing timestamp")?.to_string();
    let hostname = parts.next().ok_or("Missing hostname")?.to_string();
    let app_name = parts.next().ok_or("Missing app_name")?.to_string();
    
    // Skip over SD type
    parts.next(); 
    
    // Parse sd_type
    let sd_type = parts.next().ok_or("Missing SD type")?.to_string();

    // Parse the message part of the syslog
    let message = parts.next()
        .ok_or("Missing SD message")?
        .to_string()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string();

    // Parse the key-value pairs from the message
    let kv_pairs = parse_kv_pairs(&message)?;

    // Return the populated SyslogMessage struct
    Ok(SyslogMessage {
        pri,
        version,
        timestamp,
        hostname,
        app_name,
        sd_type,
        kv_pairs,
    })
}
