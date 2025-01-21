#[derive(Debug)]
struct SyslogMessage {
    pri: String,
    version: u32,
    ts: String,
    hostname: String,
    app_name: String,
    sd_type: String,
   kv_pairs: Vec<(String, Option<String>)>,
}

fn parse_kv_pairs(message: &str) -> Result<Vec<(String, Option<String>)>, String> {
    let kv_pairs: Vec<(String, Option<String>)> = message
        .split_whitespace()
        .map(|pair| {
            if let Some((key, value)) = pair.split_once('=') {
                (key.to_string(), Some(value.to_string()))
            } else {
                (pair.to_string(), None)
            }
        })
        .collect();
    
    Ok(kv_pairs)
}

fn parse_syslog(input: &str) -> Result<SyslogMessage, String> {
    let mut parts = input.splitn(8, |c| c == ' ' || c == '>' || c == '[' || c == ']');
    let pri = parts.next().unwrap().trim_start_matches('<').trim_end_matches('>').to_string();
    let version: u32 = parts.next().unwrap().parse().unwrap();
    let ts = parts.next().unwrap().to_string();
    let hostname = parts.next().unwrap().to_string();
    let app_name = parts.next().unwrap().to_string();
    parts.next(); // Consume the "-" character
    let sd_type = parts.next().unwrap().to_string();
    let message = parts.next().unwrap().to_string().trim_start_matches('[').trim_end_matches(']').to_string();

        
    let kv_pairs = parse_kv_pairs(&message)?;
        
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

fn main() {
    let input = r#"<14>1 2019-12-27T09:48:23.298Z YAOFW01 RT_FLOW - RT_FLOW_SESSION_CLOSE [junos@2636.1.1.1.2.28 x=y v=t ]"#;
    match parse_syslog(input) {
        Ok(msg) => println!("{:?}", msg),
        Err(err) => println!("Error: {}", err),
    }
}