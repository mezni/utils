#[derive(Debug)]
struct SyslogMessage {
    pri: String,
    version: u32,
    ts: String,
    hostname: String,
    app_name: String,
    sd_type: String,
    kv_pairs: Vec<(String, String)>,
}

fn parse_kv_pairs(message: &str) -> Result<Vec<(String, String)>, String> {
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
    let input = r#"<14>1 2019-12-27T09:48:23.298Z YAOFW01 RT_FLOW - RT_FLOW_SESSION_CLOSE [junos@2636.1.1.1.2.28 reason="idle Timeout" source-address="10.40.186.212" source-port="38812" destination-address="41.202.217.132" destination-port="53" connection-tag="0" service-name="junos-dns-udp" nat-source-address="41.202.207.5" nat-source-port="23329" nat-destination-address="41.202.217.132" nat-destination-port="53" nat-connection-tag="0" src-nat-rule-type="source rule" src-nat-rule-name="rule_1" dst-nat-rule-type="N/A" dst-nat-rule-name="N/A" protocol-id="17" policy-name="Gi_TO_Untrust_1" source-zone-name="Gi-SZ" destination-zone-name="Untrust" session-id-32="94942576" packets-from-client="1" bytes-from-client="70" packets-from-server="1" bytes-from-server="130" elapsed-time="3" application="UNKNOWN" nested-application="UNKNOWN" username="N/A" roles="N/A" packet-incoming-interface="reth0.2572" encrypted="UNKNOWN"]"#;
    match parse_syslog(input) {
        Ok(msg) => println!("{:?}", msg),
        Err(err) => println!("Error: {}", err),
    }
}