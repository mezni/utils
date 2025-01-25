use chrono::{DateTime, Utc};
use rand::rngs::ThreadRng;
use rand::Rng;

#[derive(Debug, Clone)]
pub struct SyslogMessage {
    pub session_id: String,
    pub source_ip_address: String,
    pub source_port: String,
    pub dest_ip_address: String,
    pub dest_port: String,
    pub start_ts: String,
    pub end_ts: String,
    pub duration: String,
}

impl SyslogMessage {
    pub fn new(rng: &mut ThreadRng) -> Self {
        let now = Utc::now();
        let one_minute_ago = now - chrono::Duration::minutes(1);
        let random_seconds = rng.gen_range(0..120);
        let start_ts = one_minute_ago + chrono::Duration::seconds(random_seconds as i64);
        let duration = rng.gen_range(0..120);
        let end_ts = start_ts + chrono::Duration::seconds(duration as i64);

        SyslogMessage {
            session_id: rng.gen_range(100_000..999_999).to_string(),
            source_ip_address: Self::generate_ip_address(rng),
            source_port: rng.gen_range(1025..35000).to_string(),
            dest_ip_address: Self::generate_ip_address(rng),
            dest_port: rng.gen_range(1025..35000).to_string(),
            start_ts: start_ts.to_rfc3339(),
            end_ts: end_ts.to_rfc3339(),
            duration: duration.to_string(),
        }
    }

    fn generate_ip_address(rng: &mut ThreadRng) -> String {
        format!(
            "{}.{}.{}.{}",
            rng.gen_range(1..256),
            rng.gen_range(0..256),
            rng.gen_range(0..256),
            rng.gen_range(0..256)
        )
    }
}
