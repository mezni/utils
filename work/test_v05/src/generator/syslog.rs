use chrono::{DateTime, Duration, Utc};
use rand::{thread_rng, Rng};
use serde::Serialize;
use std::cmp;

const BATCH_SIZE: u32 = 10000;

#[derive(Serialize, Debug, Clone)]
pub struct SyslogMessage {
    pub session_id: String,
    pub source_ip_address: String,
    pub source_port: String,
    pub dest_ip_address: String,
    pub dest_port: String,
    pub start_ts: DateTime<Utc>,
    pub end_ts: DateTime<Utc>,
    pub duration: Duration,
    pub msg_type: String,
}

pub struct SyslogMessageBatch {
    pub open: Vec<SyslogMessage>,
    pub close: Vec<SyslogMessage>,
}

impl SyslogMessageBatch {
    pub fn new() -> Self {
        Self {
            open: Vec::new(),
            close: Vec::new(),
        }
    }

    pub async fn load(&mut self, count: u32) -> Result<(), String> {
        let mut rng = thread_rng();

        for _ in 0..count {
            let now = Utc::now();
            let start_interval = now - Duration::minutes(2);
            let random_seconds = rng.gen_range(0..120);
            let start_ts = start_interval + Duration::seconds(random_seconds as i64);
            let duration = rng.gen_range(0..180);
            let end_ts = start_ts + Duration::seconds(duration as i64);

            let session_id = format!("{}", rng.gen_range(10000000..99999999));
            let source_ip_address = Self::generate_ip_address(&mut rng);
            let source_port = format!("{}", rng.gen_range(1024..65535));
            let dest_ip_address = Self::generate_ip_address(&mut rng);
            let dest_port = format!("{}", rng.gen_range(1024..65535));

            let msg_open = SyslogMessage {
                session_id: session_id.clone(),
                source_ip_address: source_ip_address.clone(),
                source_port: source_port.clone(),
                dest_ip_address: dest_ip_address.clone(),
                dest_port: dest_port.clone(),
                start_ts,
                end_ts: Utc::now(),
                duration: Duration::seconds(0),
                msg_type: "open".to_string(),
            };

            let msg_close = SyslogMessage {
                session_id: session_id.clone(),
                source_ip_address: source_ip_address.clone(),
                source_port: source_port.clone(),
                dest_ip_address: dest_ip_address.clone(),
                dest_port: dest_port.clone(),
                start_ts: Utc::now(),
                end_ts,
                duration: Duration::seconds(duration as i64),
                msg_type: "close".to_string(),
            };

            self.open.push(msg_open);
            self.close.push(msg_close);
        }

        Ok(())
    }

    fn generate_ip_address(rng: &mut rand::rngs::ThreadRng) -> String {
        format!(
            "{}.{}.{}.{}",
            rng.gen_range(1..256),
            rng.gen_range(0..256),
            rng.gen_range(0..256),
            rng.gen_range(0..256)
        )
    }

    pub async fn generate(&mut self) -> Result<(Vec<SyslogMessage>, Vec<SyslogMessage>), String> {
        let now = Utc::now();
        let mut open_out = Vec::new();
        let mut close_out = Vec::new();

        self.open.retain(|item| {
            if item.start_ts < now {
                open_out.push(item.clone());
                false
            } else {
                true
            }
        });

        self.close.retain(|item| {
            if item.end_ts < now {
                close_out.push(item.clone());
                false
            } else {
                true
            }
        });

        let additional_count = BATCH_SIZE - cmp::min(open_out.len(), close_out.len()) as u32;
        if additional_count > 0 {
            self.load(additional_count).await?;
        }

        Ok((open_out, close_out))
    }
}
