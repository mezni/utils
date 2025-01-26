use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::{Duration, Utc};
use rand::thread_rng;
use rand::Rng;
use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration as StdDuration;

const BUFFER_SIZE: u32 = 20000;
const BATCH_SIZE: u32 = 10000;

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
    pub msg_type: String,
}

pub struct SyslogMessageBatch {
    buffer: HashMap<DateTime<Utc>, SyslogMessage>,
}

impl SyslogMessageBatch {
    // Constructor for SyslogMessageBatch that calls load
    pub fn new() -> Self {
        SyslogMessageBatch::load(BUFFER_SIZE)
    }

    // Load a batch of SyslogMessages
    pub fn load(count: u32) -> Self {
        let mut rng = thread_rng();
        let mut buffer: HashMap<DateTime<Utc>, SyslogMessage> = HashMap::new();

        for _ in 0..count {
            let now = Utc::now();
            let start_interval = now - Duration::minutes(1);
            let random_seconds = rng.gen_range(0..120);
            let start_ts = start_interval + Duration::seconds(random_seconds as i64);
            let duration = rng.gen_range(0..120);
            let end_ts = start_ts + Duration::seconds(duration as i64);

            let session_id = format!("{}", rng.gen_range(10000000..99999999));
            let source_ip_address = SyslogMessageBatch::generate_ip_address(&mut rng);
            let source_port = format!("{}", rng.gen_range(1024..65535));
            let dest_ip_address = SyslogMessageBatch::generate_ip_address(&mut rng);
            let dest_port = format!("{}", rng.gen_range(1024..65535));

            let msg_open = SyslogMessage {
                session_id: session_id.clone(),
                source_ip_address: source_ip_address.clone(),
                source_port: source_port.clone(),
                dest_ip_address: dest_ip_address.clone(),
                dest_port: dest_port.clone(),
                start_ts: start_ts.to_rfc3339_opts(SecondsFormat::Millis, true),
                end_ts: "".to_string(),
                duration: "".to_string(),
                msg_type: "open".to_string(),
            };

            let msg_close = SyslogMessage {
                session_id: session_id.clone(),
                source_ip_address: source_ip_address.clone(),
                source_port: source_port.clone(),
                dest_ip_address: dest_ip_address.clone(),
                dest_port: dest_port.clone(),
                start_ts: "".to_string(),
                end_ts: end_ts.to_rfc3339_opts(SecondsFormat::Millis, true),
                duration: format!("{}", duration),
                msg_type: "close".to_string(),
            };

            buffer.insert(start_ts, msg_open);
            buffer.insert(end_ts, msg_close);
        }

        SyslogMessageBatch { buffer }
    }

    // Helper function to generate random IP addresses
    fn generate_ip_address(rng: &mut rand::rngs::ThreadRng) -> String {
        format!(
            "{}.{}.{}.{}",
            rng.gen_range(1..256),
            rng.gen_range(0..256),
            rng.gen_range(0..256),
            rng.gen_range(0..256)
        )
    }

    // Extend the current batch with another batch of messages
    pub fn extend(&mut self, other: SyslogMessageBatch) {
        self.buffer.extend(other.buffer);
    }

    // Return the buffer length
    pub fn length(&self) -> usize {
        self.buffer.len()
    }

    pub fn generate(&mut self) -> Vec<SyslogMessage> {
        let mut messages = Vec::new();

        while messages.len() < BATCH_SIZE as usize {
            let now = Utc::now();
            let mut keys_to_remove: Vec<DateTime<Utc>> = self
                .buffer
                .keys()
                .filter(|&key| *key < now)
                .cloned()
                .collect();
            keys_to_remove.sort();
            for key in keys_to_remove
                .into_iter()
                .take(BATCH_SIZE as usize - messages.len())
            {
                if let Some(msg) = self.buffer.remove(&key) {
                    messages.push(msg);
                }
            }
            if self.buffer.len() < BUFFER_SIZE as usize {
                let additional_messages =
                    SyslogMessageBatch::load((BUFFER_SIZE - self.buffer.len() as u32) / 2);
                self.buffer.extend(additional_messages.buffer);
            }
            sleep(StdDuration::from_secs(1));
        }

        messages
    }
}
