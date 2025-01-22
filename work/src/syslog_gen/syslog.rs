use chrono::{DateTime, Utc};
use rand::Rng;
use rand::rngs::ThreadRng;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone)]
pub struct SyslogMessage {
    pub session_id: u32,
    pub source_ip_address: String,
    pub source_port: u16,
    pub dest_ip_address: String,
    pub dest_port: u16,
    pub start_ts: DateTime<Utc>,
    pub end_ts: DateTime<Utc>,
    pub duration: u64,
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
            session_id: rng.gen_range(100_000..999_999),
            source_ip_address: Self::generate_ip_address(rng),
            source_port: rng.gen_range(1025..35000),
            dest_ip_address: Self::generate_ip_address(rng),
            dest_port: rng.gen_range(1025..35000),
            start_ts,
            end_ts,
            duration,
        }
    }

    fn generate_ip_address(rng: &mut ThreadRng) -> String {
        format!("{}.{}.{}.{}",
                 rng.gen_range(1..256),
                 rng.gen_range(0..256),
                 rng.gen_range(0..256),
                 rng.gen_range(0..256))
    }

    pub fn to_string(&self, event_type: &str) -> String {
        format!(
            "<14>1 {} YAOFW01 RT_FLOW - {} [junos@2636.1.1.1.2.28 reason=\"idle Timeout\" source-address=\"{}\" source-port=\"{}\"]",
            self.start_ts.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            event_type,
            self.source_ip_address,
            self.source_port
        )
    }
}

pub fn delete_old_entries(
    now: DateTime<Utc>,
    buffer_open: &mut HashMap<String, SyslogMessage>,
    buffer_close: &mut HashMap<String, SyslogMessage>,
) -> BTreeMap<DateTime<Utc>, String> {
    let mut deleted_entries: BTreeMap<DateTime<Utc>, String> = BTreeMap::new();

    buffer_open.retain(|ts, sl| {
        let ts_dt = DateTime::parse_from_rfc3339(ts).unwrap().with_timezone(&Utc);
        if ts_dt < now {
            let val = sl.to_string("RT_FLOW_SESSION_OPEN");
            deleted_entries.insert(ts_dt, val);
            false
        } else {
            true
        }
    });

    buffer_close.retain(|ts, sl| {
        let ts_dt = DateTime::parse_from_rfc3339(ts).unwrap().with_timezone(&Utc);
        if ts_dt < now {
            let val = sl.to_string("RT_FLOW_SESSION_CLOSE");
            deleted_entries.insert(sl.start_ts, val);
            false
        } else {
            true
        }
    });

    deleted_entries
}
