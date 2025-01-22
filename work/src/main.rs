mod syslog_gen;

use chrono::Utc;
use rand::rngs::ThreadRng;
use rand::thread_rng;
use std::collections::HashMap;
use std::thread;
use std::time::Duration as StdDuration;

use syslog_gen::syslog::{SyslogMessage, delete_old_entries};

const LOOP_COUNT: usize = 1000;
const MAX_BUFFER_SIZE: usize = 1000;
const SLEEP_DURATION_SECS: u64 = 5;

fn main() {
    let mut rng: ThreadRng = thread_rng();
    let mut buffer_open: HashMap<String, SyslogMessage> = HashMap::new();
    let mut buffer_close: HashMap<String, SyslogMessage> = HashMap::new();

    for _ in 0..LOOP_COUNT {
        let cnt = buffer_open.len();
        if cnt < MAX_BUFFER_SIZE {
            for _ in 0..MAX_BUFFER_SIZE - cnt {
                let syslog_message = SyslogMessage::new(&mut rng);
                buffer_open.insert(
                    syslog_message.start_ts.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
                    syslog_message.clone(),
                );
                buffer_close.insert(
                    syslog_message.end_ts.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
                    syslog_message,
                );
            }
        } else {
            thread::sleep(StdDuration::from_secs(SLEEP_DURATION_SECS));
        }

        let now = Utc::now();
        let deleted_entries = delete_old_entries(now, &mut buffer_open, &mut buffer_close);
        for (_, sl) in &deleted_entries {
            //println!("{:?}", sl);
            println!("{}", sl);
        }
        thread::sleep(StdDuration::from_secs(1));
    }

    while buffer_open.len() > 0 || buffer_close.len() > 0 {
        let now = Utc::now();
        let deleted_entries = delete_old_entries(now, &mut buffer_open, &mut buffer_close);
        for (_, sl) in &deleted_entries {
            println!("{:?}", sl);
        }
        thread::sleep(StdDuration::from_secs(SLEEP_DURATION_SECS));
    }
}
