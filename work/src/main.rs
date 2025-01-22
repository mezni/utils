mod syslog_gen;

use chrono::Utc;
use rand::rngs::ThreadRng;
use rand::thread_rng;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Write, BufWriter};
use std::thread;
use std::time::Duration as StdDuration;

use syslog_gen::syslog::{SyslogMessage, delete_old_entries};

const LOOP_COUNT: usize = 1000;
const MAX_BUFFER_SIZE: usize = 1000;
const SLEEP_DURATION_SECS: u64 = 5;
const MAX_LINES: usize = 20_000; // Limit to 20,000 lines

fn main() {
    let mut rng: ThreadRng = thread_rng();
    let mut buffer_open: HashMap<String, SyslogMessage> = HashMap::new();
    let mut buffer_close: HashMap<String, SyslogMessage> = HashMap::new();
    let mut line_count: usize = 0; // Counter for the number of lines written

    // Open a file for writing logs
    let file = File::create("syslog_output.log").expect("Failed to create file");
    let mut writer = BufWriter::new(file);

    for _ in 0..LOOP_COUNT {
        if line_count >= MAX_LINES {
            break;
        }

        let cnt = buffer_open.len();
        if cnt < MAX_BUFFER_SIZE {
            for _ in 0..MAX_BUFFER_SIZE - cnt {
                if line_count >= MAX_LINES {
                    break;
                }

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
            if line_count >= MAX_LINES {
                break;
            }

            writeln!(writer, "{}", sl).expect("Failed to write to file");
            line_count += 1;
        }

        if line_count >= MAX_LINES {
            break;
        }

        writer.flush().expect("Failed to flush writer");
        thread::sleep(StdDuration::from_secs(1));
    }

    // Flush any remaining writes to ensure all data is saved
    writer.flush().expect("Failed to flush writer");

    println!("Finished writing {} lines to the file.", line_count);
}
