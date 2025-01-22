mod syslog_gen;

use chrono::Utc;
use rand::rngs::ThreadRng;
use rand::thread_rng;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::thread;
use std::time::Duration as StdDuration;

use syslog_gen::syslog::{delete_old_entries, SyslogMessage};

const LOOP_COUNT: usize = 1000;
const MAX_BUFFER_SIZE: usize = 1000;
const SLEEP_DURATION_SECS: u64 = 5;
const MAX_LINES_PER_FILE: usize = 5_000;

fn generate() {
    let mut rng: ThreadRng = thread_rng();
    let mut buffer_open: HashMap<String, SyslogMessage> = HashMap::new();
    let mut buffer_close: HashMap<String, SyslogMessage> = HashMap::new();
    let mut line_count: usize = 0;
    let mut file_index: usize = 1;

    // Create the first file
    let mut file_name = format!("syslog_{:02}.log", file_index);
    let mut file = File::create(&file_name).expect("Failed to create file");
    let mut writer = BufWriter::new(file);

    for _ in 0..LOOP_COUNT {
        let cnt = buffer_open.len();
        if cnt < MAX_BUFFER_SIZE {
            for _ in 0..MAX_BUFFER_SIZE - cnt {
                if line_count >= MAX_LINES_PER_FILE {
                    // Switch to a new file
                    writer.flush().expect("Failed to flush writer");
                    file_index += 1;
                    file_name = format!("syslog_{:02}.log", file_index);
                    file = File::create(&file_name).expect("Failed to create file");
                    writer = BufWriter::new(file);
                    line_count = 0; // Reset line count for the new file
                }

                let syslog_message = SyslogMessage::new(&mut rng);
                buffer_open.insert(
                    syslog_message
                        .start_ts
                        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                        .to_string(),
                    syslog_message.clone(),
                );
                buffer_close.insert(
                    syslog_message
                        .end_ts
                        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                        .to_string(),
                    syslog_message,
                );
            }
        } else {
            thread::sleep(StdDuration::from_secs(SLEEP_DURATION_SECS));
        }

        let now = Utc::now();
        let deleted_entries = delete_old_entries(now, &mut buffer_open, &mut buffer_close);
        for (_, sl) in &deleted_entries {
            if line_count >= MAX_LINES_PER_FILE {
                // Switch to a new file
                writer.flush().expect("Failed to flush writer");
                file_index += 1;
                file_name = format!("syslog_{:02}.log", file_index);
                file = File::create(&file_name).expect("Failed to create file");
                writer = BufWriter::new(file);
                line_count = 0; // Reset line count for the new file
            }

            writeln!(writer, "{}", sl).expect("Failed to write to file");
            line_count += 1;
        }

        writer.flush().expect("Failed to flush writer");
        thread::sleep(StdDuration::from_secs(1));
    }

    // Flush any remaining writes to ensure all data is saved
    writer.flush().expect("Failed to flush writer");

    println!("Finished writing logs to multiple files.");
}

fn main() {
    generate()
}
