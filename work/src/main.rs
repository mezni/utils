mod syslog_gen;
mod syslog_parser;

use chrono::Utc;
use rand::rngs::ThreadRng;
use rand::thread_rng;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufRead};
use std::error::Error;
use std::io::{Write, BufWriter};
use std::thread;
use std::time::Duration as StdDuration;
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{Schema, Field};
use syslog_gen::syslog::{SyslogMessage as GenSyslogMessage, delete_old_entries}; // Renaming to avoid conflict
use syslog_parser::message::SyslogMessage as ParserSyslogMessage;
use datafusion::execution::context::SessionContext; // Use SessionContext instead of ExecutionContext

use std::sync::Arc;
use tokio;


const LOOP_COUNT: usize = 1000;
const MAX_BUFFER_SIZE: usize = 1000;
const SLEEP_DURATION_SECS: u64 = 5;
const MAX_LINES_PER_FILE: usize = 5_000;

fn generate() {
    let mut rng: ThreadRng = thread_rng();
    let mut buffer_open: HashMap<String, GenSyslogMessage> = HashMap::new();
    let mut buffer_close: HashMap<String, GenSyslogMessage> = HashMap::new();
    let mut line_count: usize = 0;
    let mut file_index: usize = 1;

    let mut file_name = format!("syslog_{:02}.log", file_index);
    let mut file = File::create(&file_name).expect("Failed to create file");
    let mut writer = BufWriter::new(file);

    for _ in 0..LOOP_COUNT {
        let cnt = buffer_open.len();
        if cnt < MAX_BUFFER_SIZE {
            for _ in 0..MAX_BUFFER_SIZE - cnt {
                if line_count >= MAX_LINES_PER_FILE {
                    writer.flush().expect("Failed to flush writer");
                    file_index += 1;
                    file_name = format!("syslog_{:02}.log", file_index);
                    file = File::create(&file_name).expect("Failed to create file");
                    writer = BufWriter::new(file);
                    line_count = 0;
                }

                let syslog_message = GenSyslogMessage::new(&mut rng);
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
            if line_count >= MAX_LINES_PER_FILE {
                writer.flush().expect("Failed to flush writer");
                file_index += 1;
                file_name = format!("syslog_{:02}.log", file_index);
                file = File::create(&file_name).expect("Failed to create file");
                writer = BufWriter::new(file);
                line_count = 0;
            }

            writeln!(writer, "{}", sl).expect("Failed to write to file");
            line_count += 1;
        }

        thread::sleep(StdDuration::from_secs(1));
    }

    writer.flush().expect("Failed to flush writer");
    println!("Finished writing logs to multiple files.");
}


async fn read_and_parse_syslog_file(file_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    if !std::path::Path::new(file_name).exists() {
        return Err(format!("File {} not found", file_name).into());
    }
    
    let file = std::fs::File::open(file_name)?;
    let reader = std::io::BufReader::new(file);

    // Vectors to hold values for DataFrame columns
    let mut ts_values = Vec::new();
    let mut source_address_values = Vec::new();
    let mut source_port_values = Vec::new();

    for line in reader.lines() {
        let line = line?;
        match ParserSyslogMessage::parse_syslog(&line) {
            Ok(parsed_message) => {
                ts_values.push(parsed_message.ts.clone());

                let mut source_address = String::new();
                let mut source_port = String::new();

                for (key, value) in parsed_message.kv_pairs {
                    match key.as_str() {
                        "source-address" => source_address = value.to_string(),
                        "source-port" => source_port = value.to_string(),
                        _ => {}
                    }
                }

                source_address_values.push(source_address);
                source_port_values.push(source_port);
            }
            Err(err) => {
                println!("Failed to parse line: {}. Error: {}", line, err);
            }
        }
    }

    // Create Arrow Arrays from the vectors
    let ts_array: ArrayRef = Arc::new(StringArray::from(ts_values));
    let source_address_array: ArrayRef = Arc::new(StringArray::from(source_address_values));
    let source_port_array: ArrayRef = Arc::new(StringArray::from(source_port_values));

    // Create a schema for the DataFrame
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", datafusion::arrow::datatypes::DataType::Utf8, false),
        Field::new("source-address", datafusion::arrow::datatypes::DataType::Utf8, false),
        Field::new("source-port", datafusion::arrow::datatypes::DataType::Utf8, false),
    ]));

    // Initialize a SessionContext
    let mut ctx = SessionContext::new();

    // Use the create_dataframe_from_batches method for creating DataFrame
    let batch = datafusion::arrow::record_batch::RecordBatch::try_new(schema.clone(), vec![
        ts_array,
        source_address_array,
        source_port_array,
    ])?;

    let df = ctx.create_dataframe_from_batches(vec![batch])?;

    // Show the DataFrame
    df.show()?;

    Ok(())
}

fn main() {
    let file_name = "syslog_01.log"; 

    // Create a tokio runtime to run async code
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        if let Err(e) = read_and_parse_syslog_file(file_name).await {
            eprintln!("Error reading or parsing syslog file: {}", e);
        }
    });
}