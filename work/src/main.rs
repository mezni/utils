mod generator;
use generator::syslog::{SyslogMessage, SyslogMessageBatch};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use chrono::Local;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;
use tokio;

// Converts Vec<SyslogMessage> into RecordBatch
pub fn vec_to_arrow(messages: Vec<SyslogMessage>) -> RecordBatch {
    let session_ids: Vec<String> = messages.iter().map(|msg| msg.session_id.clone()).collect();
    let source_ips: Vec<String> = messages
        .iter()
        .map(|msg| msg.source_ip_address.clone())
        .collect();
    let source_ports: Vec<String> = messages.iter().map(|msg| msg.source_port.clone()).collect();
    let dest_ips: Vec<String> = messages
        .iter()
        .map(|msg| msg.dest_ip_address.clone())
        .collect();
    let dest_ports: Vec<String> = messages.iter().map(|msg| msg.dest_port.clone()).collect();
    let start_ts: Vec<String> = messages.iter().map(|msg| msg.start_ts.clone()).collect();
    let end_ts: Vec<String> = messages.iter().map(|msg| msg.end_ts.clone()).collect();
    let durations: Vec<String> = messages.iter().map(|msg| msg.duration.clone()).collect();
    let msg_types: Vec<String> = messages.iter().map(|msg| msg.msg_type.clone()).collect();

    let session_id_array = Arc::new(StringArray::from(session_ids));
    let source_ip_array = Arc::new(StringArray::from(source_ips));
    let source_port_array = Arc::new(StringArray::from(source_ports));
    let dest_ip_array = Arc::new(StringArray::from(dest_ips));
    let dest_port_array = Arc::new(StringArray::from(dest_ports));
    let start_ts_array = Arc::new(StringArray::from(start_ts));
    let end_ts_array = Arc::new(StringArray::from(end_ts));
    let duration_array = Arc::new(StringArray::from(durations));
    let msg_type_array = Arc::new(StringArray::from(msg_types));

    let schema = Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("source_ip_address", DataType::Utf8, false),
        Field::new("source_port", DataType::Utf8, false),
        Field::new("dest_ip_address", DataType::Utf8, false),
        Field::new("dest_port", DataType::Utf8, false),
        Field::new("start_ts", DataType::Utf8, false),
        Field::new("end_ts", DataType::Utf8, false),
        Field::new("duration", DataType::Utf8, false),
        Field::new("msg_type", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            session_id_array,
            source_ip_array,
            source_port_array,
            dest_ip_array,
            dest_port_array,
            start_ts_array,
            end_ts_array,
            duration_array,
            msg_type_array,
        ],
    )
    .expect("Failed to create RecordBatch")
}

// Saves RecordBatch to Parquet file
pub fn save_to_parquet(record_batch: &RecordBatch, file_path: &str) -> Result<(), std::io::Error> {
    let file = File::create(file_path)?;
    let properties = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, record_batch.schema(), Some(properties))?;

    writer.write(record_batch)?;
    writer.close()?;

    println!("Parquet file saved to {}", file_path);
    Ok(())
}

pub fn generate() -> Result<(), std::io::Error> {
    for i in 0..5 {
        let mut batch = SyslogMessageBatch::new();
        let messages = batch.generate();
        let record_batch = vec_to_arrow(messages);

        let timestamp = Local::now().format("%Y%m%d%H%M%S").to_string();
        let file_path = format!("MQ/INPUT/syslog_{}_0{}.parquet", timestamp, i + 1);

        save_to_parquet(&record_batch, &file_path)?;
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(err) = generate() {
        eprintln!("Error generating Parquet files: {}", err);
    }
}
