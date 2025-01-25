mod generator;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use generator::syslog::SyslogMessage;
use generator::syslog::SyslogMessageBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use std::fs::File;
use std::sync::Arc;

// Assuming that you have SyslogMessage defined and it has the same fields as mentioned previously
pub fn vec_to_arrow(messages: Vec<SyslogMessage>) -> RecordBatch {
    // Convert Vec<SyslogMessage> into columns for Arrow format
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

    // Create Arrow Arrays
    let session_id_array = StringArray::from(session_ids);
    let source_ip_array = StringArray::from(source_ips);
    let source_port_array = StringArray::from(source_ports);
    let dest_ip_array = StringArray::from(dest_ips);
    let dest_port_array = StringArray::from(dest_ports);
    let start_ts_array = StringArray::from(start_ts);
    let end_ts_array = StringArray::from(end_ts);
    let duration_array = StringArray::from(durations);
    let msg_type_array = StringArray::from(msg_types);

    // Create Arrow Schema
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

    // Create RecordBatch
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(session_id_array) as Arc<dyn arrow::array::Array>,
            Arc::new(source_ip_array),
            Arc::new(source_port_array),
            Arc::new(dest_ip_array),
            Arc::new(dest_port_array),
            Arc::new(start_ts_array),
            Arc::new(end_ts_array),
            Arc::new(duration_array),
            Arc::new(msg_type_array),
        ],
    )
    .expect("Failed to create RecordBatch")
}

// Function to save RecordBatch to Parquet file
pub fn save_to_parquet(record_batch: &RecordBatch, file_path: &str) {
    // Create a file for writing Parquet data
    let file = File::create(file_path).expect("Unable to create file");

    // Set writer properties (optional)
    let properties = WriterProperties::builder().build();

    // Create the Parquet Arrow Writer
    let mut writer = ArrowWriter::try_new(file, record_batch.schema(), Some(properties))
        .expect("Unable to create Arrow writer");

    // Write the record batch to the Parquet file
    writer
        .write(&record_batch)
        .expect("Unable to write record batch to Parquet");

    // Finish the writer
    writer.close().expect("Unable to close Parquet writer");

    println!("Parquet file saved to {}", file_path);
}

fn main() {
    for i in 0..5 {
        let mut batch = SyslogMessageBatch::new();
        let messages = batch.generate();
        let record_batch = vec_to_arrow(messages);
        let file_path = format!("syslog_data_0{}.parquet", i + 1);

        save_to_parquet(&record_batch, &file_path);
    }
}
