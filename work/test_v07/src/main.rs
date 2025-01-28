use chrono::{DateTime, Duration, Utc};
use rand::Rng;
use rand::thread_rng;
use serde::Serialize;
use std::cmp;
use std::sync::Arc;
use datafusion::arrow::{
    array::{Float64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::prelude::*;
use datafusion::datasource::memory::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::dataframe::DataFrameWriteOptions;
const BATCH_SIZE: u32 = 2000;

#[derive(Serialize, Debug, Clone)]
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
        let mut rng = rand::thread_rng();

        for _ in 0..count {
            let now = Utc::now();
            let start_interval = now - Duration::minutes(1);
            let random_seconds = rng.gen_range(0..120);
            let start_ts = start_interval + Duration::seconds(random_seconds as i64);
            let duration = rng.gen_range(0..120);
            let end_ts = start_ts + Duration::seconds(duration as i64);

            let session_id = format!("{}", rng.gen_range(10000000..99999999));
            let source_ip_address = Self::generate_ip_address(&mut rng);
            let source_port = format!("{}", rng.gen_range(1024..65535));
            let dest_ip_address = Self::generate_ip_address(&mut rng);
            let dest_port = format!("{}", rng.gen_range(1024..65535));

            let start_ts_str = start_ts.to_rfc3339();
            let end_ts_str = end_ts.to_rfc3339();
            let duration_str = format!("{}", duration);

            let msg_open = SyslogMessage {
                session_id: session_id.clone(),
                source_ip_address: source_ip_address.clone(),
                source_port: source_port.clone(),
                dest_ip_address: dest_ip_address.clone(),
                dest_port: dest_port.clone(),
                start_ts: start_ts_str,
                end_ts: Utc::now().to_rfc3339(),
                duration: "0".to_string(),
                msg_type: "open".to_string(),
            };

            let msg_close = SyslogMessage {
                session_id: session_id.clone(),
                source_ip_address: source_ip_address.clone(),
                source_port: source_port.clone(),
                dest_ip_address: dest_ip_address.clone(),
                dest_port: dest_port.clone(),
                start_ts: Utc::now().to_rfc3339(),
                end_ts: end_ts_str,
                duration: duration_str,
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

    pub async fn generate_chunk(
        &mut self,
    ) -> Result<(Vec<SyslogMessage>, Vec<SyslogMessage>), String> {
        let now = Utc::now();
        let mut open_out = Vec::new();
        let mut close_out = Vec::new();

        let mut i = 0;
        while i < self.open.len() {
            let start_ts = DateTime::parse_from_rfc3339(&self.open[i].start_ts).unwrap();
            if Utc::now() > start_ts {
                // Use swap_remove to efficiently remove and push to the output
                open_out.push(self.open.swap_remove(i));
            } else {
                i += 1;
            }
        }

        let mut j = 0;
        while j < self.close.len() {
            let end_ts = DateTime::parse_from_rfc3339(&self.close[j].end_ts).unwrap();
            if Utc::now() > end_ts {
                // Use swap_remove to efficiently remove and push to the output
                close_out.push(self.close.swap_remove(j));
            } else {
                j += 1;
            }
        }

        let additional_count = BATCH_SIZE - cmp::min(open_out.len(), close_out.len()) as u32;
        if additional_count > 0 {
            self.load(additional_count).await?;
        }

        Ok((open_out, close_out))
    }


    pub async fn generate(&mut self) -> Result<(Vec<SyslogMessage>, Vec<SyslogMessage>), String> {
        let mut all_open_out = Vec::new();
        let mut all_close_out = Vec::new();
        let iterations = 15;
        for _ in 0..iterations {
            let (open_out, close_out) = self.generate_chunk().await.unwrap();
            // Clone the vectors to avoid moving the original values
            all_open_out.extend(open_out.clone());
            all_close_out.extend(close_out.clone());
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        Ok((all_open_out, all_close_out))
    }
}


pub fn create_string_arrays_from_messages(
        input: &Vec<SyslogMessage>,
    ) -> (StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray, StringArray) {
        let session_ids = StringArray::from(
            input.iter().map(|msg| msg.session_id.clone()).collect::<Vec<String>>()
        );
        let source_ips = StringArray::from(
            input.iter().map(|msg| msg.source_ip_address.clone()).collect::<Vec<String>>()
        );
        let source_ports = StringArray::from(
            input.iter().map(|msg| msg.source_port.clone()).collect::<Vec<String>>()
        );
        let dest_ips = StringArray::from(
            input.iter().map(|msg| msg.dest_ip_address.clone()).collect::<Vec<String>>()
        );
        let dest_ports = StringArray::from(
            input.iter().map(|msg| msg.dest_port.clone()).collect::<Vec<String>>()
        );
        let start_ts = StringArray::from(
            input.iter().map(|msg| msg.start_ts.clone()).collect::<Vec<String>>()
        );
        let end_ts = StringArray::from(
            input.iter().map(|msg| msg.end_ts.clone()).collect::<Vec<String>>()
        );
        let durations = StringArray::from(
            input.iter().map(|msg| msg.duration.clone()).collect::<Vec<String>>()
        );
        let msg_types = StringArray::from(
            input.iter().map(|msg| msg.msg_type.clone()).collect::<Vec<String>>()
        );

        (
            session_ids,
            source_ips,
            source_ports,
            dest_ips,
            dest_ports,
            start_ts,
            end_ts,
            durations,
            msg_types,
        )
    }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Define schema using Arrow
    let schema = Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("source_ip_address", DataType::Utf8, false),
        Field::new("source_port", DataType::Utf8, false),
        Field::new("dest_ip_address", DataType::Utf8, false),
        Field::new("dest_port", DataType::Utf8, false),
        Field::new("start_ts", DataType::Utf8, false), // Adjust the data type as needed
        Field::new("end_ts", DataType::Utf8, false),   // Adjust the data type as needed
        Field::new("duration", DataType::Utf8, false),
        Field::new("msg_type", DataType::Utf8, false),
    ]));

    // Create SyslogMessageBatch and load data
    let mut batch = SyslogMessageBatch::new();
    batch.load(BATCH_SIZE).await.unwrap();

    // Generate open and close syslog messages
    let (open_out, close_out) = batch.generate().await.unwrap();

let (session_ids, source_ips, source_ports, dest_ips, dest_ports, start_ts, end_ts, durations, msg_types) =
    SyslogMessageBatch::create_string_arrays_from_messages(&open_out);

    // Create the RecordBatch
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(session_ids),
        Arc::new(source_ips),
        Arc::new(source_ports),
        Arc::new(dest_ips),
        Arc::new(dest_ports),
        Arc::new(start_ts),
        Arc::new(end_ts),
        Arc::new(durations),
        Arc::new(msg_types),
    ])
    .map_err(|e| DataFusionError::ArrowError(e, None))?;

    // Step 3: Create a MemTable
    let table = MemTable::try_new(schema, vec![vec![batch]])?;

    // Step 4: Create a SessionContext and register the table
    let ctx = SessionContext::new();
    ctx.register_table("my_table", Arc::new(table))?;

    // Step 5: Query the DataFrame
    let df = ctx.sql("SELECT * FROM my_table").await?;
    df.clone().show().await?;

    let target_path =  "data.parquet";
    df.write_parquet(
        target_path,
        DataFrameWriteOptions::new(),
        None, // writer_options
    ).await;

    Ok(())
}
