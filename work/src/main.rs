use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio; // Ensure tokio is included

#[tokio::main]
async fn main() -> Result<()> {
    // Define syslog configuration
    let mut syslog_config: HashMap<String, (String, Vec<String>)> = HashMap::new();

    syslog_config.insert(
        "CLOSE".to_string(),
        (
            "RT_FLOW_SESSION_CLOSE".to_string(),
            vec![
                "source-address".to_string(),
                "source-port".to_string(),
                "destination-address".to_string(),
                "destination-port".to_string(),
            ],
        ),
    );

    syslog_config.insert(
        "OPEN".to_string(),
        (
            "RT_FLOW_SESSION_OPEN".to_string(),
            vec![
                "source-address".to_string(),
                "source-port".to_string(),
                "destination-address".to_string(),
                "destination-port".to_string(),
            ],
        ),
    );

    let inputs = vec![
        r#"<14>1 2019-12-27T09:48:23.298Z YAOFW01 RT_FLOW - RT_FLOW_SESSION_CLOSE [junos@2636.1.1.1.2.28 reason="idle Timeout" source-address="10.40.186.212" source-port="38812" destination-address="41.202.217.132" destination-port="53" connection-tag="0" service-name="junos-dns-udp" nat-source-address="41.202.207.5" nat-source-port="23329" nat-destination-address="41.202.217.132" nat-destination-port="53" nat-connection-tag="0" src-nat-rule-type="source rule" src-nat-rule-name="rule_1" dst-nat-rule-type="N/A" dst-nat-rule-name="N/A" protocol-id="17" policy-name="Gi_TO_Untrust_1" source-zone-name="Gi-SZ" destination-zone-name="Untrust" session-id-32="94942576" packets-from-client="1" bytes-from-client="70" packets-from-server="1" bytes-from-server="130" elapsed-time="3" application="UNKNOWN" nested-application="UNKNOWN" username="N/A" roles="N/A" packet-incoming-interface="reth0.2572" encrypted="UNKNOWN"]"#,
        r#"<14>1 2019-12-28T10:15:10.123Z YAOFW02 RT_FLOW - RT_FLOW_SESSION_OPEN [junos@2636.1.1.1.2.29 reason="new connection" source-address="192.168.1.1" source-port="20000" destination-address="10.1.1.1" destination-port="80" connection-tag="1" service-name="http" protocol-id="6" policy-name="Policy_1" application="HTTP" elapsed-time="120" username="user1" session-id-32="94942588"]"#,
        // Add more inputs as needed
    ];

    // Define schema for the syslog data
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
    ]));

    let mut timestamps: Vec<String> = Vec::new();
    let mut session_ids: Vec<String> = Vec::new();

    let mut input_batch: Vec<&str> = Vec::new();
    let batch_size = 100;

    // Process syslog inputs in batches of 100
    for input in inputs {
        input_batch.push(input);

        // Once we have 100 inputs, process them and generate a DataFrame
        if input_batch.len() == batch_size {
            process_batch(&input_batch, &syslog_config, &mut timestamps, &mut session_ids)?;

            // Convert to Arrow arrays
            let timestamp_array = StringArray::from(timestamps.clone());
            let session_id_array = StringArray::from(session_ids.clone());

            // Create Arrow RecordBatch
            let data = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(timestamp_array) as ArrayRef,
                    Arc::new(session_id_array) as ArrayRef,
                ],
            )?;

            // Do something with the RecordBatch, e.g., print it
            println!("RecordBatch created with {} rows", data.num_rows());

            // Use SessionContext to work with the data
            let ctx = SessionContext::new();
            let df = ctx.read_batch(data)?; // Use read_batch instead of create_dataframe
            df.clone().show_limit(10).await?;

            // Clear the batch for next set of 100 inputs
            input_batch.clear();
            timestamps.clear();
            session_ids.clear();
        }
    }

    // If there are remaining inputs less than 100
    if !input_batch.is_empty() {
        process_batch(&input_batch, &syslog_config, &mut timestamps, &mut session_ids)?;

        // Convert to Arrow arrays
        let timestamp_array = StringArray::from(timestamps.clone());
        let session_id_array = StringArray::from(session_ids.clone());

        // Create Arrow RecordBatch
        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(timestamp_array) as ArrayRef,
                Arc::new(session_id_array) as ArrayRef,
            ],
        )?;

        // Do something with the RecordBatch, e.g., print it
        println!("RecordBatch created with {} rows", data.num_rows());

        // Use SessionContext to work with the data
        let ctx = SessionContext::new();
        let df = ctx.read_batch(data)?; // Use read_batch instead of create_dataframe
        df.clone().show_limit(10).await?;
    }

    Ok(())
}

// Function to process a batch of syslog input
fn process_batch(
    input_batch: &[&str],
    syslog_config: &HashMap<String, (String, Vec<String>)>,
    timestamps: &mut Vec<String>,
    session_ids: &mut Vec<String>,
) -> Result<(), DataFusionError> {
    for input in input_batch {
        match process_syslog(input, syslog_config, timestamps, session_ids) {
            Ok(()) => {
                println!("Timestamp extracted: {}", timestamps.last().unwrap());
                println!("Session ID extracted: {}", session_ids.last().unwrap());
            }
            Err(err) => {
                // Convert String error to DataFusionError
                return Err(DataFusionError::Execution(err));
            }
        }
    }
    Ok(())
}

// Function to process syslog input
fn process_syslog(
    input: &str,
    syslog_config: &HashMap<String, (String, Vec<String>)>,
    timestamps: &mut Vec<String>,
    session_ids: &mut Vec<String>,
) -> Result<(), String> {
    // Extract the timestamp from the syslog input (assumed format: "YYYY-MM-DDTHH:MM:SS.sssZ")
    if let Some(timestamp_start) = input.find('T') {
        if let Some(timestamp_end) = input[timestamp_start..].find('Z') {
            let timestamp = &input[timestamp_start - 10..timestamp_start + timestamp_end + 1];
            timestamps.push(timestamp.to_string());
        } else {
            return Err("Timestamp not found.".to_string());
        }
    } else {
        return Err("Timestamp not found.".to_string());
    }

    // Extract the session-id-32 from the syslog input
    if let Some(session_id_start) = input.find("session-id-32=") {
        let session_id_start = session_id_start + 14; // length of "session-id-32="
        if let Some(session_id_end) = input[session_id_start..].find(' ') {
            let session_id = &input[session_id_start..session_id_start + session_id_end];
            session_ids.push(session_id.to_string());
        } else if let Some(session_id_end) = input[session_id_start..].find(']') {
            let session_id = &input[session_id_start..session_id_start + session_id_end];
            session_ids.push(session_id.to_string());
        } else {
            return Err("Session ID not found.".to_string());
        }
    } else {
        return Err("Session ID not found.".to_string());
    }

    // Process event if required (example, processing CLOSE/OPEN events)
    for (key, (event_type, fields)) in syslog_config {
        if input.contains(event_type) {
            println!("Processing event: {}", key);
            // Extract fields from the input (mocked for now)
            for field in fields {
                if input.contains(field) {
                    println!("Found field: {}", field);
                }
            }
            return Ok(());
        }
    }

    Err("No matching event type found.".to_string())
}
