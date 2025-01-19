// Import necessary libraries
use rand::Rng;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use std::error::Error;

// Use the derive macro to automatically generate implementations for the Serialize and Deserialize traits
#[derive(Debug, Serialize, Deserialize)]
struct CDR {
    call_id: String,
    caller_number: String,
    callee_number: String,
    start_time: String,
    end_time: String,
    duration: u32,
}

impl CDR {
    // Function to generate a new CDR
    fn new() -> Self {
        // Generate random caller and callee numbers
        let caller_number = format!("{}{:06}", "+21650", rand::thread_rng().gen_range(0..1000000));
        let callee_number = format!("{}{:06}", "+21650", rand::thread_rng().gen_range(0..1000000));

        // Generate a random duration
        let duration = rand::thread_rng().gen_range(1..3600);

        // Generate random start and end time
        let start_time: DateTime<Utc> = Utc::now();
        let end_time = start_time + chrono::Duration::seconds(duration as i64);

        // Generate a unique call ID
        let call_id = Uuid::new_v4().to_string();

        // Return the new CDR
        CDR {
            call_id,
            caller_number,
            callee_number,
            start_time: start_time.to_rfc3339(),
            end_time: end_time.to_rfc3339(),
            duration,
        }
    }

    // Function to generate a list of CDRs
    fn generate_cdrs(n: usize) -> Vec<Self> {
        let mut cdrs = Vec::new();
        for _ in 0..n {
            let cdr = CDR::new();
            cdrs.push(cdr);
        }
        cdrs
    }
}

fn save_cdrs_to_csv(cdrs: Vec<CDR>, filename: &str) -> Result<(), Box<dyn Error>> {
    // Create a new CSV writer
    let mut wtr = csv::Writer::from_path(filename)?;

    // Write the CDRs to the CSV file
    for cdr in cdrs {
        wtr.serialize(cdr)?;
    }

    // Finalize the CSV writing
    wtr.flush()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    // Set the number of CDRs to generate
    let num_cdrs = 20000;

    // Generate the CDRs
    let cdrs = CDR::generate_cdrs(num_cdrs);

    // Save the CDRs to a CSV file
    let filename = "cdr_records.csv";
    save_cdrs_to_csv(cdrs, filename)?;

    println!("CDRs successfully written to {}", filename);
    Ok(())
}