mod config;  // Declare the config module

use config::{read_config, process_customer};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read and process the configuration
    let mut customer_list = read_config("config.yaml")?;

    // Iterate over each customer in the customer list and process
    for customer in &mut customer_list.customers {
        // Process each customer (e.g., validate or update fields)
        process_customer(customer);

        // Print the cc, mcc, mnc, and ndc of each DirectoryNumberConfig in cc_ndc
        for cc_ndc in &customer.cc_ndc {
            let ndc_str = cc_ndc.ndc.iter().map(|&ndc| ndc.to_string()).collect::<Vec<String>>().join(", ");
            println!("CC: {}, MCC: {}, MNC: {}, NDC: {}", cc_ndc.cc, cc_ndc.mcc.unwrap_or_default(), cc_ndc.mnc.unwrap_or_default(), ndc_str);
        }
    }

    Ok(())
}


