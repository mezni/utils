use rand::Rng;
use serde::Deserialize;

const DEFAULT_COUNT: usize = 100;
const DEFAULT_DIGITS: usize = 6;

#[derive(Debug, Deserialize)]
pub struct GeneratorConfigList {
    pub customers: Vec<GeneratorConfig>,
}

#[derive(Debug, Deserialize)]
pub struct GeneratorConfig {
    pub customer_type: String,
    pub cc_ndc: Vec<DirectoryNumberConfig>,
    pub digits: Option<usize>,
    pub count: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct DirectoryNumberConfig {
    pub cc: String,
    pub ndc: Vec<u16>,
    pub mcc: Option<u16>,
    pub mnc: Option<u16>,
    pub perc: Option<u16>,
}

pub fn read_config(file_path: &str) -> Result<GeneratorConfigList, Box<dyn std::error::Error>> {
    // Read the YAML file
    let yaml_content = std::fs::read_to_string(file_path)?;

    // Parse the YAML content into the GeneratorConfigList struct
    let mut customer_list: GeneratorConfigList = serde_yaml::from_str(&yaml_content)?;

    // Process each customer
    for customer in &mut customer_list.customers {
        process_customer(customer);
    }

    Ok(customer_list)
}

pub fn process_customer(customer: &mut GeneratorConfig) {
    // Set default value for digits if missing, using the constant
    let digits = customer.digits.unwrap_or(DEFAULT_DIGITS);
    customer.digits = Some(digits);

    // Set default value for count if missing, using the constant
    let count = customer.count.unwrap_or(DEFAULT_COUNT);
    customer.count = Some(count);

    // Calculate perc per entry based on the number of cc_ndc entries
    let cc_ndc_count = customer.cc_ndc.len();
    let perc_per_entry = if cc_ndc_count > 0 { 100 / cc_ndc_count } else { 0 };

    // First pass: set perc and generate mcc, mnc values
    for cc_ndc in &mut customer.cc_ndc {
        // Generate a random 3-digit MCC if it is missing
        if cc_ndc.mcc.is_none() {
            cc_ndc.mcc = Some(generate_random_mcc());
        }

        // Generate a random 2-digit MNC if it is missing
        if cc_ndc.mnc.is_none() {
            cc_ndc.mnc = Some(generate_random_mnc());
        }

        // If perc is missing, set it to the calculated value
        if cc_ndc.perc.is_none() {
            cc_ndc.perc = Some(perc_per_entry.try_into().unwrap_or(0));
        }
    }

    // Second pass: Ensure perc sum is exactly 100
    let total_perc: u16 = customer.cc_ndc.iter().map(|cc_ndc| cc_ndc.perc.unwrap_or(0)).sum();
    let diff = 100 - total_perc;

    if diff != 0 {
        if diff > 0 {
            if let Some(first) = customer.cc_ndc.get_mut(0) {
                let new_perc = first.perc.unwrap_or(0) + diff;
                first.perc = Some(new_perc);
            }
        } else {
            if let Some(last) = customer.cc_ndc.last_mut() {
                let new_perc = last.perc.unwrap_or(0) + diff;
                last.perc = Some(new_perc);
            }
        }
    }
}

fn generate_random_mcc() -> u16 {
    let mut rng = rand::thread_rng();
    rng.gen_range(100..1000)
}

fn generate_random_mnc() -> u16 {
    let mut rng = rand::thread_rng();
    rng.gen_range(10..100)
}
