use rand::Rng;
use std::sync::{Arc, Mutex};

struct Customer {
    id: u32,
    customer_type: String,
    msisdn: String,
    imsi: String,
    imei: String,
}

impl Customer {
    fn new(customer_type: String, msisdn: String, imsi: String, imei: String) -> Self {
        let id = Customer::generate_id();
        Customer {
            id,
            customer_type,
            msisdn,
            imsi,
            imei,
        }
    }
    fn generate_id() -> u32 {
        static COUNTER: Mutex<u32> = Mutex::new(0);
        let mut counter = COUNTER.lock().unwrap();
        *counter += 1;
        *counter
    }
}

fn generate_msisdn(cc_code: &str, ndc_code: u16, sn_length: usize) -> Result<String, String> {
    if sn_length < 6 || sn_length > 10 {
        return Err("Subscriber number length must be between 6 and 10.".to_string());
    }

    let mut rng = rand::thread_rng();
    let sn_random_number: String = (0..sn_length)
        .map(|_| rng.gen_range(0..10).to_string())
        .collect();
    Ok(format!(
        "{}{}{}",
        cc_code,
        ndc_code.to_string(),
        sn_random_number
    ))
}

fn generate_imsi(mcc_code: u16, mnc_code: u16) -> Result<String, String> {
    let hlr_code = "01";
    let msin_length = 8;
    if mcc_code > 999 || mcc_code < 100 {
        return Err("MCC must be a 3-digit number.".to_string());
    }
    if mnc_code > 99 || mnc_code < 10 {
        return Err("MNC must be a 2-digit number.".to_string());
    }
    let mut rng = rand::thread_rng();
    let msin_random_number: String = (0..msin_length)
        .map(|_| rng.gen_range(0..10).to_string())
        .collect();
    Ok(format!(
        "{}{}{}{}",
        mcc_code.to_string(),
        mnc_code.to_string(),
        hlr_code,
        msin_random_number
    ))
}

fn generate_imei() -> Result<String, String> {
    let mut rng = rand::thread_rng();

    let imei_base: String = (0..14).map(|_| rng.gen_range(0..10).to_string()).collect();

    let checksum = calculate_luhn_checksum(&imei_base);

    Ok(format!("{}{}", imei_base, checksum))
}

fn calculate_luhn_checksum(number: &str) -> u8 {
    let digits: Vec<u8> = number
        .chars()
        .map(|c| c.to_digit(10).unwrap() as u8)
        .collect();

    let sum: u8 = digits
        .iter()
        .rev()
        .enumerate()
        .map(|(i, &d)| {
            if i % 2 == 1 {
                let doubled = d * 2;
                if doubled > 9 {
                    doubled - 9
                } else {
                    doubled
                }
            } else {
                d
            }
        })
        .sum();

    let checksum = (10 - (sum % 10)) % 10;
    checksum
}

fn main() {
    println!("Hello, world!");
    let cc_code = "+216";
    let ndc_code = 50;
    let sn_length = 6;

    // MSISDN with full cc_code and ndc_code
    match generate_msisdn(cc_code, ndc_code, sn_length) {
        Ok(msisdn) => println!("Generated MSISDN: {}", msisdn),
        Err(e) => eprintln!("Error: {}", e),
    }

    let mcc_code = 111;
    let mnc_code = 22;
    // IMSI generation
    match generate_imsi(mcc_code, mnc_code) {
        Ok(imsi) => println!("Generated IMSI: {}", imsi),
        Err(e) => eprintln!("Error: {}", e),
    }

    // IMEI generation
    match generate_imei() {
        Ok(imei) => println!("Generated IMEI: {}", imei),
        Err(e) => eprintln!("Error: {}", e),
    }

    let home_ndc_codes: Vec<u16> = vec![30, 31, 32, 50, 51, 52];
    let mut rng = rand::thread_rng();
    let home_ndc_code = home_ndc_codes[rng.gen_range(0..home_ndc_codes.len())];
    println!("Randomly selected number: {}", home_ndc_code);

    let customer = Customer::new(
        "Home".to_string(),
        "Home".to_string(),
        "Home".to_string(),
        "Home".to_string(),
    );
    println!("Customer ID: {}", customer.id);
}
