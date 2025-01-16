use rand::Rng;
use std::sync::Mutex;

struct DirectoryNumberConfig {
    cc: String,
    ndc: Vec<u16>,
    mcc: u16,
    mnc: u16,
    perc: u16,
}

struct GeneratorConfig {
    id: u32,
    customer_type: String,
    cc_ndc: Vec<DirectoryNumberConfig>,
    digits: usize,
    count: usize,
}

struct Customer {
    id: u32,
    customer_type: String,
    msisdn: String,
    imsi: String,
    imei: String,
}

impl Customer {
    fn new(
        customer_type: String,
        cc_code: &str,
        ndc_code: u16,
        sn_length: usize,
        mcc_code: u16,
        mnc_code: u16,
    ) -> Result<Self, String> {
        let id = Customer::generate_id();
        let msisdn = Customer::generate_msisdn(cc_code, ndc_code, sn_length)?;
        let imsi = Customer::generate_imsi(mcc_code, mnc_code)?;
        let imei = Customer::generate_imei()?;
        Ok(Customer {
            id,
            customer_type,
            msisdn,
            imsi,
            imei,
        })
    }

    fn generate_id() -> u32 {
        static COUNTER: Mutex<u32> = Mutex::new(0);
        let mut counter = COUNTER.lock().unwrap();
        *counter += 1;
        *counter
    }

    fn generate_msisdn(cc_code: &str, ndc_code: u16, sn_length: usize) -> Result<String, String> {
        if sn_length < 6 || sn_length > 10 {
            return Err("Subscriber number length must be between 6 and 10.".to_string());
        }

        let mut rng = rand::thread_rng();
        let sn_random_number: String = (0..sn_length)
            .map(|_| rng.gen_range(0..10).to_string())
            .collect();
        Ok(format!("{}{}{}", cc_code, ndc_code, sn_random_number))
    }

    fn generate_imsi(mcc_code: u16, mnc_code: u16) -> Result<String, String> {
        if mcc_code > 999 || mcc_code < 100 {
            return Err("MCC must be a 3-digit number.".to_string());
        }
        if mnc_code > 99 || mnc_code < 10 {
            return Err("MNC must be a 2-digit number.".to_string());
        }

        let mut rng = rand::thread_rng();
        let hlr_code = "01";
        let msin_length = 8;
        let msin_random_number: String = (0..msin_length)
            .map(|_| rng.gen_range(0..10).to_string())
            .collect();
        Ok(format!("{}{}{}{}", mcc_code, mnc_code, hlr_code, msin_random_number))
    }

    fn generate_imei() -> Result<String, String> {
        let mut rng = rand::thread_rng();
        let imei_base: String = (0..14).map(|_| rng.gen_range(0..10).to_string()).collect();
        let checksum = calculate_luhn_checksum(&imei_base);
        Ok(format!("{}{}", imei_base, checksum))
    }
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
            if i % 2 == 0 {
                d
            } else {
                let doubled = d * 2;
                if doubled > 9 {
                    doubled - 9
                } else {
                    doubled
                }
            }
        })
        .sum();

    (10 - (sum % 10)) % 10
}

fn generator(config: &GeneratorConfig) {
    let mut rng = rand::thread_rng();

    println!("Customer Type: {}", config.customer_type);
    println!("Digits: {}", config.digits);
    println!("Count: {}", config.count);

    let mut cumulative_perc = Vec::new();
    let mut total = 0;

    for cc_ndc in &config.cc_ndc {
        total += cc_ndc.perc as u32;
        cumulative_perc.push(total);
    }

    if total != 100 {
        println!("Warning: Total percentages do not sum to 100!");
    }

    for _ in 0..config.count {
        let choice = rng.gen_range(0..total);
        let mut selected = None;

        for (i, &cumulative) in cumulative_perc.iter().enumerate() {
            if choice < cumulative {
                selected = Some(&config.cc_ndc[i]);
                break;
            }
        }

        if let Some(selected) = selected {
            let ndc = selected.ndc[rng.gen_range(0..selected.ndc.len())];
            match Customer::new(
                config.customer_type.clone(),
                &selected.cc,
                ndc,
                config.digits,
                selected.mcc,
                selected.mnc,
            ) {
                Ok(customer) => println!(
                    "Generated Customer {}: ID: {}, MSISDN: {}, IMSI: {}, IMEI: {}",
                    customer.customer_type, customer.id, customer.msisdn, customer.imsi, customer.imei
                ),
                Err(e) => println!("Error generating customer: {}", e),
            }
        } else {
            println!("Error: No configuration selected!");
        }
    }
}

fn main() {
    let cc_ndc1 = DirectoryNumberConfig {
        cc: "+216".to_string(),
        ndc: vec![30, 31, 32],
        mcc: 603,
        mnc: 10,
        perc: 40,
    };

    let cc_ndc2 = DirectoryNumberConfig {
        cc: "+216".to_string(),
        ndc: vec![50, 51, 52, 53],
        mcc: 603,
        mnc: 11,
        perc: 60,
    };

    let config = GeneratorConfig {
        id: 1,
        customer_type: "home".to_string(),
        cc_ndc: vec![cc_ndc1, cc_ndc2],
        digits: 6,
        count: 10,
    };

    generator(&config);
}
