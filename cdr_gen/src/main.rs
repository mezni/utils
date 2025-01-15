use rand::Rng;
use sled::{Db};  // Removed IVec import
use serde::{Serialize, Deserialize};  // Added serde imports
use std::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]  // Derive Serialize and Deserialize
enum CustomerType {
    Home,
    National,
    International,
}

#[derive(Debug, Clone, Serialize, Deserialize)]  // Added Serialize and Deserialize
struct Customer {
    id: u32,
    customer_type: CustomerType,
    msisdn: String,
    imsi: String,
    imei: String,
}

impl Customer {
    fn new(cc_code: &str, ndc_code: u16, sn_length: usize, mcc_code: u16, mnc_code: u16, customer_type: CustomerType) -> Self {
        let id = Customer::generate_id();
        let msisdn = Customer::generate_msisdn(cc_code, ndc_code, sn_length);
        let imsi = Customer::generate_imsi(mcc_code, mnc_code);
        let imei = Customer::generate_imei();
        
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

    fn generate_msisdn(cc_code: &str, ndc_code: u16, sn_length: usize) -> String {
        let mut rng = rand::thread_rng();
        let sn_random_number: String = (0..sn_length)
            .map(|_| rng.gen_range(0..10).to_string())
            .collect();
        format!("{}{}{}", cc_code, ndc_code, sn_random_number)
    }

    fn generate_imsi(mcc_code: u16, mnc_code: u16) -> String {
        let msin_length = 8;
        let mut rng = rand::thread_rng();
        let msin_random_number: String = (0..msin_length)
            .map(|_| rng.gen_range(0..10).to_string())
            .collect();
        format!("{}{}01{}", mcc_code, mnc_code, msin_random_number)
    }

    fn generate_imei() -> String {
        let mut rng = rand::thread_rng();
        let imei_base: String = (0..14)
            .map(|_| rng.gen_range(0..10).to_string())
            .collect();
        let checksum = calculate_luhn_checksum(&imei_base);
        format!("{}{}", imei_base, checksum)
    }
}

fn calculate_luhn_checksum(number: &str) -> u8 {
    let digits: Vec<u8> = number.chars()
        .map(|c| c.to_digit(10).unwrap() as u8)
        .collect();
    
    let sum: u8 = digits.iter().rev().enumerate().map(|(i, &d)| {
        if i % 2 == 1 { 
            let doubled = d * 2;
            if doubled > 9 { doubled - 9 } else { doubled }
        } else {
            d
        }
    }).sum();
    
    let checksum = (10 - (sum % 10)) % 10;
    checksum
}

// Database repository using Sled
struct CustomerDatabaseRepository {
    db: Db,
}

impl CustomerDatabaseRepository {
    fn new(db_path: &str) -> Self {
        let db = sled::open(db_path).expect("Failed to open Sled database");
        CustomerDatabaseRepository { db }
    }

    fn add_customer(&self, customer: &Customer) {
        let customer_id = customer.id.to_string();
        let serialized_customer = bincode::serialize(customer).expect("Failed to serialize customer");

        self.db.insert(customer_id, serialized_customer).expect("Failed to insert customer");
    }

    fn get_customer_by_id(&self, id: u32) -> Option<Customer> {
        let customer_id = id.to_string();
        if let Some(serialized_customer) = self.db.get(customer_id).expect("Failed to retrieve customer") {
            let customer: Customer = bincode::deserialize(&serialized_customer).expect("Failed to deserialize customer");
            Some(customer)
        } else {
            None
        }
    }

    fn get_all_customers(&self) -> Vec<Customer> {
        let mut customers = Vec::new();
        for item in self.db.iter() {
            if let Ok((_, serialized_customer)) = item {
                if let Ok(customer) = bincode::deserialize::<Customer>(&serialized_customer) {
                    customers.push(customer);
                }
            }
        }
        customers
    }
}

fn main() {
    // Database example using Sled
    let db_repo = CustomerDatabaseRepository::new("customers.sled");
    let customer1 = Customer::new("+1", 415, 7, 310, 10, CustomerType::National);
    db_repo.add_customer(&customer1);
    let customer2 = Customer::new("+1", 415, 7, 310, 10, CustomerType::National);
    db_repo.add_customer(&customer2);
    let customer3 = Customer::new("+1", 415, 7, 310, 11, CustomerType::National);
    db_repo.add_customer(&customer3);

    if let Some(customer) = db_repo.get_customer_by_id(1) {
        println!("{:?}", customer);
    }

    // Print all customers stored in the database
    let all_customers = db_repo.get_all_customers();
    for customer in all_customers {
        println!("{:?}", customer);
    }
}
