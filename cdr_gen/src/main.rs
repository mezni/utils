fn generate_msisdn(cc_code: &str, ndc_code: u8, sn_length: usize) -> String {
    format!("xxxx") 
}
fn main() {
    println!("Hello, world!");
    let cc_code = "+216";
    let ndc_code = 50;
    let sn_length = 6;  
    let msisdn = generate_msisdn(cc_code, ndc_code,sn_length);  
    println!("Generated MSISDN: {}", msisdn); 
}
