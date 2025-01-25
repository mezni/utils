mod generator;

use generator::syslog::SyslogMessageBatch;


use std::thread::sleep;
use std::time::Duration as StdDuration;


fn main() {
    let mut batch = SyslogMessageBatch::new();  // Calls the constructor that internally calls load(1000)

    // Print messages in a loop
    for _ in 0..900 {
        batch.print_messages();
        // Sleep for 1 second after each print
        sleep(StdDuration::from_secs(5));
        println!("Batch length: {}", batch.length());
    }
}