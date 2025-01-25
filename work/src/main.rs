mod generator;

use generator::syslog::SyslogMessage;
use rand::thread_rng;

fn main() {
    let mut rng = thread_rng();
    let mut syslog_messages = Vec::new();

    // Generate 10 random syslog messages
    for _ in 0..10 {
        let syslog_message = SyslogMessage::new(&mut rng);
        syslog_messages.push(syslog_message);
    }

    // Display the generated syslog messages
    for (i, message) in syslog_messages.iter().enumerate() {
        println!("Syslog Message {}: {:#?}", i + 1, message);
    }
}
