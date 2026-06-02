use common_types::events::{Channel, EventEnvelope, EventName};

const VERIFY_CHANNEL: Channel = Channel::DriverWeb;
const VERIFY_EVENT: EventName = EventName::PageViewed;
fn _use_envelope(_e: &EventEnvelope) {}

fn main() {
    println!("driver-service ready");
}