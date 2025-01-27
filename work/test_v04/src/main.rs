mod cache;
mod generator;
use cache::cache::CacheManager;
use generator::syslog::{SyslogMessage, SyslogMessageBatch};
use serde::Serialize;
use serde_json;
use serde_json::Value;
use sled::IVec;
use std::collections::HashMap;
use std::time::Instant;
use tokio;

async fn create_and_insert_items(
    manager: &mut CacheManager,
    table_name: &str,
) -> Result<(), sled::Error> {
    for i in 1..=10000 {
        let item = HashMap::from([
            ("session_id".to_string(), i.to_string()),
            ("timestamp".to_string(), "2014".to_string()),
            ("type".to_string(), "open".to_string()),
        ]);

        let session_key = item.get("session_id").unwrap().as_str();

        match serde_json::to_vec(&item) {
            Ok(session_value) => {
                manager
                    .insert_async(table_name, session_key, session_value)
                    .await?;
            }
            Err(err) => {
                eprintln!("Error serializing item to JSON: {}", err);
            }
        }
    }
    Ok(())
}

async fn read_all_items(
    manager: &mut CacheManager,
    table_name: &str,
) -> Result<Vec<(String, Vec<u8>)>, sled::Error> {
    let tree = manager.get_or_create_table(table_name)?;
    let items: Vec<(Vec<u8>, Vec<u8>)> = tree
        .iter()
        .filter_map(|item| item.ok())
        .collect::<Vec<(IVec, IVec)>>()
        .into_iter()
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect();
    let items: Vec<(String, Vec<u8>)> = items
        .into_iter()
        .map(|(key, value)| (String::from_utf8(key).unwrap(), value))
        .collect();
    Ok(items)
}

// Function to convert any struct that implements Serialize to HashMap<String, String>
fn to_hashmap<T: Serialize>(item: &T) -> HashMap<String, String> {
    // Serialize the struct into a JSON value (which is a serde_json::Value)
    let json: Value = serde_json::to_value(item).expect("Failed to serialize");

    // Convert the JSON value into a HashMap<String, String>
    json.as_object()
        .expect("Expected a JSON object")
        .iter()
        .map(|(key, value)| {
            // Convert the value to a string
            (key.clone(), value.as_str().unwrap_or("").to_string())
        })
        .collect()
}
/*
use std::any::type_name;

fn print_type<T>(value: T) {
    // Print the type name of the value
    println!("The type of value is: {}", type_name::<T>());
}
*/
#[tokio::main]
async fn main() -> Result<(), sled::Error> {
    let start = Instant::now();
    /*
        let mut manager = CacheManager::new("my_cache")?;
        create_and_insert_items(&mut manager, "sessions").await?;

        let items = read_all_items(&mut manager, "sessions").await?;
        for (key, value) in items {
            let item: HashMap<String, String> = match serde_json::from_slice(&value) {
                Ok(item) => item,
                Err(err) => {
                    eprintln!("Error deserializing item from JSON: {}", err);
                    return Ok(());
                }
            };
            println!("Session {}: {:?}", key, item);
        }
    */

    let mut manager = CacheManager::new("my_cache")?;
    create_and_insert_items(&mut manager, "sessions").await?;

    let mut batch = SyslogMessageBatch::new();

    // Declare vectors to store HashMaps for 'open' and 'close' messages
    let mut open_messages: Vec<HashMap<String, String>> = Vec::new();
    let mut close_messages: Vec<HashMap<String, String>> = Vec::new();

    // Iterate over the buffer of Syslog messages
    for (timestamp, rec) in batch.get_buffer().iter() {
        // Check the msg_type and separate the messages into 'open' and 'close'
        if rec.msg_type == "open" {
            let map_rec = to_hashmap(&rec);
            open_messages.push(map_rec);
        } else if rec.msg_type == "close" {
            let map_rec = to_hashmap(&rec);
            close_messages.push(map_rec);
        } else {
            println!("Unknown message type: {}", rec.msg_type);
        }
    }
    println!("Size of batch: {}", batch.get_buffer().len());
    println!("Size of open_messages: {}", open_messages.len());
    println!("Size of close_messages: {}", close_messages.len());

    let table_name = "close";
    for item in close_messages.iter() {
        let item_key = item.get("session_id").unwrap().as_str();

        match serde_json::to_vec(&item) {
            Ok(item_value) => {
                manager
                    .insert_async(table_name, item_key, item_value)
                    .await?;
            }
            Err(err) => {
                eprintln!("Error serializing item to JSON: {}", err);
            }
        }
    }

    let table_name = "open";
    for item in open_messages.iter() {
        let item_key = item.get("session_id").unwrap().as_str();

        match serde_json::to_vec(&item) {
            Ok(item_value) => {
                manager
                    .insert_async(table_name, item_key, item_value)
                    .await?;
            }
            Err(err) => {
                eprintln!("Error serializing item to JSON: {}", err);
            }
        }
    }

    let duration = start.elapsed();
    println!("Time taken to read all items: {:?}", duration);
    Ok(())
}
