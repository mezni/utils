mod cache;
use cache::cache::CacheManager;
use tokio;
use serde_json;
use std::collections::HashMap;
use sled::IVec;
use std::time::Instant;

async fn create_and_insert_items(manager: &mut CacheManager, table_name: &str) -> Result<(), sled::Error> {
    for i in 1..=1000 {
        let item = HashMap::from([
            ("session_id".to_string(), i.to_string()),
            ("timestamp".to_string(), "2014".to_string()),
            ("type".to_string(), "open".to_string())
        ]);

        let session_key = item.get("session_id").unwrap().as_str();

        match serde_json::to_vec(&item) {
            Ok(session_value) => {
                manager.insert_async(table_name, session_key, session_value).await?;
            }
            Err(err) => {
                eprintln!("Error serializing item to JSON: {}", err);
            }
        }
    }
    Ok(())
}

async fn read_all_items(manager: &mut CacheManager, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, sled::Error> {
    let tree = manager.get_or_create_table(table_name)?;
    let items: Vec<(Vec<u8>, Vec<u8>)> = tree.iter()
        .filter_map(|item| item.ok())
        .collect::<Vec<(IVec, IVec)>>()
        .into_iter()
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect();
    let items: Vec<(String, Vec<u8>)> = items.into_iter().map(|(key, value)| (String::from_utf8(key).unwrap(), value)).collect();
    Ok(items)
}

#[tokio::main]
async fn main() -> Result<(), sled::Error> {
    let start = Instant::now();
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
    let duration = start.elapsed();
    println!("Time taken to read all items: {:?}", duration);
    Ok(())
}