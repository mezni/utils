mod cache;

use cache::CacheManager;
use tokio::runtime::Builder;
use serde_json;
use std::collections::HashMap;

fn main() -> Result<(), sled::Error> {
    let mut runtime = Builder::new_multi_thread().enable_all().build()?;
    runtime.block_on(async {
        let mut manager = CacheManager::new("my_cache")?;

        let user_key = "1";
        let user_value = "John Doe";
        manager.insert_async("users", user_key, user_value).await?;

        let user_value = manager.get_async("users", user_key).await?.unwrap();
        println!("User Value: {}", String::from_utf8(user_value).unwrap());

        manager.delete_async("users", user_key).await?;

        let item: HashMap<String, String> = [
            ("session_id".to_string(), "233".to_string()),
            ("timestamp".to_string(), "2014".to_string()),
            ("type".to_string(), "open".to_string())
        ].iter().cloned().collect();

        let session_key = item.get("session_id").unwrap().as_str();

        match serde_json::to_vec(&item) {
            Ok(session_value) => {
                manager.insert_async("sessions", session_key, session_value).await?;
                let session_value = manager.get_async("sessions", session_key).await?.unwrap();
                println!("Session Value: {}", String::from_utf8(session_value).unwrap());
            }
            Err(err) => {
                eprintln!("Error serializing item to JSON: {}", err);
            }
        }

        Ok(())
    })
}