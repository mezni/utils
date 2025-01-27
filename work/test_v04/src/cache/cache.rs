use sled::{Db, Tree};
use std::path::Path;
use std::collections::HashMap;

pub struct CacheManager {
    db: Db,
    tables: HashMap<String, Tree>,
}

impl CacheManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        Ok(CacheManager {
            db,
            tables: HashMap::new(),
        })
    }

    pub fn get_or_create_table(&mut self, name: &str) -> Result<Tree, sled::Error> {
        if let Some(tree) = self.tables.get(name) {
            return Ok(tree.clone());
        }
        let tree = self.db.open_tree(name)?;
        self.tables.insert(name.to_string(), tree.clone());
        Ok(tree)
    }

    pub async fn insert_async<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, table: &str, key: K, value: V) -> Result<(), sled::Error> {
        let tree = self.get_or_create_table(table)?;
        let value = value.as_ref().to_vec();
        tree.insert(key, value)?;
        Ok(())
    }

    pub async fn get_async<K: AsRef<[u8]>>(&mut self, table: &str, key: K) -> Result<Option<Vec<u8>>, sled::Error> {
        let tree = self.get_or_create_table(table)?;
        let value = tree.get(key)?;
        let value = value.map(|ivec| ivec.to_vec());
        Ok(value)
    }

    pub async fn delete_async<K: AsRef<[u8]>>(&mut self, table: &str, key: K) -> Result<(), sled::Error> {
        let tree = self.get_or_create_table(table)?;
        tree.remove(key)?;
        Ok(())
    }
}
