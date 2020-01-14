//! kvs is a lib for in-memory kv storage
#![deny(missing_docs)]
use std::collections::HashMap;

/// KvStore keeps all K-V pairs in a HashMap
pub struct KvStore {
    data: HashMap<String, String>,
}

/// Create new KvStore with HashMap::default()
impl Default for KvStore {
    fn default() -> Self {
        KvStore {
            data: HashMap::default(),
        }
    }
}

impl KvStore {
    /// Create a new KvStore
    pub fn new() -> Self {
        KvStore {
            data: HashMap::new(),
        }
    }

    /// Insert or update a K-V pair
    pub fn set(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    /// Get value by a key
    pub fn get(&mut self, key: String) -> Option<String> {
        self.data.get(&key).cloned()
    }

    /// Remove a K-V pair from KvStore
    pub fn remove(&mut self, key: String) {
        self.data.remove(&key);
    }
}
