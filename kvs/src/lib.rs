//! kvs is a lib for in-memory kv storage
#![deny(missing_docs)]
pub use std::io::Result;
use std::path::PathBuf;

/// KvStore provides all functions of this lib
pub struct KvStore {}

/// Create new KvStore with HashMap::default()
impl Default for KvStore {
    fn default() -> Self {
        KvStore {}
    }
}

impl KvStore {
    /// Open a db file
    pub fn open(_path: impl Into<PathBuf>) -> Result<KvStore> {
        unimplemented!();
    }

    /// Create a new KvStore
    pub fn new() -> Self {
        KvStore {}
    }

    /// Insert or update a K-V pair
    pub fn set(&mut self, _key: String, _value: String) -> Result<()> {
        unimplemented!();
    }

    /// Get value by a key
    pub fn get(&mut self, _key: String) -> Result<Option<String>> {
        unimplemented!();
    }

    /// Remove a K-V pair from KvStore
    pub fn remove(&mut self, _key: String) -> Result<()> {
        unimplemented!();
    }
}
