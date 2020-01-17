//! kvs is a lib for in-memory kv storage
#![deny(missing_docs)]
use failure::Fail;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::PathBuf;

/// Result is an alias to simplify error-handling.
pub type Result<T> = std::result::Result<T, Error>;

/// Error for kv operateions
#[derive(Fail, Debug)]
pub enum Error {
    /// For operations on a non-exist key
    #[fail(display = "Key not found")]
    KeyNotFound(String),
    /// Errors from filesystem
    #[fail(display = "IO err: {}", _0)]
    IoErr(std::io::Error),
    /// Errors from serde_json
    #[fail(display = "JSON err: {}", _0)]
    JsonErr(serde_json::Error),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::IoErr(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Error {
        Error::JsonErr(e)
    }
}

/// Command is a representation of a request made to kvs.
/// Rm and Set can be serialized and append to log,
/// or deserizlized from log data.
/// Use JSON format here, for both readable and portable
#[derive(Serialize, Deserialize)]
enum Command {
    Rm(String),
    Get(String),
    Set(String, String),
}

/// KvStore provides all functions of this lib
pub struct KvStore {
    file: File,
    map: HashMap<String, u64>,
}

/// Create new KvStore with HashMap::default()
impl Default for KvStore {
    fn default() -> Self {
        KvStore {
            file: std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .read(true)
                .open("/tmp/tmp-kvs.db")
                .unwrap(),
            map: HashMap::default(),
        }
    }
}

impl KvStore {
    /// Open a db file
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut pb: PathBuf = path.into();
        pb.push("tmp-kvs.log");
        let path = pb.as_path();
        let mut store = KvStore {
            file: std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .read(true)
                .open(path)
                .unwrap(),
            map: HashMap::default(),
        };
        let len = store.file.metadata()?.len() as usize;
        let mut buf = std::io::BufReader::new(&store.file);
        let mut cur = 0;
        while cur < len {
            let mut s = Vec::new();
            let cmd_size = buf.read_until(b'\n', &mut s)?;
            let cmd = serde_json::from_slice(&s)?;
            match cmd {
                Command::Set(k, _) => {
                    store.map.insert(k, cur as u64);
                }
                Command::Rm(k) => {
                    store.map.remove(&k);
                }
                _ => {}
            }
            cur += cmd_size;
        }

        Ok(store)
    }

    /// Create a new KvStore
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or update a K-V pair
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set(key, value);
        let cur = self.file.metadata()?.len();
        serde_json::to_writer(&self.file, &cmd)?;
        self.file.write(&[b'\n'])?;
        self.file.flush()?;
        if let Command::Set(k, _) = cmd {
            self.map.insert(k, cur);
        }
        Ok(())
    }

    /// Get value by a key
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(offset) = self.map.get(&key) {
            let mut reader = BufReader::new(&self.file);
            reader.seek(SeekFrom::Start(*offset))?;
            let mut buf = Vec::new();
            reader.read_until(b'\n', &mut buf)?;
            let cmd = serde_json::from_slice(&buf)?;
            if let Command::Set(_, v) = cmd {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    /// Remove a K-V pair from KvStore
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.map.contains_key(&key) {
            return Err(Error::KeyNotFound(key));
        }
        let cmd = Command::Rm(key);
        serde_json::to_writer(&self.file, &cmd)?;
        self.file.write(&[b'\n'])?;
        self.file.flush()?;
        if let Command::Rm(k) = cmd {
            self.map.remove(&k);
        }
        Ok(())
    }
}
