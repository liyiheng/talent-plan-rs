//! kvs is a lib for in-memory kv storage
#![deny(missing_docs)]
use failure::Fail;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::Path;
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
enum Command<'a> {
    Rm(String),
    Get(String),
    Set(&'a str, &'a str),
}

/// KvStore provides all functions of this lib
pub struct KvStore {
    log_size: usize,
    cur_version: usize,
    data_dir: PathBuf,
    file: File,
    map: HashMap<String, u64>,
}

impl KvStore {
    const MAX_REDUNDANT_RATE: usize = 2;
    const MIN_LOG_SIZE: usize = 500;

    /// Compact the log file if needed,
    /// by creating a new one, then remove the older one
    fn check_compact(&mut self) -> Result<()> {
        let size = self.map.len();
        let need_compact =
            size >= Self::MIN_LOG_SIZE && (self.log_size / size) >= Self::MAX_REDUNDANT_RATE;
        if !need_compact {
            return Ok(());
        }

        let mut dir = self.data_dir.as_path().to_owned();
        dir.push("compacting.tmp");
        let tmp_path = dir.as_path();
        let mut tmp_file = open_file(tmp_path)?;
        let mut new_map = HashMap::with_capacity(self.map.len());
        let mut cur = 0;
        for (k, _) in self.map.iter() {
            let v = self.get(k.to_string()).unwrap().unwrap();
            let cmd = Command::Set(k, &v);
            let mut dat = serde_json::to_vec(&cmd)?;
            dat.push(b'\n');
            tmp_file.write_all(&dat)?;
            new_map.insert(k.to_owned(), cur);
            cur += dat.len() as u64;
        }
        tmp_file.flush()?;
        let mut data_dir = self.data_dir.as_path().to_owned();
        let ver = self.cur_version + 1;
        data_dir.push(ver.to_string());
        std::fs::rename(tmp_path, data_dir.as_path())?;
        let file = open_file(data_dir.as_path())?;

        self.log_size = new_map.len();
        self.file = file;
        self.map = new_map;
        self.cur_version += 1;

        data_dir.pop();
        data_dir.push((self.cur_version - 1).to_string());
        let _ = std::fs::remove_file(data_dir.as_path());
        Ok(())
    }

    /// Open a db file
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut pb: PathBuf = path.into();
        let mut versions: Vec<usize> = pb
            .read_dir()?
            .filter(|f| f.is_ok())
            .map(|r| {
                r.unwrap()
                    .path()
                    .file_name()
                    .map(|s| s.to_string_lossy().parse::<usize>())
            })
            .filter(|v| v.is_some())
            .map(|v| v.unwrap())
            .filter(|v| v.is_ok())
            .map(|v| v.unwrap())
            .collect();
        versions.sort();
        let version = versions.pop().unwrap_or(0);
        pb.push(version.to_string());
        let file = open_file(pb.as_path())?;
        pb.pop();
        let mut store = KvStore {
            log_size: 0,
            cur_version: version,
            data_dir: pb,
            file,
            map: HashMap::default(),
        };
        let len = store.file.metadata()?.len() as usize;
        let mut buf = std::io::BufReader::new(&store.file);
        let mut cur = 0;
        while cur < len {
            let mut s = Vec::new();
            let cmd_size = buf.read_until(b'\n', &mut s)?;
            let cmd = serde_json::from_slice(&s)?;
            store.log_size += 1;
            match cmd {
                Command::Set(k, _) => {
                    store.map.insert(k.to_owned(), cur as u64);
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

    /// Insert or update a K-V pair
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set(&key, &value);
        let cur = self.file.metadata()?.len();
        self.append_log(&cmd)?;
        self.map.insert(key, cur);
        self.log_size += 1;
        self.check_compact()?;
        Ok(())
    }

    /// Get value by a key
    pub fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(offset) = self.map.get(&key) {
            let mut reader = BufReader::new(&self.file);
            reader.seek(SeekFrom::Start(*offset))?;
            let mut buf = Vec::new();
            reader.read_until(b'\n', &mut buf)?;
            let cmd = serde_json::from_slice(&buf)?;
            if let Command::Set(_, v) = cmd {
                return Ok(Some(v.to_owned()));
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
        self.append_log(&cmd)?;
        self.log_size += 1;
        if let Command::Rm(k) = cmd {
            self.map.remove(&k);
        }
        self.check_compact()?;
        Ok(())
    }

    fn append_log(&mut self, cmd: &Command) -> Result<()> {
        serde_json::to_writer(&self.file, cmd)?;
        self.file.write_all(&[b'\n'])?;
        self.file.flush()?;
        Ok(())
    }
}

#[inline]
fn open_file<P: AsRef<Path>>(p: P) -> Result<File> {
    OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .open(p)
        .map_err(Error::from)
}
