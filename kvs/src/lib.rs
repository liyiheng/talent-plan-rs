//! kvs is a lib for key-value database
#![deny(missing_docs)]
use failure::Fail;
use serde::{Deserialize, Serialize};
use sled;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Provide thread pools
pub mod thread_pool;

/// Contains a server and a client
pub mod app;

const ENGINE_KEY: &str = "ENGINE";
const ENGINE_KVS: &str = "kvs";
const ENGINE_SLED: &str = "sled";

/// Result is an alias to simplify error-handling.
pub type Result<T> = std::result::Result<T, Error>;

/// KvsEngine is extracted for pluggable storage engines
pub trait KvsEngine: Clone + Send + 'static {
    /// Get value from the engine by key
    fn get(&self, key: String) -> Result<Option<String>>;

    /// Insert or update a K-V pair
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Remove a K-V pair from the engine
    fn remove(&self, key: String) -> Result<()>;
}

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
    /// Errors from sled
    #[fail(display = "Sled err: {}", _0)]
    SledErr(sled::Error),
    /// Error used when engine dismatching db file
    #[fail(display = "Wrong engine")]
    WrongEngine,
    /// Error from rayon
    #[fail(display = "Rayon: {}", _0)]
    Rayon(rayon::ThreadPoolBuildError),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::IoErr(e)
    }
}

impl From<sled::Error> for Error {
    fn from(e: sled::Error) -> Error {
        Error::SledErr(e)
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
/// Use JSON format here, for both human and machine friendly
#[derive(Serialize, Deserialize)]
pub enum Command<'a> {
    /// Remove a key
    Rm(&'a str),
    /// Get value by key
    Get(&'a str),
    /// Insert or update a K-V pair
    Set(&'a str, &'a str),
}

use std::sync::Arc;
use std::sync::Mutex;

/// KvStore provides all functions of this lib
#[derive(Clone)]
pub struct KvStore {
    inner: Arc<Mutex<KvStoreInner>>,
}

impl KvStore {
    /// Open a db with specified path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let inner = KvStoreInner::open(path)?;
        Ok(KvStore {
            inner: Arc::new(Mutex::new(inner)),
        })
    }
}

struct KvStoreInner {
    log_count: usize,
    version: usize,
    data_dir: PathBuf,
    map: HashMap<String, u64>,
    reader: BufReader<File>,
    writer: BufWriter<File>,
}

impl KvStoreInner {
    const MAX_REDUNDANT_RATE: usize = 2;

    /// Compact the log file if needed,
    /// by creating a new one, then remove the older one
    fn check_compact(&mut self) -> Result<()> {
        let size = self.map.len();
        let need_compact = size > 0 && (self.log_count / size) >= Self::MAX_REDUNDANT_RATE;
        if !need_compact {
            return Ok(());
        }

        let mut dir = self.data_dir.clone();
        dir.push("compacting.tmp");
        let tmp_path = dir.as_path();
        let mut tmp_file = open_file(tmp_path)?;
        let mut new_map = HashMap::with_capacity(self.map.len());
        let mut cur = 0;
        for (k, v) in self.map.iter() {
            self.reader.seek(SeekFrom::Start(*v))?;
            let mut buf = Vec::new();
            self.reader.read_until(b'\n', &mut buf)?;
            tmp_file.write_all(&buf)?;
            new_map.insert(k.to_owned(), cur);
            cur += buf.len() as u64;
        }
        tmp_file.flush()?;
        let mut data_dir = self.data_dir.clone();
        data_dir.push((self.version + 1).to_string());
        std::fs::rename(tmp_path, data_dir.as_path())?;
        let file = open_file(data_dir.as_path())?;
        let file2 = file.try_clone()?;

        self.log_count = new_map.len();
        self.map = new_map;
        self.version += 1;
        self.reader = BufReader::new(file);
        self.writer = BufWriter::new(file2);

        data_dir.pop();
        data_dir.push((self.version - 1).to_string());
        let _ = std::fs::remove_file(data_dir.as_path());
        Ok(())
    }

    fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let mut pb: PathBuf = path.into();
        check_engine(pb.clone(), ENGINE_KVS)?;
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
        let f1 = file.try_clone()?;
        let f2 = file.try_clone()?;
        let len = f1.metadata()?.len() as usize;
        let reader = BufReader::new(f1);
        let writer = BufWriter::new(f2);

        let mut store = KvStoreInner {
            log_count: 0,
            version,
            data_dir: pb,
            map: HashMap::default(),
            reader,
            writer,
        };

        let mut cur = 0;
        while cur < len {
            let mut s = Vec::new();
            let cmd_size = store.reader.read_until(b'\n', &mut s)?;
            let cmd = serde_json::from_slice(&s)?;
            store.log_count += 1;
            match cmd {
                Command::Set(k, _) => {
                    store.map.insert(k.to_owned(), cur as u64);
                }
                Command::Rm(k) => {
                    store.map.remove(k);
                }
                _ => {}
            }
            cur += cmd_size;
        }

        Ok(store)
    }

    fn append_log(&mut self, cmd: &Command) -> Result<()> {
        let mut dat = serde_json::to_vec(cmd)?;
        dat.push(b'\n');
        self.writer.write_all(&dat)?;
        self.writer.flush()?;
        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn remove(&self, key: String) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.map.contains_key(&key) {
            return Err(Error::KeyNotFound(key));
        }
        let cmd = Command::Rm(&key);
        inner.append_log(&cmd)?;
        inner.log_count += 1;
        inner.map.remove(&key);
        inner.check_compact()?;
        Ok(())
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let cmd = Command::Set(&key, &value);
        let cur = inner.writer.seek(SeekFrom::End(0))?;
        inner.append_log(&cmd)?;
        inner.map.insert(key, cur);
        inner.log_count += 1;
        inner.check_compact()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(offset) = inner.map.get(&key) {
            let offset = *offset;
            inner.reader.seek(SeekFrom::Start(offset))?;
            let mut buf = Vec::new();
            inner.reader.read_until(b'\n', &mut buf)?;
            let cmd = serde_json::from_slice(&buf)?;
            if let Command::Set(_, v) = cmd {
                return Ok(Some(v.to_owned()));
            }
        }
        Ok(None)
    }
}

/// Implementation of KvsEngine with sled
#[derive(Clone)]
pub struct SledKvsEngine(sled::Db);

impl SledKvsEngine {
    /// Open sled db
    pub fn open(path: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        let pb: PathBuf = path.into();
        check_engine(pb.clone(), ENGINE_SLED)?;
        sled::open(pb)
            .map(|db| SledKvsEngine(db))
            .map_err(|e| Error::from(e))
    }
}
fn check_engine(mut path: PathBuf, engine: &str) -> Result<()> {
    path.push(ENGINE_KEY);
    let mut f = open_file(path.as_path())?;
    if f.metadata()?.len() == 0 {
        f.write_all(engine.as_bytes())?;
        return Ok(());
    }

    let mut cur = String::new();
    f.read_to_string(&mut cur)?;
    if &cur != engine {
        return Err(Error::WrongEngine);
    }
    Ok(())
}

impl KvsEngine for SledKvsEngine {
    fn get(&self, key: String) -> Result<Option<String>> {
        self.0
            .get(key)
            .map(|v| v.map(|v| String::from_utf8_lossy(v.as_ref()).to_string()))
            .map_err(|e| Error::from(e))
    }
    fn set(&self, key: String, value: String) -> Result<()> {
        let result = self
            .0
            .insert(key, value.as_bytes())
            .map(|_| ())
            .map_err(|e| Error::from(e));
        self.0.flush().map_err(Error::from)?;
        result
    }
    fn remove(&self, key: String) -> Result<()> {
        let result = match self.0.remove(&key) {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(Error::KeyNotFound(key)),
            Err(e) => Err(Error::from(e)),
        };
        self.0.flush().map_err(Error::from)?;
        result
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
