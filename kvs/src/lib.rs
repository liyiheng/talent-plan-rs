//! kvs is a lib for key-value database
#![deny(missing_docs)]
use chashmap::CHashMap;
use crossbeam_epoch::{self as epoch, Atomic, Owned};
use failure::Fail;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{atomic::Ordering, Arc, Mutex, MutexGuard};
use std::thread::ThreadId;

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

/// KvStore provides all functions of this lib
#[derive(Clone)]
pub struct KvStore {
    inner: Arc<KvStoreInner>,
}

impl KvStore {
    /// Open a db with specified path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let inner = KvStoreInner::open(path)?;
        Ok(KvStore {
            inner: Arc::new(inner),
        })
    }
}

struct KvStoreInner {
    log_count: Atomic<usize>,
    version: Atomic<usize>,
    data_dir: Arc<PathBuf>,
    index: Atomic<CHashMap<String, u64>>,
    readers: Atomic<CHashMap<ThreadId, BufReader<File>>>,
    writer: Atomic<Mutex<BufWriter<File>>>,
}

impl KvStoreInner {
    const MAX_REDUNDANT_RATE: usize = 2;

    /// Compact the log file if needed,
    /// by creating a new one, then remove the older one
    fn check_compact(&self) -> Result<()> {
        let guard = epoch::pin();
        let size = unsafe { self.index.load(Ordering::Acquire, &guard).deref().len() };
        let log_count = unsafe { self.log_count.load(Ordering::Acquire, &guard).deref() };
        let need_compact = size > 0 && (log_count / size) >= Self::MAX_REDUNDANT_RATE;
        if !need_compact {
            return Ok(());
        }
        let mut dir = self.data_dir.as_ref().clone();
        dir.push("compacting.tmp");
        let tmp_path = dir.as_path();
        let mut tmp_file = open_file(tmp_path)?;
        let new_map = CHashMap::with_capacity(size);
        let mut reader = {
            let tid = std::thread::current().id();
            let readers = unsafe { self.readers.load(Ordering::Acquire, &guard).deref() };
            if !readers.contains_key(&tid) {
                let ver = unsafe { self.version.load(Ordering::Acquire, &guard).deref() };
                let mut path = self.data_dir.as_ref().clone();
                path.push(ver.to_string());
                let file = File::open(path.as_path())?;
                let reader = BufReader::new(file);
                readers.insert(tid, reader);
            }
            readers.get_mut(&tid).unwrap()
        };
        let old_index = unsafe { self.index.load(Ordering::Acquire, &guard).deref().clone() };
        let mut cur = 0;
        for (k, v) in old_index.into_iter() {
            reader.seek(SeekFrom::Start(v))?;
            let mut buf = Vec::new();
            reader.read_until(b'\n', &mut buf)?;
            tmp_file.seek(SeekFrom::End(0))?;
            tmp_file.write_all(&buf)?;
            new_map.insert(k.to_owned(), cur);
            cur += buf.len() as u64;
        }
        tmp_file.flush()?;
        let mut data_dir = self.data_dir.as_ref().clone();
        let version = unsafe { self.version.load(Ordering::Acquire, &guard).deref_mut() };
        let new_version = *version + 1;
        data_dir.push(new_version.to_string());
        std::fs::rename(tmp_path, data_dir.as_path())?;
        let file = open_file(data_dir.as_path())?;

        self.log_count
            .store(Owned::new(new_map.len()), Ordering::Release);
        self.writer.store(
            Owned::new(Mutex::new(BufWriter::new(file))),
            Ordering::Release,
        );

        *version += 1;
        self.index.store(Owned::new(new_map), Ordering::Release);
        // self.cur_file.store(Owned::new(file), Ordering::Release);
        self.readers
            .store(Owned::new(CHashMap::new()), Ordering::Release);

        data_dir.pop();
        data_dir.push((new_version - 1).to_string());
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
        let f1 = open_file(pb.as_path())?;
        pb.pop();
        let mut f2 = f1.try_clone()?;
        let len = f1.metadata()?.len() as usize;
        let mut reader = BufReader::new(f1);
        let version = Atomic::new(version);
        let mut cur = 0;
        let mut log_cnt = 0;
        let index = CHashMap::new();
        while cur < len {
            let mut s = Vec::new();
            let cmd_size = reader.read_until(b'\n', &mut s)?;
            let cmd = serde_json::from_slice(&s)?;
            match cmd {
                Command::Set(k, _) => {
                    index.insert(k.to_owned(), cur as u64);
                }
                Command::Rm(k) => {
                    index.remove(k);
                }
                _ => {}
            }
            cur += cmd_size;
            log_cnt += 1;
        }
        f2.seek(SeekFrom::End(0))?;
        let writer = Atomic::new(Mutex::new(BufWriter::new(f2)));
        let store = KvStoreInner {
            log_count: Atomic::new(log_cnt),
            version,
            data_dir: Arc::new(pb),
            index: Atomic::new(index),
            readers: Atomic::new(CHashMap::new()),
            writer,
        };
        Ok(store)
    }

    fn append_log(&self, cmd: &Command, mutex: &mut MutexGuard<'_, BufWriter<File>>) -> Result<()> {
        let mut dat = serde_json::to_vec(cmd)?;
        dat.push(b'\n');
        mutex.seek(SeekFrom::End(0))?;
        mutex.write_all(&dat)?;
        mutex.flush()?;
        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn remove(&self, key: String) -> Result<()> {
        let guard = epoch::pin();
        let index = unsafe { self.inner.index.load(Ordering::Acquire, &guard).deref() };
        if !index.contains_key(&key) {
            return Err(Error::KeyNotFound(key));
        }
        let mut mutex_guard = unsafe {
            self.inner
                .writer
                .load(Ordering::Acquire, &guard)
                .deref()
                .lock()
                .unwrap()
        };
        let cmd = Command::Rm(&key);
        self.inner.append_log(&cmd, &mut mutex_guard)?;
        let v = unsafe {
            self.inner
                .log_count
                .load(Ordering::Acquire, &guard)
                .deref_mut()
        };
        *v += 1;
        index.remove(&key);
        self.inner.check_compact()?;
        Ok(())
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set(&key, &value);
        let guard = epoch::pin();
        let mut mutex_guard = unsafe {
            self.inner
                .writer
                .load(Ordering::Acquire, &guard)
                .deref()
                .lock()
                .unwrap()
        };
        let cur = mutex_guard.get_ref().metadata()?.len();
        self.inner.append_log(&cmd, &mut mutex_guard)?;
        unsafe {
            self.inner
                .index
                .load(Ordering::Acquire, &guard)
                .deref()
                .insert(key, cur);
        }
        unsafe {
            *self
                .inner
                .log_count
                .load(Ordering::Acquire, &guard)
                .deref_mut() += 1;
        }
        self.inner.check_compact()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let tid = std::thread::current().id();
        let guard = epoch::pin();
        let position = unsafe {
            self.inner
                .index
                .load(Ordering::Acquire, &guard)
                .deref()
                .get(&key)
                .map(|v| *v)
        };
        let readers = unsafe { self.inner.readers.load(Ordering::Acquire, &guard).deref() };
        if !readers.contains_key(&tid) {
            let ver = unsafe { self.inner.version.load(Ordering::Acquire, &guard).deref() };
            let mut path = self.inner.data_dir.as_ref().clone();
            path.push(ver.to_string());
            let file = File::open(path.as_path())?;
            let reader = BufReader::new(file);
            readers.insert(tid, reader);
        }
        let mut reader = readers.get_mut(&tid).unwrap();
        if let Some(offset) = position {
            reader.seek(SeekFrom::Start(offset))?;
            let mut buf = Vec::new();
            reader.read_until(b'\n', &mut buf)?;
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
