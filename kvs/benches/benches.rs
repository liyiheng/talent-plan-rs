use assert_cmd::prelude::*;
use criterion::{criterion_group, criterion_main, Criterion};
use kvs::{app::*, thread_pool::*, *};
use rand;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand::SeedableRng;
use std::collections::HashMap;
use std::process::Command as StdCommand;
use std::process::Stdio;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tempfile::TempDir;

fn gen_string(rng: &mut SmallRng) -> String {
    let size = rng.gen_range(1, 100000);
    rng.sample_iter(&Alphanumeric)
        .take(size)
        .collect::<String>()
}

const KV_COUNT: usize = 100;
const READ_FACTOR: usize = 10;

fn gen_paris() -> HashMap<String, String> {
    let mut rng = rand::rngs::SmallRng::from_seed([6; 16]);
    let mut map = HashMap::with_capacity(KV_COUNT);
    for _ in 0..KV_COUNT {
        map.insert(gen_string(&mut rng), gen_string(&mut rng));
    }
    map
}

fn ensure_dir(dir_name: &str) {
    let no_dir = std::fs::read_dir("/tmp")
        .unwrap()
        .find(|x| {
            let x = x.as_ref().unwrap();
            x.file_name().to_str().unwrap() == dir_name && x.file_type().unwrap().is_dir()
        })
        .is_none();
    if no_dir {
        std::fs::create_dir(format!("/tmp/{}", dir_name)).unwrap();
    }
}

fn get_db_sled() -> impl KvsEngine {
    ensure_dir("bench_sled");
    kvs::SledKvsEngine::open("/tmp/bench_sled").unwrap()
}

fn get_db_kvs() -> impl KvsEngine {
    ensure_dir("bench_kvs");
    kvs::KvStore::open("/tmp/bench_kvs").unwrap()
}

pub fn write(db: &impl KvsEngine, data: &HashMap<String, String>) {
    for (k, v) in data {
        db.set(k.clone(), v.clone()).unwrap();
    }
}

pub fn read(db: &impl KvsEngine, data: &HashMap<String, String>) {
    for _ in 0..READ_FACTOR {
        for (k, v) in data.iter() {
            let value = db.get(k.clone()).unwrap().unwrap();
            assert_eq!(v, &value);
        }
    }
}

pub fn sled_write(c: &mut Criterion) {
    let data = gen_paris();
    let mut db = get_db_sled();
    c.bench_function("sled_write", move |b| {
        b.iter(|| {
            write(&mut db, &data);
        })
    });
}

pub fn kvs_write(c: &mut Criterion) {
    let data = gen_paris();
    let mut db = get_db_kvs();
    c.bench_function("kvs_write", move |b| {
        b.iter(|| {
            write(&mut db, &data);
        })
    });
}

pub fn kvs_read(c: &mut Criterion) {
    let data = gen_paris();
    let mut db = get_db_kvs();
    c.bench_function("kvs_read", move |b| {
        b.iter(|| {
            read(&mut db, &data);
        })
    });
}

pub fn sled_read(c: &mut Criterion) {
    let data = gen_paris();
    let mut db = get_db_sled();
    c.bench_function("sled_read", move |b| {
        b.iter(|| {
            read(&mut db, &data);
        })
    });
}

fn gen_keys() -> Vec<String> {
    let mut keys = Vec::with_capacity(1000);
    for i in 0..1000 {
        keys.push((100000 + i).to_string())
    }
    keys
}

fn get_num_threads() -> Vec<usize> {
    let cpus = num_cpus::get();
    // vec![1, 2, 4, cpus, cpus * 2]
    vec![2, cpus]
}

pub fn read_with_params(c: &mut Criterion, sled: bool, shared_queue_pool: bool) {
    let engine = if sled { "sled" } else { "kvs" };
    let pool = if shared_queue_pool {
        "shared-queue"
    } else {
        "rayon"
    };
    let id = format!("read_{}_{}", pool, engine);
    let inputs = get_num_threads();
    c.bench_function_over_inputs(
        id.as_str(),
        move |b, &num| {
            let temp_dir = TempDir::new().unwrap();
            let threads = num.to_string();
            let addr = "127.0.0.1:4101";
            let mut cmd = StdCommand::cargo_bin("kvs-server").unwrap();
            let mut child = cmd
                .args(&[
                    "--engine",
                    engine,
                    "--addr",
                    addr,
                    "-t",
                    threads.as_str(),
                    "-p",
                    pool,
                ])
                .current_dir(&temp_dir)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            std::thread::sleep(Duration::from_secs(1));
            let keys = gen_keys();
            let mut client = Client::new(addr).unwrap();
            for k in keys.iter() {
                let cmd = Command::Set(k, "Hello world");
                let _ = client.send(&cmd);
            }
            let pool = SharedQueueThreadPool::new(10).unwrap();
            b.iter(|| {
                let pair = Arc::new((Mutex::new(false), Condvar::new()));
                let pair2 = pair.clone();
                pool.spawn(move || {
                    let mut client = Client::new(addr).unwrap();
                    let keys = gen_keys();
                    for k in keys.into_iter() {
                        let cmd = Command::Get(&k);
                        let resp = client.send(&cmd);
                        assert!(resp.is_ok());
                        let resp = resp.unwrap();
                        assert_eq!(resp.value, Some("Hello world".to_owned()));
                    }
                    let (finished, condvar) = &*pair2;
                    *finished.lock().unwrap() = true;
                    condvar.notify_one();
                });

                let (lock, cvar) = &*pair;
                let mut finished = lock.lock().unwrap();
                while !*finished {
                    finished = cvar.wait(finished).unwrap();
                }
            });
            std::mem::drop(pool);
            child.kill().unwrap();
        },
        inputs,
    );
}

pub fn write_with_params(c: &mut Criterion, sled: bool, shared_queue_pool: bool) {
    let engine = if sled { "sled" } else { "kvs" };
    let pool = if shared_queue_pool {
        "shared-queue"
    } else {
        "rayon"
    };
    let id = format!("write_{}_{}", pool, engine);
    let inputs = get_num_threads();
    c.bench_function_over_inputs(
        id.as_str(),
        move |b, &num| {
            let temp_dir = TempDir::new().unwrap();
            let threads = num.to_string();
            let addr = "127.0.0.1:4101";
            let mut cmd = StdCommand::cargo_bin("kvs-server").unwrap();
            let mut child = cmd
                .args(&[
                    "--engine",
                    engine,
                    "--addr",
                    addr,
                    "-t",
                    threads.as_str(),
                    "-p",
                    pool,
                ])
                .current_dir(&temp_dir)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            std::thread::sleep(Duration::from_secs(1));
            let pool = SharedQueueThreadPool::new(8).unwrap();
            b.iter(|| {
                let pair = Arc::new((Mutex::new(false), Condvar::new()));
                let pair2 = pair.clone();
                pool.spawn(move || {
                    let keys = gen_keys();
                    let client = Client::new(addr);
                    assert!(client.is_ok());
                    let mut client = client.unwrap();
                    for k in keys.iter() {
                        let cmd = Command::Set(k, "Hello world");
                        assert!(client.send(&cmd).is_ok());
                    }
                    let (finished, condvar) = &*pair2;
                    *finished.lock().unwrap() = true;
                    condvar.notify_one();
                });
                let (lock, cvar) = &*pair;
                let mut finished = lock.lock().unwrap();
                while !*finished {
                    finished = cvar.wait(finished).unwrap();
                }
            });
            std::mem::drop(pool);
            child.kill().unwrap();
        },
        inputs,
    );
}

pub fn read_queued_kv_store(c: &mut Criterion) {
    read_with_params(c, false, true);
}
pub fn write_queued_kv_store(c: &mut Criterion) {
    write_with_params(c, false, true);
}

pub fn read_rayon_kv_store(c: &mut Criterion) {
    read_with_params(c, true, true);
}
pub fn write_rayon_kv_store(c: &mut Criterion) {
    write_with_params(c, true, true);
}

pub fn read_rayon_sled(c: &mut Criterion) {
    read_with_params(c, true, false);
}
pub fn write_rayon_sled(c: &mut Criterion) {
    write_with_params(c, true, false);
}

criterion_group!(engines, sled_write, sled_read, kvs_write, kvs_read);
criterion_group!(
    server,
    read_queued_kv_store,
    write_queued_kv_store,
    read_rayon_kv_store,
    write_rayon_kv_store,
    read_rayon_sled,
    write_rayon_sled
);
criterion_main!(server);
