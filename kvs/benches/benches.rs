use criterion::{criterion_group, criterion_main, Criterion};
use kvs::KvsEngine;
use rand;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand::SeedableRng;
use std::collections::HashMap;

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

fn get_db(kvs: bool) -> Box<dyn KvsEngine> {
    if kvs {
        ensure_dir("bench_kvs");
        Box::new(kvs::KvStore::open("/tmp/bench_kvs").unwrap())
    } else {
        ensure_dir("bench_sled");
        Box::new(kvs::SledKvsEngine::open("/tmp/bench_sled").unwrap())
    }
}

pub fn write(db: &mut Box<dyn KvsEngine>, data: &HashMap<String, String>) {
    for (k, v) in data {
        db.set(k.clone(), v.clone()).unwrap();
    }
}

pub fn read(db: &mut Box<dyn KvsEngine>, data: &HashMap<String, String>) {
    for _ in 0..READ_FACTOR {
        for (k, v) in data.iter() {
            let value = db.get(k.clone()).unwrap().unwrap();
            assert_eq!(v, &value);
        }
    }
}

pub fn sled_write(c: &mut Criterion) {
    let data = gen_paris();
    let mut db = get_db(false);
    c.bench_function("sled_write", move |b| {
        b.iter(|| {
            write(&mut db, &data);
        })
    });
}

pub fn kvs_write(c: &mut Criterion) {
    let data = gen_paris();
    let mut db = get_db(true);
    c.bench_function("kvs_write", move |b| {
        b.iter(|| {
            write(&mut db, &data);
        })
    });
}

pub fn kvs_read(c: &mut Criterion) {
    let data = gen_paris();
    let mut db = get_db(true);
    c.bench_function("kvs_read", move |b| {
        b.iter(|| {
            read(&mut db, &data);
        })
    });
}

pub fn sled_read(c: &mut Criterion) {
    let data = gen_paris();
    let mut db = get_db(false);
    c.bench_function("sled_read", move |b| {
        b.iter(|| {
            read(&mut db, &data);
        })
    });
}

criterion_group!(benches, sled_write, sled_read, kvs_write, kvs_read);
criterion_main!(benches);
