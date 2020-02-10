#[macro_use]
extern crate log;
use kvs::{app::KvServer, thread_pool::*};

const THREAD_POOL: &str = "THREAD-POOL";
const NUM_THREADS: &str = "NUM-THREADS";
const IP_PORT: &str = "IP-PORT";
const ENGINE: &str = "ENGINE-NAME";

fn main() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .target(env_logger::fmt::Target::Stderr)
        .init();
    let ver = env!("CARGO_PKG_VERSION");
    let n = num_cpus::get().to_string();
    let args = clap::App::new("kvs-server")
        .arg(
            clap::Arg::with_name(IP_PORT)
                .long("addr")
                .default_value("127.0.0.1:4000"),
        )
        .arg(
            clap::Arg::with_name(ENGINE)
                .long("engine")
                .default_value("kvs")
                .help("sled or kvs"),
        )
        .arg(
            clap::Arg::with_name(NUM_THREADS)
                .long("threads")
                .short("t")
                .default_value(n.as_str()),
        )
        .arg(
            clap::Arg::with_name(THREAD_POOL)
                .long("pool")
                .short("p")
                .help("shared-queue or rayon")
                .default_value("shared-queue"),
        )
        .version(ver)
        .get_matches();
    let addr = args.value_of(IP_PORT).unwrap();
    let engine = args.value_of(ENGINE).unwrap();
    let threads = args.value_of(NUM_THREADS).unwrap().parse().unwrap();
    let pool = args.value_of(THREAD_POOL).unwrap();
    info!("Version: {}", ver);
    info!("Address: {}", addr);
    info!("Engine: {}", engine);
    info!("Threads: {}", threads);
    info!("Pool: {}", pool);

    match engine {
        "kvs" => {
            let db = kvs::KvStore::open("./").unwrap();
            match pool {
                "rayon" => {
                    let pool = RayonThreadPool::new(threads).unwrap();
                    let server = KvServer::new(addr, pool, db).unwrap();
                    server.start();
                }
                "shared-queue" => {
                    let pool = SharedQueueThreadPool::new(threads).unwrap();
                    let server = KvServer::new(addr, pool, db).unwrap();
                    server.start();
                }
                _ => panic!("Invalid pool"),
            }
        }
        "sled" => {
            let db = kvs::SledKvsEngine::open("./").unwrap();
            match pool {
                "rayon" => {
                    let pool = RayonThreadPool::new(threads).unwrap();
                    let server = KvServer::new(addr, pool, db).unwrap();
                    server.start();
                }
                "shared-queue" => {
                    let pool = SharedQueueThreadPool::new(threads).unwrap();
                    let server = KvServer::new(addr, pool, db).unwrap();
                    server.start();
                }
                _ => panic!("Invalid pool"),
            }
        }
        _ => {
            unreachable!();
        }
    };
}
