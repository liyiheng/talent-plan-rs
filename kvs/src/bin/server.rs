#[macro_use]
extern crate log;
use kvs::{KvsEngine, Response};
use std::io::prelude::*;
use std::net::TcpListener;
use std::net::TcpStream;

const IP_PORT: &str = "IP-PORT";
const ENGINE: &str = "ENGINE-NAME";

fn main() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .target(env_logger::fmt::Target::Stderr)
        .init();
    let ver = env!("CARGO_PKG_VERSION");
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
        .version(ver)
        .get_matches();
    let addr = args.value_of(IP_PORT).unwrap();
    let engine = args.value_of(ENGINE).unwrap();
    info!("Version: {}", ver);
    info!("Address: {}", addr);
    info!("Engine: {}", engine);
    let mut db: Box<dyn KvsEngine> = match engine {
        "kvs" => Box::new(kvs::KvStore::open("./").unwrap()),
        "sled" => Box::new(kvs::SledKvsEngine::open("./").unwrap()),
        _ => {
            unreachable!();
        }
    };
    let listener = TcpListener::bind(addr).unwrap();
    for stream in listener.incoming() {
        if let Ok(s) = stream {
            handle_stream(db.as_mut(), s);
        }
    }
}

fn handle_stream(db: &mut dyn KvsEngine, s: TcpStream) {
    let mut reader = std::io::BufReader::new(s);
    let mut data = vec![];
    loop {
        data.clear();
        let read_result = reader.read_until(b'\n', &mut data);
        if read_result.is_err() {
            break;
        }
        data.pop();
        let response = handle_cmd(db, &data);
        let mut dat = serde_json::to_vec(&response).unwrap();
        dat.push(b'\n');
        if reader.get_mut().write_all(&dat).is_err() {
            break;
        }
    }
}

fn handle_cmd(db: &mut dyn KvsEngine, data: &[u8]) -> Response {
    let cmd = serde_json::from_slice(data);
    if cmd.is_err() {
        let e = cmd.err().unwrap().to_string();
        return Response {
            value: None,
            error: Some(e),
        };
    }
    match cmd.unwrap() {
        kvs::Command::Get(k) => match db.get(k.to_owned()) {
            Ok(v) => Response {
                value: v,
                error: None,
            },
            Err(e) => Response {
                value: None,
                error: Some(e.to_string()),
            },
        },
        kvs::Command::Set(k, v) => match db.set(k.to_owned(), v.to_owned()) {
            Err(e) => Response {
                value: None,
                error: Some(e.to_string()),
            },
            Ok(_) => Response {
                value: None,
                error: None,
            },
        },
        kvs::Command::Rm(k) => match db.remove(k.to_owned()).map_err(|e| e.to_string()) {
            Err(e) => Response {
                value: None,
                error: Some(e),
            },
            Ok(_) => Response {
                value: None,
                error: None,
            },
        },
    }
}
