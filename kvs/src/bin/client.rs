use clap::{App, Arg};
use kvs::{self, Response};
use std::io::prelude::*;
use std::net::TcpStream;
use std::net::ToSocketAddrs;

const IP_PORT: &str = "IP-PORT";

fn main() {
    let cmds = App::new("kvs-client")
        .subcommands(vec![
            App::new("set")
                .arg(Arg::with_name("key").index(1))
                .arg(Arg::with_name("value").index(2)),
            App::new("get").arg(Arg::with_name("key")),
            App::new("rm").arg(Arg::with_name("key")),
        ])
        .arg(
            Arg::with_name(IP_PORT)
                .long("addr")
                .global(true)
                .default_value("127.0.0.1:4000"),
        )
        .version(env!("CARGO_PKG_VERSION"))
        .get_matches();
    let addr = cmds.value_of(IP_PORT).unwrap();
    let addr = addr.to_socket_addrs().unwrap().into_iter().next().unwrap();
    let mut stream = TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(5)).unwrap();
    let (sub_cmd, args) = cmds.subcommand();
    let k = args.unwrap().value_of("key").unwrap();
    let v = args.unwrap().value_of("value");
    let op = match sub_cmd {
        "set" => kvs::Command::Set(&k, &v.unwrap()),
        "get" => kvs::Command::Get(&k),
        "rm" => kvs::Command::Rm(&k),
        _ => unreachable!(),
    };
    let mut dat = serde_json::to_vec(&op).unwrap();
    dat.push(b'\n');
    stream.write_all(&dat).unwrap();
    let mut reader = std::io::BufReader::new(stream);
    let mut data = vec![];
    reader.read_until(b'\n', &mut data).unwrap();
    if data.is_empty() {
        return;
    }
    data.pop();
    let Response { value, error } = serde_json::from_slice(&data).unwrap();
    match value {
        Some(v) => println!("{}", v),
        _ => match op {
            kvs::Command::Get(_) => {
                println!("Key not found");
            }
            _ => {}
        },
    }
    match error {
        Some(e) => {
            eprintln!("{}", e);
            std::process::exit(1);
        }
        _ => {}
    }
}
