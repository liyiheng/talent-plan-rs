use clap::{App, Arg};
use kvs::{self, app::*};

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
    let (sub_cmd, args) = cmds.subcommand();
    let k = args.unwrap().value_of("key").unwrap();
    let v = args.unwrap().value_of("value");
    let op = match sub_cmd {
        "set" => kvs::Command::Set(&k, &v.unwrap()),
        "get" => kvs::Command::Get(&k),
        "rm" => kvs::Command::Rm(&k),
        _ => unreachable!(),
    };
    let mut client = Client::new(addr).unwrap();
    let resp = client.send(&op).unwrap();
    let Response { value, error } = resp;
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
