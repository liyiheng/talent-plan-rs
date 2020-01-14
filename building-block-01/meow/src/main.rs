#[macro_use]
extern crate clap;
use clap::App;
use std::env;
use std::ffi::OsString;

fn main() {
    let yaml = load_yaml!("cli.yml");
    let m = App::from_yaml(yaml).get_matches();
    match m.value_of("argument1") {
        Some(x) => println!("{:?}", x),
        None => println!("None"),
    }
    if check_env_vars().is_err() {
        panic!("panic");
    }
}

fn check_env_vars() -> Result<(), MyErr> {
    // Get PORT from .env
    // let port = dotenv!("PORT");

    let k = "PORT";
    // Get env-var from system
    if let Some(v) = env::var_os(k) {
        println!("Key:{}, value:{:?}", k, v);
        if v.to_string_lossy().parse::<u16>().is_err() {
            Err(MyErr::PortNotNumber(v))
        } else {
            Ok(())
        }
    } else {
        Err(MyErr::PortNotFound)
    }
}

enum MyErr {
    PortNotFound,
    PortNotNumber(OsString),
}
