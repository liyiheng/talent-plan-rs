use kvs::{Error, KvStore, KvsEngine, Result};
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(version = env!("CARGO_PKG_VERSION"))]
enum Commands {
    Set { key: String, value: String },
    Get { key: String },
    Rm { key: String },
}

fn main() -> Result<()> {
    let pb = PathBuf::from("./");
    let cmd = Commands::from_args();
    let mut db = KvStore::open(pb)?;
    match cmd {
        Commands::Set { key: k, value: v } => {
            db.set(k, v)?;
        }
        Commands::Get { key: k } => {
            let v = db.get(k)?;
            if let Some(v) = v {
                println!("{}", v);
            } else {
                println!("Key not found");
            }
        }
        Commands::Rm { key: k } => {
            let result = db.remove(k);
            if let Err(Error::KeyNotFound(_)) = result {
                println!("Key not found");
                std::process::exit(1);
            }
            result?;
        }
    }
    Ok(())
}
