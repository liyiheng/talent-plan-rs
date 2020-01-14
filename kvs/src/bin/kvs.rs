use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(version = env!("CARGO_PKG_VERSION"))]
enum Commands {
    Set { key: String, value: String },
    Get { key: String },
    Rm { key: String },
}

fn main() {
    let cmd = Commands::from_args();
    match cmd {
        Commands::Set { key: _, value: _ } => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
        Commands::Get { key: _ } => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
        Commands::Rm { key: _ } => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
    }
}
