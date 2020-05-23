use bb3::RedisType;
use building_block_03 as bb3;
use std::io::prelude::*;
use std::net::TcpStream;

fn main() {
    let addr = "127.0.0.1:6379";
    let reader = std::io::stdin();
    let mut stream = TcpStream::connect(addr).unwrap();
    loop {
        print!("{}>", addr);
        std::io::stdout().flush().unwrap();
        let mut line = String::new();
        if let Err(e) = reader.read_line(&mut line) {
            println!("{}", e.to_string());
            break;
        }
        let segs: Vec<&str> = line.split(' ').collect();
        let segs: Vec<RedisType> = segs
            .into_iter()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .map(|v| RedisType::BulkStr(v))
            .collect();
        let arr = RedisType::Array(segs);
        stream
            .write_all(bb3::custom_ser::to_resp(&arr).unwrap().as_bytes())
            .unwrap();
        let mut reader = std::io::BufReader::new(&mut stream);
        match bb3::from_reader(&mut reader).unwrap() {
            RedisType::Str(v) => println!("{}", v),
            RedisType::BulkStr(v) => println!("{}", v),
            RedisType::Integer(i) => println!("{}", i),
            RedisType::Error(e) => println!("ERROR: {}", e),
            RedisType::Array(a) => {
                for e in a {
                    println!("{:?}", e);
                }
            }
        }
    }
}
