use building_block_03 as bb3;
use std::io::prelude::*;
use std::net::TcpStream;

fn gen_data(i: i32) -> Vec<bb3::RedisType> {
    let mut arr = vec![];
    let a = bb3::RedisType::Str("PING".to_owned());
    let b = bb3::RedisType::Str(format!("Hello world {}", i));
    arr.push(a);
    arr.push(b);
    arr
}

fn main() {
    let mut stream = TcpStream::connect("127.0.0.1:4000").unwrap();
    let arr = bb3::RedisType::Array(gen_data(0));
    stream.write_all(arr.to_string().as_bytes()).unwrap();
    let mut reader = std::io::BufReader::new(stream);
    let response = bb3::from_reader(&mut reader).unwrap();
    println!("{:?}", response);
}
