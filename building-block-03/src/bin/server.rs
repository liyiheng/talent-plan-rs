use bb3::custom_ser::to_resp;
use bb3::RedisType;
use building_block_03 as bb3;
use std::io::prelude::*;
use std::io::BufReader;
use std::net::TcpListener;
use std::net::TcpStream;

fn main() {
    let addr = "127.0.0.1:6379";
    println!("Listening {}", addr);
    println!("Please use redis-cli to have a try");
    let listener = TcpListener::bind(addr).unwrap();
    for stream in listener.incoming() {
        if let Ok(s) = stream {
            std::thread::spawn(move || handle_stream(s));
        }
    }
}

fn handle_stream(mut s: TcpStream) {
    let mut reader = BufReader::new(&mut s);
    loop {
        let msg = match bb3::from_reader(&mut reader) {
            Err(e) => {
                println!("{:?}", e);
                RedisType::Str(format!("{}", e))
            }
            Ok(r) => {
                println!("{:?}", r);
                gen_resp(r)
            }
        };
        let resp = to_resp(&msg).unwrap();
        if reader.get_mut().write_all(resp.as_bytes()).is_err() {
            break;
        }
    }
}

fn gen_resp(r: RedisType) -> RedisType {
    match r {
        RedisType::Str(str_val) => {
            if &str_val.to_uppercase() == "PING" {
                RedisType::Str("PONG".to_owned())
            } else {
                RedisType::BulkStr(str_val)
            }
        }
        RedisType::Array(cmds) => {
            if cmds.is_empty() {
                return RedisType::Str("".to_owned());
            }
            if let RedisType::Str(first) = cmds.get(0).unwrap() {
                if first.to_uppercase() == "PING" {
                    if let Some(v) = cmds.get(1) {
                        if let RedisType::Str(s) = v {
                            return RedisType::BulkStr(s.clone());
                        }
                        return v.clone();
                    } else {
                        return RedisType::Str("PONG".to_owned());
                    }
                }
            }
            return RedisType::BulkStr(format!("{:?}", cmds));
        }
        v => RedisType::BulkStr(format!("{:?}", v)),
    }
}
