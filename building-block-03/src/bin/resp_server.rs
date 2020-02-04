use building_block_03 as bb3;
use building_block_03::RedisType;
use std::io::prelude::*;
use std::io::BufReader;
use std::net::TcpListener;
use std::net::TcpStream;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        if let Ok(s) = stream {
            std::thread::spawn(move || handle_stream(s));
        }
    }
}

fn handle_stream(mut s: TcpStream) {
    let mut reader = BufReader::new(&mut s);
    let plain = match bb3::from_reader(&mut reader) {
        Err(e) => {
            println!("{:?}", e);
            format!("{}", e)
        }
        Ok(r) => {
            println!("{:?}", r);
            gen_resp(r)
        }
    };
    let response = RedisType::Str(plain).to_string();
    s.write_all(response.as_bytes()).unwrap();
}

fn gen_resp(r: RedisType) -> String {
    match r {
        RedisType::Str(str_val) => {
            if &str_val == "PING" {
                "PONG".to_owned()
            } else {
                str_val
            }
        }
        RedisType::Array(cmds) => {
            if cmds.len() != 2 {
                return format!("{:?}", cmds);
            }
            if let RedisType::Str(first) = cmds.get(0).unwrap() {
                if first == "PING" {
                    if let RedisType::Str(arg) = cmds.get(1).unwrap() {
                        return arg.to_owned();
                    }
                }
            }
            return format!("{:?}", cmds);
        }
        v => format!("{:?}", v),
    }
}
