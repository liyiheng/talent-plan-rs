use bson;
use ron;
use serde::{Deserialize, Serialize};
use serde_json;
use std::io::{Read, Result, Seek, SeekFrom, Write};

fn main() {
    json_with_file();
    ron_with_buffer();

    let f = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open("/tmp/b.bson")
        .unwrap();

    multi_bson(f);

    multi_bson(MyBuffer::default());
}

fn multi_bson<T: Read + Write + Seek>(mut buf: T) {
    for i in 0..1000 {
        let a = match i % 4 {
            0 => Move::Up(i),
            1 => Move::Down(i),
            2 => Move::Left(i),
            _ => Move::Right(i),
        };
        let b = bson::to_bson(&a).unwrap();
        let doc = b.as_document().unwrap();
        bson::encode_document(&mut buf, doc).unwrap();
    }
    buf.flush().unwrap();
    let len = buf.seek(SeekFrom::End(0)).unwrap();
    buf.seek(SeekFrom::Start(0)).unwrap();
    for i in 0..std::usize::MAX {
        let cur = buf.seek(SeekFrom::Current(0)).unwrap();
        if cur == len as u64 {
            println!("Break at {}", i);
            break;
        }
        bson::decode_document(&mut buf).unwrap();
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Move {
    Up(i32),
    Down(i32),
    Left(i32),
    Right(i32),
}

struct MyBuffer(Vec<u8>, usize);

impl Default for MyBuffer {
    fn default() -> Self {
        MyBuffer(vec![], 0)
    }
}

impl Write for MyBuffer {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if self.0.len() == self.1 {
            self.0.extend(buf);
            self.1 += buf.len();
        } else {
            let cnt = (&mut self.0[self.1..]).write(buf).unwrap();
            self.1 += cnt;
            if cnt < buf.len() {
                self.0.extend(&buf[cnt..]);
                self.1 += buf.len() - cnt;
            }
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Read for MyBuffer {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize> {
        let n = if self.1 + buf.len() > self.0.len() {
            buf.write(&self.0[self.1..]).unwrap()
        } else {
            buf.write(&self.0[self.1..self.1 + buf.len()]).unwrap()
        };
        self.1 += n;
        Ok(n)
    }
}
impl Seek for MyBuffer {
    fn seek(&mut self, s: SeekFrom) -> Result<u64> {
        let (from, offset) = match s {
            SeekFrom::Current(i) => (self.1 as i64, i as i64),
            SeekFrom::End(i) => (self.0.len() as i64, i as i64),
            SeekFrom::Start(i) => (0, i as i64),
        };
        let pos = (from + offset).min(self.0.len() as i64).max(0);
        self.1 = pos as usize;
        Ok(pos as u64)
    }
}

fn json_with_file() {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open("/tmp/a.json")
        .unwrap();
    let a = Move::Up(1);
    serde_json::to_writer(&mut f, &a).unwrap();
    f.flush().unwrap();
    f.seek(SeekFrom::Start(0)).unwrap();
    let b: Move = serde_json::from_reader(f).unwrap();
    println!("{:?}, {:?}", a, b);
}

fn ron_with_buffer() {
    let a = Move::Up(1);
    let bytes = ron::ser::to_string(&a).unwrap().into_bytes();
    let b: Move = ron::de::from_bytes(&bytes).unwrap();
    println!("{:?}, {:?}", a, b);
}
