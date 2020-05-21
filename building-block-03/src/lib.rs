//! A lib for RESP encoding and decoding
#![deny(missing_docs)]
use failure::{self, Error};
use serde::de::Visitor;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::io::prelude::*;

/// Represents all types of RESP
#[derive(Debug, Clone)]
pub enum RedisType {
    /// Simple string of RESP
    Str(String),
    /// Bulk string of RESP
    BulkStr(String),
    /// Errors of RESP
    Error(String),
    /// Integer of RESP
    Integer(i64),
    /// Array of RESP
    Array(Vec<RedisType>),
}

struct RedisTypeVisitor;

impl<'de> Visitor<'de> for RedisTypeVisitor {
    type Value = RedisType;
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "invalid data")
    }
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let mut reader = std::io::BufReader::new(v.as_bytes());
        from_reader(&mut reader).map_err(|e| E::custom(e.to_string()))
    }
}

impl<'de> Deserialize<'de> for RedisType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(RedisTypeVisitor)
    }
}

impl Serialize for RedisType {
    fn serialize<S>(&self, serilizer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            RedisType::BulkStr(v) => {
                let s = format!("${}\r\n{}\r\n", v.len(), v);
                serilizer.serialize_str(&s)
            }
            RedisType::Str(v) => {
                let s = format!("+{}\r\n", v);
                serilizer.serialize_str(&s)
            }
            RedisType::Error(v) => {
                let s = format!("-{}\r\n", v);
                serilizer.serialize_str(&s)
            }
            RedisType::Integer(v) => {
                let s = format!(":{}\r\n", v);
                serilizer.serialize_str(&s)
            }
            RedisType::Array(v) => {
                let mut seq = serilizer.serialize_seq(Some(v.len()))?;
                for ele in v.iter() {
                    seq.serialize_element(ele)?;
                }
                seq.end()
            }
        }
    }
}

/// Decode from reader
pub fn from_reader(reader: &mut impl BufRead) -> Result<RedisType, Error> {
    let mut one_byte = [0; 1];
    let mut first = [0; 1];
    reader.read_exact(&mut first)?;
    let first = first[0];
    match first {
        b'-' | b'+' => {
            let mut v = vec![];
            reader.read_until(b'\r', &mut v)?;
            reader.read_exact(&mut one_byte)?;
            v.pop();
            let s = String::from_utf8_lossy(&v).to_string();
            if first == b'-' {
                return Ok(RedisType::Error(s));
            }
            return Ok(RedisType::Str(s));
        }
        b':' => {
            let mut v = vec![];
            reader.read_until(b'\r', &mut v)?;
            reader.read_exact(&mut one_byte)?;
            v.pop();
            let s = String::from_utf8_lossy(&v).to_string();
            let i: i64 = s.parse()?;
            return Ok(RedisType::Integer(i));
        }
        b'$' => {
            let mut v = vec![];
            reader.read_until(b'\r', &mut v)?;
            v.pop();
            reader.read_exact(&mut one_byte)?;
            let len: isize = String::from_utf8_lossy(&v).parse()?;
            if len <= 0 {
                return Ok(RedisType::BulkStr(String::new()));
            }
            let mut data = vec![0; len as usize + 2];
            reader.read_exact(&mut data[..])?;
            data.pop();
            data.pop();
            return Ok(RedisType::Str(String::from_utf8_lossy(&data).to_string()));
        }
        b'*' => {
            let mut v = vec![];
            reader.read_until(b'\r', &mut v)?;
            v.pop();
            reader.read_exact(&mut one_byte)?;
            let len: isize = String::from_utf8(v).unwrap().parse()?;
            if len <= 0 {
                return Ok(RedisType::Array(vec![]));
            }
            let mut arr = Vec::with_capacity(len as usize);
            for _ in 0..len {
                let ele = from_reader(reader)?;
                arr.push(ele);
            }
            return Ok(RedisType::Array(arr));
        }
        _ => Err(failure::format_err!("Unknown type")),
    }
}

impl ToString for RedisType {
    fn to_string(&self) -> String {
        let mut resp = String::new();
        match self {
            RedisType::BulkStr(s) => {
                resp.push('$');
                resp.push_str(&s.len().to_string());
                resp.push('\r');
                resp.push('\n');
                resp.push_str(s);
                resp.push('\r');
                resp.push('\n');
            }
            RedisType::Str(s) => {
                resp.push('+');
                resp.push_str(s);
                resp.push('\r');
                resp.push('\n');
            }
            RedisType::Error(e) => {
                resp.push('-');
                resp.push_str(e);
                resp.push('\r');
                resp.push('\n');
            }
            RedisType::Integer(i) => {
                resp.push(':');
                resp.push_str(&i.to_string());
                resp.push('\r');
                resp.push('\n');
            }
            RedisType::Array(arr) => {
                resp.push('*');
                resp.push_str(&arr.len().to_string());
                resp.push('\r');
                resp.push('\n');
                for e in arr {
                    resp.push_str(&e.to_string());
                }
            }
        }
        resp
    }
}
