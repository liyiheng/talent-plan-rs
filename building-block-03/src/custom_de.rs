use crate::RedisType;
use serde::de;
use serde::de::SeqAccess;
use serde::de::{Deserialize, Visitor};
use std::fmt;
use std::result::Result as StdResult;

struct RespVisitor;

impl<'de> Visitor<'de> for RespVisitor {
    type Value = RedisType;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("invalid format")
    }
    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(RedisType::Integer(v))
    }

    fn visit_string<E>(self, value: String) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        use std::io::BufReader;
        let mut reader = BufReader::new(value.as_bytes());
        crate::from_reader(&mut reader).map_err(|e| de::Error::custom(e))
    }
    fn visit_seq<A>(self, mut seq: A) -> StdResult<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut v = Vec::with_capacity(seq.size_hint().unwrap_or_default());
        while let Some(e) = seq.next_element()? {
            v.push(e);
        }
        Ok(RedisType::Array(v))
    }
}
impl<'de> Deserialize<'de> for RedisType {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_any(RespVisitor)
    }
}
