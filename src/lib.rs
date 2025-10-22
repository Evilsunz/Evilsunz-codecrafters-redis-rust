mod handler;
mod key_value_store;

use anyhow::{Context, Result, bail};
use std::io::BufReader;
use resp::{encode, Decoder, Value};
pub use crate::handler::Handler;

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Bulk(bytes) => String::from_utf8_lossy(bytes.as_ref()).to_string(),
        Value::String(s) => s.clone(),
        Value::Integer(i) => i.to_string(),
        _ => format!("{:?}", value),
    }
}

pub fn decode_resp_array(buf: &[u8]) -> Option<Vec<String>> {
    let mut decoder = Decoder::new(BufReader::new(buf));
    let decoded = match decoder.decode() {
        Ok(val) => val,
        Err(_) => return None,
    };
    match decoded {
        Value::Array(array) => {
            let strings = array.iter()
                .map(value_to_string)
                .collect::<Vec<String>>();
            Some(strings)
        }
        _ => None,
    }
}

pub fn encode_string(s: &str) -> Vec<u8> {
    let val = Value::String(String::from(s));
    encode(&val)
}

pub fn encode_int(i: &usize) -> Vec<u8> {
    let val = Value::Integer(*i as i64);
    encode(&val)
}