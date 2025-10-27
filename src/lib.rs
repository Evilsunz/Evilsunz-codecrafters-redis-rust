mod handler;
mod key_value_store;
mod stream_store;

use std::any::type_name;
use anyhow::{Context, Result, bail};
use std::io::BufReader;
use resp::{encode, Decoder, Value};
pub use crate::handler::Handler;

#[derive(Debug)]
pub struct TXContext {
    pub is_active: bool,
    pub store: Vec<Vec<String>>
}

impl Default for TXContext {
    fn default() -> Self {
        TXContext {
            is_active: false,
            store: vec![],
        }
    }
}

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

pub fn encode_value<T: Into<Value>>(value: T) -> Vec<u8> {
    encode(&value.into())
}

// Helper wrapper types for conversions
pub struct RespNull;
pub struct RespInt(pub usize);
pub struct RespArray(pub Vec<String>);
pub struct RespArrayOfValue(pub Vec<Value>);
pub struct RespString(pub String);
pub struct RespBulkString(pub String);
pub struct RespError(pub String);


impl Into<Value> for RespNull {
    fn into(self) -> Value {
        Value::Null
    }
}

impl Into<Value> for RespString {
    fn into(self) -> Value {
        Value::String(self.0)
    }
}

impl Into<Value> for RespBulkString {
    fn into(self) -> Value {
        Value::Bulk(self.0)
    }
}

impl Into<Value> for RespError {
    fn into(self) -> Value {
        Value::Error(self.0)
    }
}

impl Into<Value> for RespInt {
    fn into(self) -> Value {
        Value::Integer(self.0 as i64)
    }
}

impl Into<Value> for RespArray {
    fn into(self) -> Value {
        Value::Array(self.0.into_iter().map(Value::String).collect())
    }
}

impl Into<Value> for RespArrayOfValue {
    fn into(self) -> Value {
        Value::Array(self.0)
    }
}

// Convenience functions for backward compatibility
pub fn encode_null() -> Vec<u8> {
    encode_value(RespNull)
}

pub fn encode_string(s: &str) -> Vec<u8> {
    encode_value(RespString(String::from(s)))
}

pub fn encode_bulk_string(s: &str) -> Vec<u8> {
    encode_value(RespBulkString(String::from(s)))
}

pub fn encode_error(s: &str) -> Vec<u8> {
    encode_value(RespError(String::from(s)))
}

pub fn encode_int(i: &usize) -> Vec<u8> {
    encode_value(RespInt(*i))
}

pub fn encode_vec(v: Vec<String>) -> Vec<u8> {
    encode_value(RespArray(v))
}

pub fn encode_vec_of_value(v: Vec<Value>) -> Vec<u8> {
    encode_value(RespArrayOfValue(v))
}

fn type_of<T>(_: T) -> &'static str {
    type_name::<T>()
}