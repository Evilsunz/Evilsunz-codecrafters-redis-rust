use crate::handler::Handler::{Echo, Get, Null, Ping, Set};
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use resp::{encode, Value};

#[derive(Debug)]
pub enum Handler {
    Ping,
    Echo(String),
    Set(String, String),
    Get(String),
    Null,
}

static KV: LazyLock<Mutex<HashMap<String, String>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

const PING: &str = "PING";
const ECHO: &str = "ECHO";
const GET: &str = "GET";
const SET: &str = "SET";

impl Handler {
    pub fn from_command(vector: Vec<String>) -> Handler {
        println!(" ++++++ {:?}", vector.first());
        match vector.first().map(|s| s.as_str()) {
            Some(PING) => Ping,
            Some(ECHO) => Echo(vector.get(1).unwrap().clone()),
            Some(GET) => Get(vector.get(1).unwrap().clone()),
            Some(SET) => Set(
                vector.get(1).unwrap().clone(),
                vector.get(2).unwrap().clone(),
            ),
            _ => Null,
        }
    }

    pub fn process_command(&self) -> Vec<u8> {
        match self {
            Ping => crate::encode_string("PONG"),
            Echo(str) => {
                println!(" ++++++ Echo");
                crate::encode_string(str)},
            Set(key, value) => Self::handle_set(key, value),
            Get(key) => Self::handle_get(key),
            Null => b"Command not recognized\r\n".to_vec(),
        }
    }

    fn handle_set(key: &str, value: &str) -> Vec<u8> {
        let mut kv = KV.lock().unwrap();
        kv.insert(key.to_string(), value.to_string());
        crate::encode_string("OK")
    }

    fn handle_get(key: &str) -> Vec<u8> {
        let kv = KV.lock().unwrap();
        match kv.get(key) {
            Some(value) => crate::encode_string(value),
            None => encode(&Value::Null),
        }
    }
}
