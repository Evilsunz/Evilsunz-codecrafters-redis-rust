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

struct KeyValueStore {
    store: Mutex<HashMap<String, String>>,
}

impl KeyValueStore {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }

    fn set(&self, key: String, value: String) -> Vec<u8> {
        let mut store = self.store.lock().unwrap();
        store.insert(key, value);
        crate::encode_string("OK")
    }

    fn get(&self, key: &str) -> Vec<u8> {
        let store = self.store.lock().unwrap();
        match store.get(key) {
            Some(value) => crate::encode_string(value),
            None => encode(&Value::Null),
        }
    }
}

static KV_STORE: LazyLock<KeyValueStore> = LazyLock::new(|| KeyValueStore::new());

const PING: &str = "PING";
const ECHO: &str = "ECHO";
const GET: &str = "GET";
const SET: &str = "SET";

impl Handler {
    pub fn from_command(vector: Vec<String>) -> Handler {
        match vector.first().map(|s| s.as_str()) {
            Some(PING) => Ping,
            Some(ECHO) => Self::parse_single_arg(&vector).map(Echo).unwrap_or(Null),
            Some(GET) => Self::parse_single_arg(&vector).map(Get).unwrap_or(Null),
            Some(SET) => Self::parse_two_args(&vector)
                .map(|(key, value)| Set(key, value))
                .unwrap_or(Null),
            _ => Null,
        }
    }

    pub fn process_command(&self) -> Vec<u8> {
        match self {
            Ping => crate::encode_string("PONG"),
            Echo(str) => crate::encode_string(str),
            Set(key, value) => KV_STORE.set(key.clone(), value.clone()),
            Get(key) => KV_STORE.get(key),
            Null => b"Command not recognized\r\n".to_vec(),
        }
    }

    fn parse_single_arg(vector: &[String]) -> Option<String> {
        vector.get(1).cloned()
    }

    fn parse_two_args(vector: &[String]) -> Option<(String, String)> {
        Some((vector.get(1)?.clone(), vector.get(2)?.clone()))
    }
}
