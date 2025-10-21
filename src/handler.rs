use crate::handler::Handler::{Echo, Get, Null, Ping, Set};
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::SystemTime;
use resp::{encode, Value};

#[derive(Debug)]
pub enum Handler {
    Ping,
    Echo(String),
    Set(String, String, Option<String>, Option<u128>),
    Get(String),
    Null,
}

struct KeyValueStore {
    store: Mutex<HashMap<String, String>>,
    expire: Mutex<HashMap<String, u128>>,
}

impl KeyValueStore {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
            expire: Mutex::new(HashMap::new()),
        }
    }

    fn set(&self, key: String, value: String, expire_unit: Option<String>, expire_dur : Option<u128>) -> Vec<u8> {
        if expire_unit.is_some() && expire_dur.is_some() {
            let mut expire_in_millis;
            if expire_unit.clone().unwrap() == "EX" {
                expire_in_millis = expire_dur.unwrap() * 1000;
            } else if expire_unit.clone().unwrap() == "PX" {
                expire_in_millis = expire_dur.unwrap();
            } else {
                panic!("Invalid expire unit : {}",expire_unit.unwrap());
            }
            let expire_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() + expire_in_millis;
            let mut expire = self.expire.lock().unwrap();
            expire.insert(key.clone(), expire_time);
        }
        let mut store = self.store.lock().unwrap();
        store.insert(key, value);
        crate::encode_string("OK")
    }

    fn get(&self, key: &str) -> Vec<u8> {
        let mut expire = self.expire.lock().unwrap();
        let mut store = self.store.lock().unwrap();
        if let Some(&stored_expiration) = expire.get(key) {
            println!("stored_expiration: {}",stored_expiration);
            println!("current_timestamp: {}",SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis());
            if self.is_expired(stored_expiration) {
                expire.remove(key);
                store.remove(key);
                return encode(&Value::Null);
            }
        }
        println!("Returning get key: {}",key);
        match store.get(key) {
            Some(value) => crate::encode_string(value),
            None => encode(&Value::Null),
        }
    }

    fn is_expired(&self, expiration_timestamp: u128) -> bool {
        let current_timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        expiration_timestamp < current_timestamp
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
            Some(SET) => Self::parse_four_args(&vector)
                .map(|(key, value, expire_unit, expire_dur)| Set(key, value, expire_unit, expire_dur))
                .unwrap_or(Null),
            _ => Null,
        }
    }

    pub fn process_command(&self) -> Vec<u8> {
        match self {
            Ping => crate::encode_string("PONG"),
            Echo(str) => crate::encode_string(str),
            Set(key, value, expire, expire_unit) => {
                KV_STORE.set(key.clone(), value.clone(), expire.clone() , expire_unit.clone())
            }
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

    fn parse_four_args(vector: &[String]) -> Option<(String, String, Option<String>, Option<u128>)> {
        Some((
            vector.get(1)?.clone(),
            vector.get(2)?.clone(),
            vector.get(3).cloned(),
            vector.get(4).and_then(|s| s.parse().ok())
        ))
    }
}
