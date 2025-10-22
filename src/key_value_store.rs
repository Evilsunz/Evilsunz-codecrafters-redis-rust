use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::SystemTime;
use resp::{encode, Value};
use crate::encode_vec;

pub static KV_STORE: LazyLock<KeyValueStore> = LazyLock::new(|| KeyValueStore::new());

pub struct KeyValueStore {
    store: Mutex<HashMap<String, String>>,
    expire: Mutex<HashMap<String, u128>>,
    lists: Mutex<HashMap<String, Vec<String>>>,
}

const EX: &'static str = "EX";
const PX: &'static str = "PX";
const OK: &'static str = "OK";

impl KeyValueStore {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
            expire: Mutex::new(HashMap::new()),
            lists: Mutex::new(HashMap::new()),
        }
    }

    pub fn add_to_list(&self, list_name: String, mut values: Vec<String>) -> Vec<u8> {
        let mut lists = self.lists.lock().unwrap();
        let internal_list =lists.entry(list_name).and_modify(|v| v.append(&mut values)).or_insert(values);
        crate::encode_int(&internal_list.len())
    }

    pub fn list_range(&self, list_name: String, start : usize, end : usize) -> Vec<u8> {
        let mut lists = self.lists.lock().unwrap();
        let inner_list = match lists.get(&list_name) {
            Some(inner_list) => inner_list,
            None => return encode_vec(vec!()),
        };
        crate::encode_vec(inner_list[start..end].to_vec())
    }
    
    pub fn set(&self, key: String, value: String, expire_unit: Option<String>, expire_dur: Option<u128>) -> Vec<u8> {
        if let (Some(unit), Some(duration)) = (expire_unit, expire_dur) {
            match self.calculate_expiration_time(&unit, duration) {
                Ok(expire_time) => {
                    let mut expire = self.expire.lock().unwrap();
                    expire.insert(key.clone(), expire_time);
                }
                Err(err_msg) => {
                    return crate::encode_string(&err_msg);
                }
            }
        }

        let mut store = self.store.lock().unwrap();
        store.insert(key, value);
        crate::encode_string(OK)
    }

    fn calculate_expiration_time(&self, expire_unit: &str, expire_dur: u128) -> Result<u128, String> {
        let expire_in_millis = match expire_unit {
            EX => expire_dur * 1000,
            PX => expire_dur,
            _ => return Err(format!("Invalid expire unit: {}", expire_unit)),
        };

        let current_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        Ok(current_time + expire_in_millis)
    }

    pub fn get(&self, key: &str) -> Vec<u8> {
        let mut expire = self.expire.lock().unwrap();
        let mut store = self.store.lock().unwrap();
        if let Some(&stored_expiration) = expire.get(key) {
            if self.is_expired(stored_expiration) {
                expire.remove(key);
                store.remove(key);
                return encode(&Value::Null);
            }
        }
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
