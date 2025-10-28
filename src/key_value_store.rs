use crate::{encode_error, encode_int, encode_null, encode_str, encode_value, encode_vec, type_of, RespArray, RespNull, RespString, TXContext};
use resp::{encode, Value};
use std::collections::{HashMap, VecDeque};
use std::convert::TryInto;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::watch;

pub static KV_STORE: LazyLock<KeyValueStore> = LazyLock::new(|| KeyValueStore::new());

pub struct KeyValueStore {
    store: Mutex<HashMap<String, String>>,
    //int_store: Mutex<HashMap<String, i64>>,
    expire: Mutex<HashMap<String, u128>>,
    lists: Mutex<HashMap<String, Vec<String>>>,
    notifiers: Mutex<HashMap<String, watch::Sender<Option<String>>>>,
}

const EX: &'static str = "EX";
const PX: &'static str = "PX";
const OK: &'static str = "OK";
const NONE: &'static str = "none";
const ERROR_NOT_INT: &str = "ERR value is not an integer or out of range";

impl KeyValueStore {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
            expire: Mutex::new(HashMap::new()),
            lists: Mutex::new(HashMap::new()),
            notifiers: Mutex::new(HashMap::new()),
            // int_store: Mutex::new(HashMap::new()),
        }
    }

    pub fn pop_first_or_wait(&self, list_name: String, duration: Option<u64>) -> Vec<u8> {
        let val = self.pop_first(list_name.clone(), None);
        if val != RespNull.into() {
            return self.build_list_response(&list_name, val);
        }

        let mut rx = self
            .notifiers
            .lock()
            .unwrap()
            .entry(list_name.clone())
            .or_insert_with(|| watch::channel(None).0)
            .subscribe();

        let timeout_millis = duration.unwrap_or(0);
        let has_timeout = timeout_millis != 0;
        let start_time = Instant::now();
        let timeout_duration = Duration::from_millis(timeout_millis);

        loop {
            if has_timeout && start_time.elapsed() > timeout_duration {
                return encode(&Value::NullArray);
            }

            if rx.borrow_and_update().is_some() {
                let val = self.pop_first(list_name.clone(), None);
                // Somebody was first to pop the element from the list, so we are waiting for the next one
                if val == RespNull.into() {
                    continue;
                }
                return self.build_list_response(&list_name, val);
            }
        }
    }

    fn build_list_response(&self, list_name: &str, value: Value) -> Vec<u8> {
        let response = vec![Value::String(list_name.to_string()), value];
        encode(&Value::Array(response))
    }

    pub fn pop_first_no_wait(&self, list_name: String, count: Option<u64>) -> Vec<u8> {
        let value = self.pop_first(list_name, count);
        encode_value(value)
    }

    pub fn pop_first(&self, list_name: String, count: Option<u64>) -> Value {
        let mut lists = self.lists.lock().unwrap();
        let inner_list = match lists.get_mut(&list_name) {
            Some(list) => list,
            None => return RespNull.into(),
        };
        if inner_list.is_empty() {
            return RespNull.into();
        }
        if let Some(count) = count {
            return self.pop_multiple_elements(inner_list, count);
        }
        let first_value = inner_list.remove(0);
        RespString(first_value).into()
    }

    fn pop_multiple_elements(&self, list: &mut Vec<String>, count: u64) -> Value {
        let count_usize = count.try_into().unwrap_or(0);
        let items = list.drain(0..count_usize).collect::<Vec<String>>();
        RespArray(items).into()
    }

    pub fn len(&self, list_name: String) -> Vec<u8> {
        let lists = self.lists.lock().unwrap();
        let inner_list = match lists.get(&list_name) {
            Some(inner_list) => inner_list,
            None => return encode_int(&0),
        };
        crate::encode_int(&inner_list.len())
    }

    pub fn add_to_list(&self, list_name: String, mut values: Vec<String>) -> Vec<u8> {
        let mut lists = self.lists.lock().unwrap();
        let internal_list = lists
            .entry(list_name.clone())
            .and_modify(|v| v.append(&mut values))
            .or_insert(values);
        println!("Adding to list: {:?}", internal_list);
        if let Some(tx) = self.notifiers.lock().unwrap().get(&list_name) {
            println!("Sending notification");
            println!(
                " ++++++++ Sending notification to {:?}",
                tx.receiver_count()
            );
            let _ = tx.send(Some(String::from("UPD")));
        }
        println!("Done sending notification");
        encode_int(&internal_list.len())
    }

    pub fn add_to_list_left(&self, list_name: String, mut values: Vec<String>) -> Vec<u8> {
        let mut lists = self.lists.lock().unwrap();
        values.reverse();
        let internal_list = lists
            .entry(list_name)
            .and_modify(|v| {
                v.splice(0..0, values.iter().cloned());
            })
            .or_insert(values);
        crate::encode_int(&internal_list.len())
    }

    pub fn list_range(&self, list_name: String, start: isize, end: isize) -> Vec<u8> {
        let lists = self.lists.lock().unwrap();
        let inner_list = match lists.get(&list_name) {
            Some(inner_list) => inner_list,
            None => return encode_vec(vec![]),
        };

        let slice_indices = self.calculate_slice_indices(start, end, inner_list.len());
        match slice_indices {
            Some((start_idx, end_idx)) => encode_vec(inner_list[start_idx..=end_idx].to_vec()),
            None => encode_vec(vec![]),
        }
    }

    pub fn incr(&self, key: String) -> Vec<u8> {
            let mut int_store = self.store.lock().unwrap();
            if let Some(existing_value) = int_store.get(&key) {
                match existing_value.parse::<usize>() {
                    Ok(current) => {
                        let new_value = current + 1;
                        int_store.insert(key, new_value.to_string());
                        encode_int(&new_value)
                    }
                    Err(_) => encode_error(ERROR_NOT_INT),
                }
            } else {
                int_store.insert(key, String::from("1"));
                encode_int(&1)
            }
    }

    pub fn set(
        &self,
        key: String,
        value: String,
        expire_unit: Option<String>,
        expire_dur: Option<u128>,
    ) -> Vec<u8> {
        if let (Some(unit), Some(duration)) = (expire_unit, expire_dur) {
            match self.calculate_expiration_time(&unit, duration) {
                Ok(expire_time) => {
                    let mut expire = self.expire.lock().unwrap();
                    expire.insert(key.clone(), expire_time);
                }
                Err(err_msg) => {
                    return crate::encode_str(&err_msg);
                }
            }
        }
        let mut store = self.store.lock().unwrap();
        store.insert(key, value);
        crate::encode_str(OK)
    }

    fn calculate_expiration_time(
        &self,
        expire_unit: &str,
        expire_dur: u128,
    ) -> Result<u128, String> {
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

    pub fn type_of(&self, key: &str) -> Option<String> {
        let mut store = self.store.lock().unwrap();
        match store.get(key) {
            Some(value) => Some("string".to_string()),
            None => None,
        }
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
            Some(value) => encode_str(value),
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

    fn calculate_slice_indices(
        &self,
        start: isize,
        end: isize,
        list_len: usize,
    ) -> Option<(usize, usize)> {
        let normalized_start = self.normalize_index(start, list_len);
        let normalized_end = self.normalize_index(end, list_len);

        if normalized_start > normalized_end || normalized_start >= list_len as isize {
            return None;
        }

        let start_idx = normalized_start.max(0) as usize;
        let end_idx = normalized_end.min((list_len as isize) - 1).max(0) as usize;

        Some((start_idx, end_idx))
    }

    fn normalize_index(&self, index: isize, len: usize) -> isize {
        if index < 0 {
            len as isize + index
        } else {
            index
        }
    }
}
