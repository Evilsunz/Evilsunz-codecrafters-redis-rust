use crate::versions::VERSIONS;
use crate::{encode_bulk_string, encode_error, encode_int, encode_null, encode_value, encode_vec, encode_vec_as_bulk, RespArrayOfValue, RespBulkString, RespNull};
use dashmap::DashMap;
use resp::{encode, Value};
use std::convert::TryInto;
use std::sync::{LazyLock};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::watch;
use bitvec::prelude::*;

pub static KV_STORE: LazyLock<KeyValueStore> = LazyLock::new(|| KeyValueStore::new());

pub struct KeyValueStore {
    store: DashMap<String, String>,
    lists: DashMap<String, Vec<String>>,
    expire: DashMap<String, u128>,
    notifiers: DashMap<String, watch::Sender<Option<String>>>,
    versions_sender: UnboundedSender<(String, String)>,
}

const EX: &'static str = "EX";
const STORAGE: &'static str = "KV";
const PX: &'static str = "PX";
const OK: &'static str = "OK";
const ERROR_NOT_INT: &str = "ERR value is not an integer or out of range";

impl KeyValueStore {
    fn new() -> Self {
        Self {
            store: DashMap::new(),
            lists: DashMap::new(),
            expire: DashMap::new(),
            notifiers: DashMap::new(),
            versions_sender: VERSIONS.lock().unwrap().sender(),
        }
    }

    pub fn pop_first_or_wait(&self, list_name: String, duration: Option<u64>) -> Vec<u8> {
        let val = self.pop_first(list_name.clone(), None);
        if val != RespNull.into() {
            return self.build_list_response(&list_name, val);
        }

        let mut rx = self
            .notifiers
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
        let response = vec![Value::Bulk(list_name.to_string()), value];
        encode(&Value::Array(response))
    }

    pub fn pop_first_no_wait(&self, list_name: String, count: Option<u64>) -> Vec<u8> {
        let value = self.pop_first(list_name, count);
        encode_value(value)
    }

    pub fn pop_first(&self, list_name: String, count: Option<u64>) -> Value {
        let mut list_guard = match self.lists.get_mut(&list_name) {
            Some(guard) => guard,
            None => return RespNull.into(),
        };

        let inner_list = list_guard.value_mut();

        if inner_list.is_empty() {
            return RespNull.into();
        }

        if let Some(count) = count {
            return self.pop_multiple_elements(inner_list, count);
        }
        let first_value = inner_list.remove(0);
        RespBulkString(first_value).into()
    }

    fn pop_multiple_elements(&self, list: &mut Vec<String>, count: u64) -> Value {
        let count_usize = count.try_into().unwrap_or(0);
        let items = list.drain(0..count_usize).collect::<Vec<String>>();
        let values: Vec<Value> = items.iter().map(|v| Value::Bulk(v.clone())).collect();
        RespArrayOfValue(values).into()
    }

    pub fn len(&self, list_name: String) -> Vec<u8> {
        let list_guard = match self.lists.get(&list_name) {
            Some(guard) => guard,
            None => return encode_int(&0),
        };
        let inner_list = list_guard.value();
        encode_int(&inner_list.len())
    }

    pub fn add_to_list(&self, list_name: String, mut values: Vec<String>) -> Vec<u8> {
        let internal_list = self.lists
            .entry(list_name.clone())
            .and_modify(|v| v.append(&mut values))
            .or_insert(values);
        println!("Adding to list: {:?}", internal_list);
        let list_len = internal_list.len();
        if let Some(tx) = self.notifiers.get(&list_name) {
            println!("Sending notification");
            println!(
                " ++++++++ Sending notification to {:?}",
                tx.receiver_count()
            );
            let _ = tx.send(Some(String::from("UPD")));
        }
        println!("Done sending notification");

        encode_int(&list_len)
    }

    pub fn add_to_list_left(&self, list_name: String, mut values: Vec<String>) -> Vec<u8> {
        values.reverse();
        let internal_list = self.lists
            .entry(list_name)
            .and_modify(|v| {
                v.splice(0..0, values.iter().cloned());
            })
            .or_insert(values);
        encode_int(&internal_list.len())
    }

    pub fn list_range(&self, list_name: String, start: isize, end: isize) -> Vec<u8> {
        let list_guard = match self.lists.get(&list_name) {
            Some(guard) => guard,
            None => return encode_vec(vec![]),
        };
        let inner_list = list_guard.value();

        let slice_indices = self.calculate_slice_indices(start, end, inner_list.len());
        match slice_indices {
            Some((start_idx, end_idx)) => {
                encode_vec_as_bulk(inner_list[start_idx..=end_idx].to_vec())
            }
            None => encode_vec(vec![]),
        }
    }

    pub fn incr(&self, key: String) -> Vec<u8> {
        use dashmap::Entry;
        match self.store.entry(key) {
            Entry::Occupied(mut occupied) => {
                match occupied.get().parse::<usize>() {
                    Ok(current) => {
                        let new_value = current + 1;
                        occupied.insert(new_value.to_string());
                        encode_int(&new_value)
                    }
                    Err(_) => encode_error(ERROR_NOT_INT),
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(String::from("1"));
                encode_int(&1)
            }
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
                    self.expire.insert(key.clone(), expire_time);
                }
                Err(err_msg) => {
                    return crate::encode_str(&err_msg);
                }
            }
        }
        let key_versions = key.clone();
        self.store.insert(key, value);
        let _ = self
            .versions_sender
            .send((STORAGE.to_string(), key_versions));
        crate::encode_str(OK)
    }

    pub fn keys(&self) -> Vec<u8> {
        let result = self
            .store
            .iter()
            .map(|kv| kv.key().clone())
            .collect::<Vec<String>>();
        encode_vec_as_bulk(result)
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
            .map_err(|e| e.to_string())?
            .as_millis();

        Ok(current_time + expire_in_millis)
    }

    pub fn type_of(&self, key: &str) -> Option<String> {
        match self.store.get(key) {
            Some(_) => Some("string".to_string()),
            None => None,
        }
    }

    pub fn get(&self, key: &str) -> Vec<u8> {
        let is_expired = self.expire.get(key)
            .map(|r| self.is_expired(*r))
            .unwrap_or(false);

        if is_expired {
            self.expire.remove(key);
            self.store.remove(key);
            return encode_null();
        }
        match self.store.get(key) {
            Some(guard) => encode_bulk_string(guard.value().to_string()),
            None => encode_null(),
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

    pub fn set_bit(&self, key: String, offset: &str, value: &str) -> Vec<u8> {
        let offset = match offset.parse::<usize>() {
            Ok(num) => num,
            Err(_) => return encode_error("Value must be a valid number"),
        };

        let bit_value = match value {
            "1" => true,
            "0" => false,
            _ => return encode_error("ERR bit is not an integer or out of range"),
        };

        let mut guard = self.store.entry(key).or_insert_with(|| {
            self.serialize_bitvec(&BitVec::<u8, Msb0>::new())
        });

        let mut bv = self.deserialize_bitvec(guard.value());

        if offset >= bv.len() {
            let required_bits = offset + 1;
            let aligned_bits = required_bits.div_ceil(8) * 8;
            bv.resize(aligned_bits, false);
        }

        let old_bit = bv.replace(offset, bit_value);
        let final_len = bv.len().div_ceil(8) * 8;
        bv.truncate(final_len);

        *guard.value_mut() = self.serialize_bitvec(&bv);

        let old_value = if old_bit { 1 } else { 0 };
        encode_int(&old_value)
    }

    pub fn get_bit(&self, key: &str, offset: &str) -> Vec<u8> {
        let offset = match offset.parse::<usize>() {
            Ok(num) => num,
            Err(_) => return encode_error("ERR bit offset is not an integer or out of range"),
        };
        match self.store.get(key) {
            Some(guard) => {
                let bv = self.deserialize_bitvec(guard.value());
                let bit_value = if offset < bv.len() {
                    if *bv.get(offset).unwrap() { 1 } else { 0 }
                } else {
                    0
                };
                encode_int(&bit_value)
            }
            None => {
                encode_int(&0)
            }
        }
    }

    pub fn str_len(&self, key: &str) -> Vec<u8> {
        match self.store.get(key) {
            Some(guard) => {
                let value = guard.value();
                encode_int(&value.len())
            }
            None => {
                encode_int(&0)
            }
        }
    }

    pub fn bit_count(&self, key: &str, start: &Option<i64>, end: &Option<i64>) -> Vec<u8> {
        let guard = match self.store.get(key) {
            Some(g) => g,
            None => return encode_int(&0),
        };
        let bv = self.deserialize_bitvec(guard.value());
        let total_bytes = bv.len() / 8;

        if total_bytes == 0 {
            return encode_int(&0);
        }

        if start.is_none() || end.is_none() {
            return encode_int(&(bv.count_ones()));
        }

        let s_val = start.unwrap();
        let e_val = end.unwrap();

        let resolve_index = |pos: i64, max_len: i64| -> i64 {
            if pos < 0 {
                let calculated = max_len + pos;
                if calculated < 0 { 0 } else { calculated }
            } else {
                pos
            }
        };

        let start_byte = resolve_index(s_val, total_bytes as i64);
        let end_byte = resolve_index(e_val, total_bytes as i64);

        if start_byte >= total_bytes as i64 || start_byte > end_byte {
            return encode_int(&0);
        }

        let end_byte = std::cmp::min(end_byte, (total_bytes - 1) as i64);
        let start_bit = (start_byte as usize) * 8;
        let end_bit = ((end_byte as usize) + 1) * 8;

        let end_bit = std::cmp::min(end_bit, bv.len());

        let count = bv[start_bit..end_bit].count_ones();
        encode_int(&(count))
    }


    pub fn bit_op(&self, op: &str, dest_key: String, src_keys: Vec<String>) -> Vec<u8> {
        let op = op.to_uppercase();
        if src_keys.is_empty() {
            return encode_error("ERR BITOP must be called with at least one source key");
        }
        let mut bitvecs: Vec<BitVec<u8, Msb0>> = Vec::new();
        let mut max_len_bits = 0;

        for key in &src_keys {
            if let Some(guard) = self.store.get(key) {
                let string_value = guard.value();
                let actual_len_bits = string_value.chars().count() * 8;
                max_len_bits = std::cmp::max(max_len_bits, actual_len_bits);

                let mut bv = self.deserialize_bitvec(string_value);
                bv.resize(actual_len_bits, false);
                bitvecs.push(bv);
            } else {
                bitvecs.push(BitVec::<u8, Msb0>::new());
            }
        }
        for bv in &mut bitvecs {
            if bv.len() < max_len_bits {
                bv.resize(max_len_bits, false);
            }
        }
        let mut result_bv = match bitvecs.first() {
            Some(bv) => bv.clone(),
            None => BitVec::<u8, Msb0>::new(),
        };
        match op.as_str() {
            "AND" => {
                for bv in bitvecs.iter().skip(1) {
                    result_bv &= bv;
                }
            }
            "OR" => {
                for bv in bitvecs.iter().skip(1) {
                    result_bv |= bv;
                }
            }
            _ => return encode_error("ERR syntax error or unknown BITOP operation"),
        };
        result_bv.truncate(max_len_bits);
        if result_bv.len() < max_len_bits {
            result_bv.resize(max_len_bits, false);
        }
        if max_len_bits == 0 {
            self.store.insert(dest_key, String::new());
            return encode_int(&0);
        }
        
        let serialized = self.serialize_bitvec(&result_bv);
        self.store.insert(dest_key, serialized);
        let result_len_bytes = max_len_bits / 8;
        encode_int(&(result_len_bytes))
    }

    pub fn serialize_bitvec(&self, bv: &BitVec<u8, Msb0>) -> String {
        let byte_count = bv.len() / 8;
        let mut bytes = vec![0u8; byte_count];
        for (i, bit) in bv.iter().enumerate() {
            let byte_idx = i / 8;
            let bit_idx = i % 8;
            if byte_idx < byte_count && *bit {
                bytes[byte_idx] |= 1 << (7 - bit_idx);
            }
        }
        bytes.iter().map(|&b| b as char).collect()
    }

    pub fn deserialize_bitvec(&self, s: &str) -> BitVec<u8, Msb0> {
        let bytes: Vec<u8> = s.chars().map(|c| c as u8).collect();
        let total_bits = bytes.len() * 8;
        let mut bv = BitVec::<u8, Msb0>::new();
        bv.resize(total_bits, false);
        for (i, &byte) in bytes.iter().enumerate() {
            for bit_idx in 0..8 {
                let bit_value = (byte & (1 << (7 - bit_idx))) != 0;
                bv.set(i * 8 + bit_idx, bit_value);
            }
        }

        bv
    }

}
