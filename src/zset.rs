use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{LazyLock, Mutex};
use indexmap::IndexMap;
use ordered_float::OrderedFloat;
use resp::Value;
use crate::{encode_buf_bulk, encode_bulk_str, encode_int, encode_null, encode_vec, encode_vec_of_value};

pub static ZSET_STORE: LazyLock<ZSetStore> = LazyLock::new(|| ZSetStore::new());

pub struct ZSetStore {
    store: Mutex<HashMap<String, IndexMap<String, OrderedFloat<f32>>>>,
}

impl ZSetStore {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }

    pub fn zadd(&self, set_name: &str, key : &str, val : f32 ) -> Vec<u8> {
        let mut binding = self.store.lock().unwrap();
        let index_map = binding.entry(set_name.to_string()).or_insert_with(IndexMap::new);
        let result = index_map.insert(key.to_string(),OrderedFloat(val));
        //inserts less that readings ? or vise versa
        self.sort(index_map, set_name);
        match result {
            Some(_) => encode_int(&(0 as usize)),
            None => encode_int(&(1 as usize))
        }
    }

    pub fn zrank(&self, set_name: &str, key : &str) -> Vec<u8> {
        let mut binding = self.store.lock().unwrap();
        let index_map = binding.entry(set_name.to_string()).or_insert_with(IndexMap::new);
        match index_map.get_full(key){
            Some((size , _ , _ )) => encode_int(&size),
            None => encode_null()
        }
    }

    pub fn zrange(&self, set_name: &str, start : isize, end : isize) -> Vec<u8> {
        let mut binding = self.store.lock().unwrap();
        let index_map = binding.entry(set_name.to_string()).or_insert_with(IndexMap::new);
        let result = Self::get_range(index_map, start, end).iter().map(|(_, k, _)| Value::Bulk(k.clone())).collect();
        encode_vec_of_value(result)
    }

    fn sort(&self, index_map : &mut IndexMap<String, OrderedFloat<f32>> , set_name: &str) {
        index_map.sort_by(|k1, v1, k2, v2|
            v1.cmp(v2).then_with(|| k1.cmp(k2))
        );
    }

    fn get_range(map: &IndexMap<String, OrderedFloat<f32>>, start: isize, end: isize) -> Vec<(usize, String, OrderedFloat<f32>)> {
        let len = map.len();

        let start_idx = if start < 0 {
            (len as isize + start).max(0) as usize
        } else {
            (start as usize).min(len)
        };

        let end_idx = if end < 0 {
            (len as isize + end + 1).max(0) as usize
        } else {
            (end as usize + 1).min(len)
        };

        (start_idx..end_idx)
            .filter_map(|i| {
                map.get_index(i).map(|(k, v)| (i, k.clone(), v.clone()))
            })
            .collect()
    }

}