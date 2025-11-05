use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use indexmap::IndexMap;
use ordered_float::OrderedFloat;
use crate::{encode_int, encode_null};

pub static ZSET_STORE: LazyLock<ZSetStore> = LazyLock::new(|| ZSetStore::new());

pub struct ZSetStore {
    store: Mutex<HashMap<String, IndexMap<String, OrderedFloat<f32>>>>,
}

const EX: &'static str = "EX";

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
        println!("++++ Result {:?}", result);
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

    fn sort(&self, index_map : &mut IndexMap<String, OrderedFloat<f32>> , set_name: &str) {
        index_map.sort_by(|k1, v1, k2, v2|
            v1.cmp(v2).then_with(|| k1.cmp(k2))
        );
    }

}