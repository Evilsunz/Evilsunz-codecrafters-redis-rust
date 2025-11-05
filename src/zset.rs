use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use indexmap::IndexMap;
use ordered_float::OrderedFloat;
use crate::encode_int;

pub static ZSET_STORE: LazyLock<ZSetStore> = LazyLock::new(|| ZSetStore::new());

pub struct ZSetStore {
    store: Mutex<HashMap<String, IndexMap<OrderedFloat<f32>, String>>>,
}

const EX: &'static str = "EX";

impl ZSetStore {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }

    pub fn zadd(&self, set_name: &str, key : f32, val : &str ) -> Vec<u8> {
        let mut binding = self.store.lock().unwrap();
        let index_map = binding.entry(set_name.to_string()).or_insert_with(IndexMap::new);
        let result = index_map.insert(OrderedFloat(key), val.to_string());
        match result {
            Some(_) => encode_int(&(0 as usize)),
            None => encode_int(&(1 as usize))
        }
    }

    // pub fn len(&self, list_name: String) -> Vec<u8> {
    //     let lists = self.lists.lock().unwrap();
    //     let inner_list = match lists.get(&list_name) {
    //         Some(inner_list) => inner_list,
    //         None => return encode_int(&0),
    //     };
    //     crate::encode_int(&inner_list.len())
    // }

}