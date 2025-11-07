use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{LazyLock, Mutex};
use indexmap::IndexMap;
use ordered_float::OrderedFloat;
use resp::Value;
use crate::{encode_buf_bulk, encode_bulk_str, encode_int, encode_null, encode_vec, encode_vec_of_value};

pub static GEO_STORE: LazyLock<GeoStore> = LazyLock::new(|| GeoStore::new());

pub struct GeoStore {
    store: Mutex<HashMap<String, IndexMap<String, OrderedFloat<f64>>>>,
}

impl GeoStore {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }

    pub fn geoadd(&self, set_name: &str, lon : &f64, lat : &f64 , place : &str ) -> Vec<u8> {
        let mut binding = self.store.lock().unwrap();
        let index_map = binding.entry(set_name.to_string()).or_insert_with(IndexMap::new);
        encode_int(&1)
        // let result = index_map.insert(key.to_string(),OrderedFloat(val));
        // //inserts less that readings ? or vise versa
        // self.sort(index_map, set_name);
        // match result {
        //     Some(_) => encode_int(&(0 as usize)),
        //     None => encode_int(&(1 as usize))
        // }
    }

}