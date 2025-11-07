use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{LazyLock, Mutex};
use indexmap::IndexMap;
use ordered_float::OrderedFloat;
use crate::{encode_buf_bulk, encode_bulk_str, encode_error, encode_int, encode_null, encode_vec, encode_vec_of_value};

pub static GEO_STORE: LazyLock<GeoStore> = LazyLock::new(|| GeoStore::new());

const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;
const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;

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
        if !Self::validate(*lon, *lat){
            return encode_error(&format!("ERR invalid longitude,latitude pair {},{}",lon,lat))
        }
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

    fn validate(lon: f64 , lat : f64) -> bool {
        (lon >= MIN_LONGITUDE && lon <= MAX_LONGITUDE) && (lat >= MIN_LATITUDE && lat <= MAX_LATITUDE)
    }

}