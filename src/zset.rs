use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{LazyLock, Mutex};
use indexmap::IndexMap;
use ordered_float::OrderedFloat;
use resp::Value;
use crate::{encode_bulk_str, encode_bulk_string, encode_error, encode_int, encode_null, encode_vec_as_bulk, encode_vec_of_value};
use crate::geo_serde::{decode, distance, encode, Coordinates, MAX_LATITUDE, MAX_LONGITUDE, MIN_LATITUDE, MIN_LONGITUDE};

pub static ZSET_STORE: LazyLock<ZSetStore> = LazyLock::new(|| ZSetStore::new());

pub struct ZSetStore {
    store: Mutex<HashMap<String, IndexMap<String, OrderedFloat<f64>>>>,
}

impl ZSetStore {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }

    pub fn zadd(&self, set_name: &str, key : &str, val : f64 ) -> Vec<u8> {
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

    pub fn zcard(&self, set_name: &str) -> Vec<u8> {
        let mut binding = self.store.lock().unwrap();
        let len = binding
            .get(set_name)
            .map(|v| v.len())
            .unwrap_or(0);
        encode_int(&len)
    }

    pub fn zscore(&self, set_name: &str, key : &str) -> Vec<u8> {
        let mut binding = self.store.lock().unwrap();
        let score_opt = binding
            .get(set_name)
            .and_then(|v| v.get(key));

        match score_opt {
            Some(score) => encode_bulk_str(&score.to_string()),
            None => encode_null(),
        }
    }

    pub fn zrem(&self, set_name: &str, key : &str) -> Vec<u8> {
        let mut binding = self.store.lock().unwrap();
        let score_opt = binding
            .get_mut(set_name)
            .and_then(|v| v.shift_remove(key));

        match score_opt {
            Some(score) => encode_int(&1),
            None => encode_int(&0),
        }
    }

    //GEO

    pub fn geoadd(&self, set_name: &str, lon : &f64, lat : &f64 , place : &str ) -> Vec<u8> {
        if !Self::validate_lon_lat(*lon, *lat){
            return encode_error(&format!("ERR invalid longitude,latitude pair {},{}",lon,lat))
        }
        let mut binding = self.store.lock().unwrap();
        let index_map = binding.entry(set_name.to_string()).or_insert_with(IndexMap::new);
        let enc_lon_lat = encode(*lon, *lat);
        let result = index_map.insert(place.to_string(),OrderedFloat(enc_lon_lat as f64));
        //inserts less that readings ? or vise versa
        self.sort(index_map, set_name);
        match result {
            Some(_) => encode_int(&(0 as usize)),
            None => encode_int(&(1 as usize))
        }
    }

    pub fn geopos(&self, set_name: &str, places: Vec<String>) -> Vec<u8> {
        let mut binding = self.store.lock().unwrap();
        let index_map = binding.entry(set_name.to_string()).or_insert_with(IndexMap::new);
        let mut response = vec![];
        for place in places {
            let entry = index_map.get(&place);
            match entry {
                Some(coords) => {
                    let coords = decode(coords.into_inner() as u64);
                    response.push(Value::Array(vec![Value::Bulk(coords.lon.to_string()), Value::Bulk(coords.lat.to_string())]));
                }
                None => {response.push(Value::NullArray)}
            }
        }
        encode_vec_of_value(response)
    }

    pub fn geodist(&self, set_name: &str, place1 : &str, place2 : &str) -> Vec<u8> {
        let mut binding = self.store.lock().unwrap();
        let index_map = binding.entry(set_name.to_string()).or_insert_with(IndexMap::new);
        let coord1 = match index_map.get(place1) {
            None => {
                return encode_null()
            }
            Some(v) => {v}
        };

        let coord2 = match index_map.get(place2) {
            None => {
                return encode_null()
            }
            Some(v) => {v}
        };
        let dist = distance(decode(coord1.into_inner() as u64), decode(coord2.into_inner() as u64));
        encode_bulk_string(dist.to_string())
    }

    pub fn geosearch(&self, set_name: &str, lon : &f64, lat : &f64 , radius : &f64, unit : &str) -> Vec<u8> {
        let point = Coordinates{
            lat: *lat,
            lon: *lon,
        };
        let mut binding = self.store.lock().unwrap();
        let index_map = binding.entry(set_name.to_string()).or_insert_with(IndexMap::new);
        let mut response = vec![];
        for (key, value) in index_map {
            let lonlat = decode(value.into_inner() as u64);
            if (distance(point.clone(), lonlat) <= *radius){
                response.push(Value::Bulk(key.clone()));
            }
        }
        encode_vec_of_value(response)
    }

    //MISC

    fn validate_lon_lat(lon: f64, lat : f64) -> bool {
        (lon >= MIN_LONGITUDE && lon <= MAX_LONGITUDE) && (lat >= MIN_LATITUDE && lat <= MAX_LATITUDE)
    }

    fn sort(&self, index_map : &mut IndexMap<String, OrderedFloat<f64>> , set_name: &str) {
        index_map.sort_by(|k1, v1, k2, v2|
            v1.cmp(v2).then_with(|| k1.cmp(k2))
        );
    }

    fn get_range(map: &IndexMap<String, OrderedFloat<f64>>, start: isize, end: isize) -> Vec<(usize, String, OrderedFloat<f64>)> {
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