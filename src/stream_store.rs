use crate::encode_error;
use crate::encode_string;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::{LazyLock, Mutex};

pub static STREAM_STORE: LazyLock<StreamStore> = LazyLock::new(|| StreamStore::new());

const ERROR_ID_QE_OR_LESS: &str =
    "ERR The ID specified in XADD is equal or smaller than the target stream top item";
const ERROR_ID_0_0: &str = "ERR The ID specified in XADD must be greater than 0-0";

pub struct StreamStore {
    store: Mutex<HashMap<String, IndexMap<StreamId, Stream>>>,
}

impl StreamStore {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }

    pub fn add_stream(&self, stream_key: String, id: String, vec: Vec<String>) -> Vec<u8> {
        let mut store = self.store.lock().unwrap();
        if let Some(last_stream_id) = store.get(stream_key.as_str()).map(|map| map.last().map(|(id, _)| id)) {
            let new_stream_id = StreamId::new(id.clone());
            if let Err(e) = Self::check_id(*last_stream_id.unwrap(), new_stream_id) {
                return encode_error(e);
            }
        }
        let new_stream_id = StreamId::new(id.clone());
        let stream = Stream::from_vec(vec);
        let mut index_map = store.entry(stream_key).or_insert_with(IndexMap::new);
        index_map.insert(new_stream_id, stream);
        println!("{:?}", store);
        encode_string(id.as_str())
    }

    fn check_id(latest_stream: StreamId, new_stream: StreamId) -> Result<(), &'static str> {
        if new_stream.time == 0 && new_stream.seq == 0 {
            return Err(ERROR_ID_0_0);
        }
        if new_stream <= latest_stream {
            Err(ERROR_ID_QE_OR_LESS)
        } else {
            Ok(())
        }
    }

    pub fn type_of(&self, key: &str) -> Option<String> {
        let mut store = self.store.lock().unwrap();
        match store.get(key) {
            Some(value) => Some("stream".to_string()),
            None => None,
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Hash, Clone, Copy)]
pub struct StreamId {
    pub time: i64,
    pub seq: i64,
}

impl Ord for StreamId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time.cmp(&other.time).then(self.seq.cmp(&other.seq))
    }
}

impl StreamId {
    fn new(id: String) -> Self {
        let mut id_vec: Vec<&str> = id.split("-").collect();
        StreamId {
            time: id_vec[0].parse::<i64>().unwrap(),
            seq: id_vec[1].parse::<i64>().unwrap(),
        }
    }
}

#[derive(Debug)]
pub struct Stream {
    pub entities: HashMap<String, String>,
}

impl Stream {
    fn new() -> Self {
        Stream {
            entities: HashMap::new(),
        }
    }

    fn from_vec(entities: Vec<String>) -> Self {
        let mut stream = Stream::new();
        entities.chunks(2).for_each(|chunk| {
            let key = chunk[0].clone();
            let value = chunk[1].clone();
            stream.entities.insert(key, value);
        });
        stream
    }
}
