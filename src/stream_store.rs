use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::{LazyLock, Mutex};
use crate::encode_string;

pub static STREAM_STORE: LazyLock<StreamStore> = LazyLock::new(|| StreamStore::new());

pub struct StreamStore {
    store: Mutex<HashMap<String, Stream>>,
}

pub struct Stream {
    pub id: String,
    pub entities: HashMap<String, String>,
}

impl Display for Stream {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Stream {{ id: {}, entities: {:?} }}",
            self.id, self.entities
        )
    }
}

impl Stream {
    fn new(id: String) -> Self {
        Stream {
            id,
            entities: HashMap::new(),
        }
    }

    fn from_vec(id: String, entities: Vec<String>) -> Self {
        let mut stream = Stream::new(id);
        entities.chunks(2).for_each(|chunk| {
            let key = chunk[0].clone();
            let value = chunk[1].clone();
            stream.entities.insert(key, value);
        });
        stream
    }
}

impl StreamStore {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }

    pub fn add_stream(&self, stream_key: String, id: String, vec: Vec<String>) -> Vec<u8> {
        let mut store = self.store.lock().unwrap();
        let stream = Stream::from_vec(id.clone(), vec);
        store.insert(stream_key, stream);
        encode_string(id.as_str())
    }

    pub fn type_of(&self, key: &str) -> Option<String> {
        let mut store = self.store.lock().unwrap();
        match store.get(key) {
            Some(value) => Some("stream".to_string()),
            None => None,
        }
    }
    
}
