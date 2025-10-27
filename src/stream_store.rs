use crate::{encode_string, RespNull};
use crate::{
    encode_bulk_string, encode_error, encode_vec, encode_vec_of_value, RespArray, RespArrayOfValue,
    RespBulkString,
};
use indexmap::IndexMap;
use resp::{encode, Value};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::watch;

pub static STREAM_STORE: LazyLock<StreamStore> = LazyLock::new(|| StreamStore::new());

const ERROR_ID_QE_OR_LESS: &str =
    "ERR The ID specified in XADD is equal or smaller than the target stream top item";
const ERROR_ID_0_0: &str = "ERR The ID specified in XADD must be greater than 0-0";

pub struct StreamStore {
    store: Mutex<HashMap<String, IndexMap<StreamId, Stream>>>,
    notifiers: Mutex<HashMap<String, watch::Sender<Option<String>>>>,
}

impl StreamStore {
    fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
            notifiers: Mutex::new(HashMap::new()),
        }
    }

    pub fn add_stream(&self, stream_key: String, id: &str, vec: Vec<String>) -> Vec<u8> {
        let mut store = self.store.lock().unwrap();

        let new_stream_id = match self.validate_new_stream_id(&store, &stream_key, id) {
            Ok(stream_id) => stream_id,
            Err(error_response) => return error_response,
        };

        let stream = Stream::from_vec(vec);
        let index_map = store.entry(stream_key.clone()).or_insert_with(IndexMap::new);
        index_map.insert(new_stream_id, stream);
        index_map.sort_unstable_keys();
        let new_stream_id_str = &format!("{}-{}", new_stream_id.time, new_stream_id.seq);
        if let Some(tx) = self.notifiers.lock().unwrap().get(&stream_key) {
            println!("Sending notification");
            println!(" ++++++++ Sending notification to {:?}", tx.receiver_count());
            let _ = tx.send(Some(new_stream_id_str.to_string()));
        }
        encode_bulk_string(new_stream_id_str)
    }

    pub fn get_xread(&self, stream_and_id : IndexMap<String, String>, timeout: Option<u64>) -> Vec<u8>{
        if timeout.is_some() {
            println!("Timeout is {:?}", timeout);
            let mut rx = self
                .notifiers
                .lock()
                .unwrap()
                .entry(stream_and_id.iter().nth(0).unwrap().0.clone())
                .or_insert_with(|| watch::channel(None).0)
                .subscribe();

            let timeout_millis = timeout.unwrap_or(0);
            let has_timeout = timeout_millis != 0;
            let start_time = Instant::now();
            let timeout_duration = Duration::from_millis(timeout_millis);

            loop {
                if has_timeout && start_time.elapsed() > timeout_duration {
                    return encode(&Value::NullArray);
                }

                if let Some(stream_id) = rx.borrow_and_update().clone() {
                    println!("Got stream id VIA CHANNEL : {:?}", stream_id);
                    self.notifiers.lock().unwrap().remove(stream_and_id.iter().nth(0).unwrap().0);
                    let mut new_stream_id : IndexMap<String,String> = IndexMap::new();
                    new_stream_id.insert(stream_and_id.iter().nth(0).unwrap().0.clone(), stream_id);
                    println!("New stream id map: {:?}", new_stream_id);
                    return self.xread(new_stream_id);
                }
            }
        } else {
            println!("No Timeout is {:?}", timeout);
            self.xread(stream_and_id)
        }
    }

    pub fn xread(&self, stream_and_id : IndexMap<String, String>) -> Vec<u8>{
        let mut final_result: Vec<Value> = vec![];
        stream_and_id.iter().for_each(|(stream_name, stream_id)| {
            let mut vec: Vec<Value> = vec![];
            let result = self.get_range(stream_name.clone(), stream_id.clone(), "+".to_string());
            vec.push(RespBulkString(stream_name.to_string()).into());
            vec.push(RespArrayOfValue(result).into());
            final_result.push(RespArrayOfValue(vec).into())
        });
        encode_vec_of_value(final_result)
    }

    pub fn get_xrange(&self, stream_key: String, start_id: String, end_id: String) -> Vec<u8>{
        encode_vec_of_value(self.get_range(stream_key, start_id, end_id))
    }

    pub fn get_range(&self, stream_key: String, start_id: String, end_id: String,) -> Vec<Value> {
        let start_stream_id = StreamId::parse_range(start_id, true);
        let end_stream_id = StreamId::parse_range(end_id, false);
        let mut store = self.store.lock().unwrap();
        let stream_map = store.get(&stream_key);
        if let Some(mut stream_map) = stream_map {
            let mut collected = stream_map
                .clone()
                .into_keys()
                .filter(|k| k.is_within_range_by_stream_id(start_stream_id, end_stream_id))
                .collect::<Vec<StreamId>>();
            let mut final_result: Vec<Value> = vec![];
            for stream_id in collected {
                let mut vec: Vec<Value> = vec![];
                let result = stream_map.get(&stream_id).unwrap();
                vec.push(RespBulkString(stream_id.to_string()).into());
                vec.push(RespArray(result.to_vec()).into());
                final_result.push(RespArrayOfValue(vec).into())
            }
            return final_result;
        }
        vec![]
    }

    fn validate_new_stream_id(
        &self,
        store: &HashMap<String, IndexMap<StreamId, Stream>>,
        stream_key: &str,
        id: &str,
    ) -> Result<StreamId, Vec<u8>> {
        //Autogenerated id
        if id.contains("*") {
            if id.len() == 1 {
                let start = SystemTime::now();
                let unix_millis = start
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                return Ok(StreamId {
                    time: unix_millis as i64,
                    seq: 0,
                });
            }

            let parsed_time = parse_time_prefix(id)?;

            let index_map = match store.get(stream_key) {
                Some(index_map) => index_map,
                None => return Ok(create_stream_id_with_default_seq(parsed_time)),
            };

            let last = match index_map
                .keys()
                .filter(|&&k| k.time == parsed_time)
                .copied()
                .last()
            {
                Some(id) => id,
                None => return Ok(create_stream_id_with_default_seq(parsed_time)),
            };

            return Ok(StreamId {
                time: last.time,
                seq: last.seq + 1,
            });
        }

        // Helper functions to extract:

        fn parse_time_prefix(id: &str) -> Result<i64, Vec<u8>> {
            let time_prefix = id.split("-").next().unwrap();
            time_prefix
                .parse::<i64>()
                .map_err(|_| encode_error("ERR invalid time format"))
        }

        fn create_stream_id_with_default_seq(time: i64) -> StreamId {
            StreamId {
                time,
                seq: if time == 0 { 1 } else { 0 },
            }
        }

        let new_stream_id = StreamId::new(id.to_string());
        if let Some(stream_map) = store.get(stream_key) {
            if let Some((last_stream_id, _)) = stream_map.last() {
                if let Err(e) = Self::check_id(*last_stream_id, new_stream_id) {
                    return Err(encode_error(e));
                }
            }
        }

        Ok(new_stream_id)
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

    fn to_string(&self) -> String {
        format!("{}-{}", self.time, self.seq)
    }

    fn parse_range(id: String, is_start: bool) -> Self {
        if id.eq("-"){
            return StreamId {
                time: 0,
                seq: 0,
            };
        }
        if id.eq("+"){
            return StreamId {
                time: i64::MAX,
                seq: i64::MAX,
            };
        }
        let mut id_vec: Vec<&str> = id.split("-").collect();
        if id_vec.len() == 1 {
            StreamId {
                time: id_vec[0].parse::<i64>().unwrap(),
                seq: if is_start { 0 } else { i64::MAX },
            }
        } else {
            StreamId {
                time: id_vec[0].parse::<i64>().unwrap(),
                seq: id_vec[1].parse::<i64>().unwrap(),
            }
        }
    }

    pub fn is_within_range_by_stream_id(&self, start: StreamId, end: StreamId) -> bool {
        self.time >= start.time
            && self.time <= end.time
            && self.seq >= start.seq
            && self.seq <= end.seq
    }

    pub fn is_within_range(
        &self,
        start_time: i64,
        end_time: i64,
        start_seq: i64,
        end_seq: i64,
    ) -> bool {
        self.time >= start_time
            && self.time <= end_time
            && self.seq >= start_seq
            && self.seq <= end_seq
    }
}

#[derive(Debug, Clone)]
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

    fn to_vec(&self) -> Vec<String> {
        self.entities
            .iter()
            .flat_map(|(k, v)| [k.clone(), v.clone()])
            .collect()
    }
}
