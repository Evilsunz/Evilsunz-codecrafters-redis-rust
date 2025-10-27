use indexmap::IndexMap;
use tokio::time::timeout;
use crate::encode_string;
use crate::Handler::{LRange, RPush, LPush, Echo, Get, Null, Ping, Set, LLen, LPop, BLPop, Type, XAdd, XRange, XRead, Incr, Multi, Exec};
use crate::key_value_store::KV_STORE;
use crate::stream_store::STREAM_STORE;

#[derive(Debug)]
pub enum Handler {
    Ping,
    Multi,
    Exec,
    Echo(String),
    Set(String, String, Option<String>, Option<u128>),
    XAdd(String, String, Vec<String>),
    XRange(String, String, String),
    XRead(Option<u64>, IndexMap<String, String>),
    Get(String),
    RPush(String, Vec<String>),
    LPush(String, Vec<String>),
    LRange(String, isize, isize),
    LLen(String),
    LPop(String, Option<u64>),
    BLPop(String, Option<u64>),
    Type(String),
    Incr(String),
    Null,
}


const PING: &str = "PING";
const ECHO: &str = "ECHO";
const GET: &str = "GET";
const SET: &str = "SET";
const RPUSH: &str = "RPUSH";
const LPUSH: &str = "LPUSH";
const LRANGE: &str = "LRANGE";
const LLEN: &str = "LLEN";
const LPOP: &str = "LPOP";
const BLPOP: &str = "BLPOP";
const TYPE: &str = "TYPE";
const XADD: &str = "XADD";
const XRANGE: &str = "XRANGE";
const XREAD: &str = "XREAD";
const INCR: &str = "INCR";
const MULTI: &str = "MULTI";
const EXEC: &str = "EXEC";


const BLOCK: &str = "block";

impl Handler {
    pub fn from_command(vector: Vec<String>) -> Handler {
        match vector.first().map(|s| s.as_str()) {
            Some(PING) => Ping,
            Some(MULTI) => Multi,
            Some(EXEC) => Exec,
            Some(ECHO) => Self::parse_single_arg(&vector).map(Echo).unwrap_or(Null),
            Some(GET) => Self::parse_single_arg(&vector).map(Get).unwrap_or(Null),
            Some(LLEN) => Self::parse_single_arg(&vector).map(LLen).unwrap_or(Null),
            Some(TYPE) => Self::parse_single_arg(&vector).map(Type).unwrap_or(Null),
            Some(INCR) => Self::parse_single_arg(&vector).map(Incr).unwrap_or(Null),
            Some(XREAD) => {
                let (timeout , config) = Self::parse_hash_map(&vector);
                XRead(timeout, config)
            },
            Some(SET) => Self::parse_four_args(&vector)
                .map(|(key, value, expire_unit, expire_dur)| Set(key, value, expire_unit, expire_dur))
                .unwrap_or(Null),
            Some(XADD) => {
                let (list_name, id, values) = Self::parse_two_and_list_args(&vector);
                XAdd(list_name, id, values)
            },
            Some(RPUSH) => {
                let (list_name, values) = Self::parse_one_and_list_args(&vector);
                RPush(list_name, values)
            },
            Some(XRANGE) => {
                let ( stream_name , start_id , end_id ) = Self::parse_three_args(&vector);
                XRange(stream_name, start_id, end_id)
            },
            Some(LPUSH) => {
                let (list_name, values) = Self::parse_one_and_list_args(&vector);
                LPush(list_name, values)
            },
            Some(LPOP) => {
                let (list_name, values) = Self::parse_one_and_list_args(&vector);
                let start = values.get(0).and_then(|s| s.parse::<isize>().ok()).unwrap_or(0);
                let count = if start > 0 { Some(start as u64) } else { None };
                LPop(list_name, count)
            },
            Some(BLPOP) => {
                let (list_name, values) = Self::parse_one_and_list_args(&vector);
                println!("BLPOP: {:?}", values.get(0));
                let start = values.get(0).and_then(|s| s.parse::<f32>().ok()).unwrap_or(0.0);
                println!("BLPOP: {:?}", start * 1000.0);
                let count = if start > 0.0 { Some((start * 1000.0) as u64) } else { None };
                BLPop(list_name, count)
            },
            Some(LRANGE) => {
                let (list_name, values) = Self::parse_one_and_list_args(&vector);
                let start = values.get(0).and_then(|s| s.parse::<isize>().ok()).unwrap_or(0);
                let end = values.get(1).and_then(|s| s.parse::<isize>().ok()).unwrap_or(0);
                LRange(list_name, start, end)
            },
            _ => Null,
        }
    }

    pub fn process_command(&self) -> Vec<u8> {
        match self {
            Ping => crate::encode_string("PONG"),
            Echo(str) => crate::encode_string(str),
            Incr(str) => KV_STORE.incr(str.clone()),
            Set(key, value, expire, expire_unit) => {
                KV_STORE.set(key.clone(), value.clone(), expire.clone() , expire_unit.clone())
            }
            Get(key) => KV_STORE.get(key),
            Type(key) => {
                KV_STORE.type_of(key)
                    .or_else(|| STREAM_STORE.type_of(key))
                    .map(|type_of| encode_string(type_of.as_str()))
                    .unwrap_or_else(|| encode_string("none"))
            },
            RPush(list_name, values) => KV_STORE.add_to_list(list_name.clone(), values.clone()),
            LPush(list_name, values) => KV_STORE.add_to_list_left(list_name.clone(), values.clone()),
            LRange(list_name, start, end) => KV_STORE.list_range(list_name.clone(), *start, *end),
            LLen(list_name) => KV_STORE.len(list_name.clone()),
            LPop(list_name,elem_number) => KV_STORE.pop_first_no_wait(list_name.clone(),elem_number.clone()),
            BLPop(list_name,elem_number) => KV_STORE.pop_first_or_wait(list_name.clone(),elem_number.clone()),
            XAdd(stream_name, id, vec) => STREAM_STORE.add_stream(stream_name.clone(),id,vec.clone()),
            XRange(stream_name, start_id, end_id) => STREAM_STORE.get_xrange(stream_name.clone(), start_id.clone(), end_id.clone()),
            XRead(timeout, map) => STREAM_STORE.get_xread(map.clone(), timeout.clone()),
            Multi => KV_STORE.multi(),
            Exec => KV_STORE.exec(),
            Null => crate::encode_string("Command not recognized"),
        }
    }

    fn parse_single_arg(vector: &[String]) -> Option<String> {
        vector.get(1).cloned()
    }

    fn parse_two_args(vector: &[String]) -> Option<(String, String)> {
        Some((vector.get(1)?.clone(), vector.get(2)?.clone()))
    }

    fn parse_three_args(vector: &[String]) -> (String, String, String) {
        (vector.get(1).cloned().unwrap_or_default(), vector.get(2).cloned().unwrap_or_default(), vector.get(3).cloned().unwrap_or_default())
    }

    fn parse_four_args(vector: &[String]) -> Option<(String, String, Option<String>, Option<u128>)> {
        Some((
            vector.get(1)?.clone(),
            vector.get(2)?.clone(),
            vector.get(3).cloned(),
            vector.get(4).and_then(|s| s.parse().ok())
        ))
    }

    fn parse_one_and_list_args(vector: &[String]) -> (String, Vec<String>) {
        let list_name = vector.get(1).cloned().unwrap_or_default();
        let values = vector.iter().skip(2).cloned().collect::<Vec<String>>();
        (list_name, values)
    }

    fn parse_two_and_list_args(vector: &[String]) -> (String,String ,Vec<String>) {
        let list_name = vector.get(1).cloned().unwrap_or_default();
        let id = vector.get(2).cloned().unwrap_or_default();
        let values = vector.iter().skip(3).cloned().collect::<Vec<String>>();
        (list_name, id, values)
    }

    fn parse_hash_map(vector: &[String]) -> (Option<u64>, IndexMap<String, String>) {
        let timeout = match vector.get(1) {
            Some(cmd) if cmd.to_lowercase().eq(BLOCK) => vector.get(2).and_then(|s| s.parse::<u64>().ok()),
            _ => None,
        };

        let skip_count = if timeout.is_some() { 4 } else { 2 };
        let iter = vector.iter().skip(skip_count);

        let mut vec = iter.cloned().collect::<Vec<String>>();
        let vec2 = vec.split_off(vec.len()/2);
        let mut map: IndexMap<String, String> = IndexMap::new();
        vec.iter().zip(vec2.iter()).for_each(|(k,v)| {
            map.entry(k.to_string()).or_insert(v.to_string());
        });
        (timeout, map)
    }

}
