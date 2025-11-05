use indexmap::IndexMap;
use tokio::time::timeout;
use crate::{decode_slice_to_value, decode_to_value, encode_error, encode_str, encode_vec, encode_vec_of_value, RdbSettings, ReplicaInstance, TXContext};
use crate::Handler::{LRange, RPush, LPush, Echo, Get, Null, Ping, Set, LLen, LPop, BLPop, Type, XAdd, XRange, XRead, Incr, Multi, Exec, Queued, Discard, Info, ReplConf, PSync, Wait, Config, Keys, Subscribe, Publish, ZAdd};
use crate::key_value_store::KV_STORE;
use crate::stream_store::STREAM_STORE;
use std::cell::RefCell;
use std::fmt;
use std::fmt::format;
use resp::Value;
use crate::channels::subscribe;
use crate::rdb::get_config;
use crate::replication::{get_info, psync, repl_conf, wait};
use crate::zset::ZSET_STORE;

#[derive(Debug)]
pub enum Handler<'a> {
    Ping,
    Multi,
    Discard(RefCell<&'a mut TXContext>),
    Exec(RefCell<&'a mut TXContext>),
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
    Queued,
    Config(String, String, RdbSettings),
    Keys,
    //Subscribe
    Subscribe(String),
    Publish(String),
    //Replication
    Info(String, ReplicaInstance),
    ReplConf(String, String, ReplicaInstance),
    PSync(String, String, ReplicaInstance),
    Wait(u64,u64),
    //ZSET
    ZAdd(String, f32, String),
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
const CONFIG: &str = "CONFIG";
const KEYS: &str = "KEYS";
// Transactions
const MULTI: &str = "MULTI";
const EXEC: &str = "EXEC";
const DISCARD: &str = "DISCARD";
//replication
const INFO: &str = "INFO";
const REPLCONF: &str = "REPLCONF";
const PSYNC: &str = "PSYNC";
const WAIT: &str = "WAIT";
//subscribe
const SUBSCRIBE: &str = "SUBSCRIBE";
const PUBLISH: &str = "PUBLISH";
//zset
const ZADD: &str = "ZADD";
//misc
const OK: &'static str = "OK";

const ERROR_EXEC_WITHOUT_MULTI: &str = "ERR EXEC without MULTI";
const ERROR_DISCARD_WITHOUT_MULTI: &str = "ERR DISCARD without MULTI";


const BLOCK: &str = "block";

impl fmt::Display for Handler<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Handler<'_> {
    pub fn from_command<'a>(vector: Vec<String>, tx_context: &'a mut TXContext, ri: &mut ReplicaInstance, rdb_settings: Option<RdbSettings>) -> Handler<'a> {
        if tx_context.is_active &&
            !vector.first().map(|s| s.as_str()).unwrap_or("").eq(EXEC) &&
            !vector.first().map(|s| s.as_str()).unwrap_or("").eq(DISCARD) {
            tx_context.store.push(vector.clone());
            println!("Adding command to queue: {:?}", vector);
            return Queued;
        }
        match vector.first().map(|s| s.as_str()) {
            Some(PING) => Ping,
            Some(MULTI) => {
                tx_context.is_active = true;
                Multi
            },
            Some(KEYS) => Keys,
            Some(EXEC) => {
                Exec(RefCell::new(tx_context))
            },
            Some(DISCARD) => {
                Discard(RefCell::new(tx_context))
            },
            Some(ECHO) => Self::parse_single_arg(&vector).map(Echo).unwrap_or(Null),
            Some(GET) => Self::parse_single_arg(&vector).map(Get).unwrap_or(Null),
            Some(LLEN) => Self::parse_single_arg(&vector).map(LLen).unwrap_or(Null),
            Some(TYPE) => Self::parse_single_arg(&vector).map(Type).unwrap_or(Null),
            Some(INCR) => Self::parse_single_arg(&vector).map(Incr).unwrap_or(Null),
            Some(SUBSCRIBE) => Self::parse_single_arg(&vector).map(Subscribe).unwrap_or(Null),
            Some(PUBLISH) => Self::parse_single_arg(&vector).map(Publish).unwrap_or(Null),
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
                let ( stream_name , start_id , end_id ) = Self::parse_three_args(&vector).unwrap_or_default();
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
            Some(INFO) => {
                let arg =Self::parse_single_arg(&vector).unwrap_or_default();
                Info(arg, ri.clone())
            },
            Some(REPLCONF) => {
                let (arg1, arg2) =Self::parse_two_args(&vector).unwrap_or_default();
                ReplConf(arg1,arg2, ri.clone())
            },
            Some(PSYNC) => {
                let (arg1, arg2) =Self::parse_two_args(&vector).unwrap_or_default();
                PSync(arg1,arg2, ri.clone())
            },
            Some(CONFIG) => {
                let (arg1, arg2) =Self::parse_two_args(&vector).unwrap_or_default();
                Config(arg1,arg2, rdb_settings.unwrap())
            },
            Some(WAIT) => {
                let (arg1, arg2) =Self::parse_two_args(&vector).unwrap_or_default();
                Wait(arg1.parse().unwrap_or_default(), arg2.parse().unwrap_or_default())
            },
            Some(ZADD) => {
                let (arg1, arg2, arg3) =Self::parse_three_args(&vector).unwrap_or_default();
                ZAdd(arg1, arg2.parse().unwrap(), arg3)
            },
            _ => Null,
        }
    }

    pub fn repl_from_command(vector: Vec<String>, ri: &mut ReplicaInstance) -> Handler<'static> {
        match vector.first().map(|s| s.as_str()) {
            Some(SET) => Self::parse_four_args(&vector)
                .map(|(key, value, expire_unit, expire_dur)| Set(key, value, expire_unit, expire_dur))
                .unwrap_or(Null),
            Some(GET) => Self::parse_single_arg(&vector).map(Get).unwrap_or(Null),
            Some(REPLCONF) => {
                let (arg1, arg2) =Self::parse_two_args(&vector).unwrap_or_default();
                ReplConf(arg1,arg2, ri.clone())
            },
            Some(PING) => Ping,
            _ => Null
        }
    }


    pub fn process_command(&self) -> Vec<u8> {
        match self {
            Ping => crate::encode_str("PONG"),
            Echo(str) => crate::encode_str(str),
            Incr(str) => KV_STORE.incr(str.clone()),
            Subscribe(str) =>vec!(),
            Publish(str) =>vec!(),
            Set(key, value, expire, expire_unit) => {
                KV_STORE.set(key.clone(), value.clone(), expire.clone() , expire_unit.clone())
            }
            Get(key) => KV_STORE.get(key),
            Type(key) => {
                KV_STORE.type_of(key)
                    .or_else(|| STREAM_STORE.type_of(key))
                    .map(|type_of| encode_str(type_of.as_str()))
                    .unwrap_or_else(|| encode_str("none"))
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
            Keys => KV_STORE.keys(),
            Multi => {
                encode_str(OK)
            },
            Discard(tx_context) => {
                if  { tx_context.borrow().is_active } {
                    let mut tx_context_borrowed = tx_context.borrow_mut();
                    tx_context_borrowed.is_active = false;
                    tx_context_borrowed.store.clear();
                    encode_str(OK)
                } else {
                    encode_error(ERROR_DISCARD_WITHOUT_MULTI)
                }

            }
            Exec(tx_context) => {
                if tx_context.borrow().is_active {
                    let mut tx_context_borrowed = tx_context.borrow_mut();
                    tx_context_borrowed.is_active = false;
                    let mut final_output: Vec<Value> = vec!();
                    for command in tx_context_borrowed.store.iter() {
                        println!("Executing command: {:?}", command);
                        let mut output = Handler::from_command(command.clone() , &mut TXContext::default(), &mut ReplicaInstance::default(), None).process_command();
                        final_output.push(decode_to_value(output));
                    }
                    encode_vec_of_value(final_output)
                } else {
                    encode_error(ERROR_EXEC_WITHOUT_MULTI)
                }
            },
            Config(arg1, arg2, rdb_settings) => get_config(arg1.to_string(), arg2.to_string(), rdb_settings.clone()),
            Queued => crate::encode_str("QUEUED"),
            Info(header,ri) => get_info(header.clone(), ri.clone()),
            ReplConf(arg1,arg2, ri) => repl_conf(arg1.clone(),arg2.clone(),ri.clone()),
            PSync(arg1,arg2, ri) => psync(arg1.clone(), arg2.clone(), ri.clone()),
            Wait(arg1,arg2) => wait(arg1, arg2),
            ZAdd(arg1, arg2, arg3) => ZSET_STORE.zadd(arg1, *arg2, arg3),
            Null => crate::encode_str("Command not recognized"),
        }
    }

    fn parse_single_arg(vector: &[String]) -> Option<String> {
        vector.get(1).cloned()
    }

    fn parse_two_args(vector: &[String]) -> Option<(String, String)> {
        Some((vector.get(1)?.clone(), vector.get(2)?.clone()))
    }

    fn parse_three_args(vector: &[String]) -> Option<(String, String, String)> {
        Some((vector.get(1)?.clone(), vector.get(2)?.clone(), vector.get(3)?.clone()))
    }

    fn parse_three_args3(vector: &[String]) -> (String, String, String) {
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
