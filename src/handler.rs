use crate::handler::Handler::{Echo, Get, Null, Ping, Set};
use crate::Handler::{LRange, RPush};
use crate::key_value_store::KV_STORE;

#[derive(Debug)]
pub enum Handler {
    Ping,
    Echo(String),
    Set(String, String, Option<String>, Option<u128>),
    Get(String),
    RPush(String, Vec<String>),
    LRange(String, usize, usize),
    Null,
}


const PING: &str = "PING";
const ECHO: &str = "ECHO";
const GET: &str = "GET";
const SET: &str = "SET";
const RPUSH: &str = "RPUSH";
const LRANGE: &str = "LRANGE";

impl Handler {
    pub fn from_command(vector: Vec<String>) -> Handler {
        match vector.first().map(|s| s.as_str()) {
            Some(PING) => Ping,
            Some(ECHO) => Self::parse_single_arg(&vector).map(Echo).unwrap_or(Null),
            Some(GET) => Self::parse_single_arg(&vector).map(Get).unwrap_or(Null),
            Some(SET) => Self::parse_four_args(&vector)
                .map(|(key, value, expire_unit, expire_dur)| Set(key, value, expire_unit, expire_dur))
                .unwrap_or(Null),
            Some(RPUSH) => {
                let (list_name, values) = Self::parse_all_list_args(&vector);
                RPush(list_name, values)
            },
            Some(LRANGE) => {
                println!("+++++++ {:?}", vector);
                let (list_name, values) = Self::parse_all_list_args(&vector);
                LRange(list_name, 0,2)
            },
            _ => Null,
        }
    }

    pub fn process_command(&self) -> Vec<u8> {
        match self {
            Ping => crate::encode_string("PONG"),
            Echo(str) => crate::encode_string(str),
            Set(key, value, expire, expire_unit) => {
                KV_STORE.set(key.clone(), value.clone(), expire.clone() , expire_unit.clone())
            }
            Get(key) => KV_STORE.get(key),
            RPush(list_name, values) => KV_STORE.add_to_list(list_name.clone(), values.clone()),
            LRange(list_name, start, end) => KV_STORE.list_range(list_name.clone(), *start, *end),
            Null => crate::encode_string("Command not recognized"),
        }
    }

    fn parse_single_arg(vector: &[String]) -> Option<String> {
        vector.get(1).cloned()
    }

    fn parse_two_args(vector: &[String]) -> Option<(String, String)> {
        Some((vector.get(1)?.clone(), vector.get(2)?.clone()))
    }

    fn parse_four_args(vector: &[String]) -> Option<(String, String, Option<String>, Option<u128>)> {
        Some((
            vector.get(1)?.clone(),
            vector.get(2)?.clone(),
            vector.get(3).cloned(),
            vector.get(4).and_then(|s| s.parse().ok())
        ))
    }

    fn parse_all_list_args(vector: &[String]) -> (String, Vec<String>) {
        let list_name = vector.get(1).cloned().unwrap_or_default();
        let values = vector.iter().skip(2).cloned().collect::<Vec<String>>();
        (list_name, values)
    }

}
