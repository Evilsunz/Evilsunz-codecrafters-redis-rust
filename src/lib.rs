mod handler;
mod key_value_store;
mod stream_store;
mod replication;

use std::any::type_name;
use anyhow::{Context, Result, bail};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use resp::{encode, Decoder, Value};
pub use crate::handler::Handler;
use rand::{rng, Rng};
use rand::distr::Alphanumeric;
pub use crate::replication::get_rdb_file;
use crate::stream_store::Stream;

const PING: &str = "PING";
const REPL_CONF1_1: &str = "REPLCONF";
const REPL_CONF1_2: &str = "listening-port";
const REPL_CONF2_1: &str = "REPLCONF";
const REPL_CONF2_2: &str = "capa";
const REPL_CONF2_3: &str = "psync2";
const PSYNC1: &str = "PSYNC";
const PSYNC2: &str = "?";
const PSYNC3: &str = "-1";



#[derive(Debug, Clone)]
pub struct TXContext {
    pub is_active: bool,
    pub store: Vec<Vec<String>>
}

// role: String::from("master"),
// master_replid : generate_master_repl_id(),
// master_repl_offset: 0

#[derive(Debug, Clone,Eq,PartialEq)]
pub struct ReplicaInstance {
    pub is_replica: bool,
    pub master_ip: String,
    pub master_port: u16,
    pub own_port: u16,
    role: String,
    master_replid : String,
    master_repl_offset: u16
}

impl ReplicaInstance {
    pub fn create_replica(master_ip: String, own_port: u16) -> Self {
        println!("Replica of {}", master_ip);
        let ( master_ip ,master_port) = master_ip.split_at(master_ip.find(' ').unwrap());
        let master_port = master_port[1..].parse::<u16>().unwrap();
        ReplicaInstance {
            is_replica: true,
            master_ip: master_ip.to_string(),
            master_port,
            own_port,
            role : String::from("slave"),
            master_replid : String::from("0000000000000000000000000000000000000000"),
            master_repl_offset: 0
        }
    }

    pub fn get_info(&self) -> String {
        format!("role:{} master_replid:{} master_repl_offset:{}", self.role, self.master_replid, self.master_repl_offset)
    }

    pub fn connect_to_master(&self) {
        if self.is_replica {
            let mut stream = TcpStream::connect(format!("{}:{}", self.master_ip, self.master_port)).unwrap();
            let ping = encode_vec(vec!(PING.to_string()));
            stream.write_all(&ping);
            let mut buffer = [0; 512];
            stream.read(&mut buffer).unwrap();
            println!("Received ping response: {:?}", decode_slice_to_value(&buffer));
            let repl_conf = encode_vec(vec!(REPL_CONF1_1.to_string(), REPL_CONF1_2.to_string(), self.own_port.to_string()));
            stream.write_all(&repl_conf);
            let mut buffer = [0; 512];
            stream.read(&mut buffer).unwrap();
            println!("Received REPL 1 response: {:?}", decode_slice_to_value(&buffer));
            let repl_conf = encode_vec(vec!(REPL_CONF2_1.to_string(), REPL_CONF2_2.to_string(), REPL_CONF2_3.to_string()));
            stream.write_all(&repl_conf);
            let repl_conf = encode_vec(vec!(PSYNC1.to_string(), PSYNC2.to_string(), PSYNC3.to_string()));
            stream.write_all(&repl_conf);
            let mut buffer = [0; 512];
            stream.read(&mut buffer).unwrap();
            println!("Received PSYNC response: {:?}", decode_slice_to_value(&buffer));
            // let mut buffer = [0; 512];
            // stream.read(&mut buffer).unwrap();
            // println!("Received FULLRESYNC: {:?}", String::from_utf8_lossy(&buffer));
            read_fullresync(stream.try_clone().unwrap());
            //read_rdb(stream.try_clone().unwrap());
            // let mut buffer = [0; 512];
            // stream.read(&mut buffer).unwrap();
            // println!("Received RDB: {:?}", String::from_utf8_lossy(&buffer));
            loop{
                let mut buffer = [0; 512];
                let size =stream.read(&mut buffer).unwrap();
                xxxx(&mut buffer).iter().for_each(|x| {
                    let decoded = decode_resp_array(x).unwrap();
                    Handler::repl_from_command(decoded).process_command();
                }
                );
            }
        }
    }
}

pub fn read_fullresync(stream: TcpStream){
    let mut buffer: Vec<u8> = vec!();;
    let mut reader = BufReader::new(stream);
    reader.read_until(b'\n', &mut buffer);
    println!("Readed {:?}", String::from_utf8_lossy(buffer.as_slice()));
    reader.read_until(b'\n', &mut buffer);
    let rdb_len: u16 = ((buffer[1] as u16) << 8) | buffer[2] as u16;
    println!("Readed {:?}", String::from_utf8_lossy(buffer.as_slice()));
    let mut new_buffer = [0,88];
    reader.read_exact( &mut new_buffer);
    println!("Readed {:?}", String::from_utf8_lossy(new_buffer.as_slice()));
    println!("Len {:?}", rdb_len);
}

pub fn read_rdb(stream: TcpStream){
    let mut buffer: Vec<u8> = vec!();;
    let mut reader = BufReader::new(stream);
    reader.read_until(b'\n', &mut buffer);
    println!("Readed {:?}", String::from_utf8_lossy(buffer.as_slice()));
    //println!("Len {:?}", rdb_len);
}

pub fn xxxx(buffer: &mut [u8]) -> Vec<Vec<u8>>{
    let mut reader = BufReader::new(buffer.as_ref());
    let mut vector:Vec<u8> = vec!();
    reader.read_until(b'\0', &mut vector);
    let new_vector = vector[..vector.len()-1].to_vec();
    let vec: Vec<Vec<u8>> = new_vector.split(|x| x.eq(&b'*'))
        .filter(|x| !x.is_empty())
        .map(|x| {
            let mut vec:Vec<u8> = vec!(b'*');
            vec.splice(1.., x.iter().cloned());
            vec
        }).collect();
    vec.iter().for_each(|x| println!(" We have !!!!! {:?}",decode_resp_array(&x[..x.len()])));
    vec
}

impl Default for ReplicaInstance {
     fn default() -> Self {
        ReplicaInstance {
            is_replica: false,
            master_ip: String::new(),
            master_port: 0,
            own_port: 0,
            role : String::from("master"),
            master_replid : generate_master_repl_id(),
            master_repl_offset: 0
        }
    }
}

impl Default for TXContext {
    fn default() -> Self {
        TXContext {
            is_active: false,
            store: vec![],
        }
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Bulk(bytes) => String::from_utf8_lossy(bytes.as_ref()).to_string(),
        Value::String(s) => s.clone(),
        Value::Integer(i) => i.to_string(),
        _ => format!("{:?}", value),
    }
}

pub fn decode_to_value(vec : Vec<u8>) -> Value {
    decode_slice_to_value(&vec)
}

pub fn decode_slice_to_value(slice : &[u8]) -> Value {
    let mut decoder = Decoder::new(BufReader::new(slice));
    decoder.decode().unwrap()
}

pub fn decode_resp_array(buf: &[u8]) -> Option<Vec<String>> {
    let mut decoder = Decoder::new(BufReader::new(buf));
    let decoded = match decoder.decode() {
        Ok(val) => val,
        Err(_) => return None,
    };
    match decoded {
        Value::Array(array) => {
            let strings = array.iter()
                .map(value_to_string)
                .collect::<Vec<String>>();
            Some(strings)
        }
        _ => None,
    }
}

pub fn encode_value<T: Into<Value>>(value: T) -> Vec<u8> {
    encode(&value.into())
}

// Helper wrapper types for conversions
pub struct RespNull;
pub struct RespInt(pub usize);
pub struct RespArray(pub Vec<String>);
pub struct RespArrayOfValue(pub Vec<Value>);
pub struct RespArrayOfValueBulk(pub Vec<u8>);
pub struct RespString(pub String);
pub struct RespBulkString(pub String);
pub struct RespError(pub String);
pub struct RespBulkBuf(pub Vec<u8>);


impl Into<Value> for RespNull {
    fn into(self) -> Value {
        Value::Null
    }
}

impl Into<Value> for RespString {
    fn into(self) -> Value {
        Value::String(self.0)
    }
}

impl Into<Value> for RespBulkString {
    fn into(self) -> Value {
        Value::Bulk(self.0)
    }
}

impl Into<Value> for RespError {
    fn into(self) -> Value {
        Value::Error(self.0)
    }
}

impl Into<Value> for RespBulkBuf {
    fn into(self) -> Value {
        Value::BufBulk(self.0)
    }
}

impl Into<Value> for RespInt {
    fn into(self) -> Value {
        Value::Integer(self.0 as i64)
    }
}

impl Into<Value> for RespArray {
    fn into(self) -> Value {
        Value::Array(self.0.into_iter().map(Value::String).collect())
    }
}

impl Into<Value> for RespArrayOfValue {
    fn into(self) -> Value {
        Value::Array(self.0)
    }
}

// Convenience functions for backward compatibility
pub fn encode_null() -> Vec<u8> {
    encode_value(RespNull)
}

pub fn encode_str(s: &str) -> Vec<u8> {
    encode_value(RespString(String::from(s)))
}

pub fn encode_string(s: String) -> Vec<u8> {
    encode_value(RespString(s))
}

pub fn encode_bulk_string(s: &str) -> Vec<u8> {
    encode_value(RespBulkString(String::from(s)))
}

pub fn encode_error(s: &str) -> Vec<u8> {
    encode_value(RespError(String::from(s)))
}

pub fn encode_int(i: &usize) -> Vec<u8> {
    encode_value(RespInt(*i))
}

pub fn encode_vec(v: Vec<String>) -> Vec<u8> {
    encode_value(RespArray(v))
}

pub fn encode_vec_of_value(v: Vec<Value>) -> Vec<u8> {
    encode_value(RespArrayOfValue(v))
}

pub fn encode_buf_bulk(v: Vec<u8>) -> Vec<u8> {
    encode_value(RespBulkBuf(v))
}

fn type_of<T>(_: T) -> &'static str {
    type_name::<T>()
}

pub fn generate_master_repl_id() -> String{
    rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(|x| x as char)
        .collect()
}