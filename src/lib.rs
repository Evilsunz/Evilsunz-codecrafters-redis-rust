mod handler;
mod key_value_store;
mod stream_store;
mod replication;

use std::any::type_name;
use anyhow::{Context, Result, bail};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, LazyLock, Mutex};
use std::thread;
use std::time::Duration;
use resp::{encode, Decoder, Value};
pub use crate::handler::Handler;
use rand::{rng, Rng};
use rand::distr::Alphanumeric;
pub use crate::replication::get_rdb_file;
pub use crate::replication::ReplicaStream;

const PING: &str = "PING";
const REPL_CONF1_1: &str = "REPLCONF";
const REPL_CONF1_2: &str = "listening-port";
const REPL_CONF2_1: &str = "REPLCONF";
const REPL_CONF2_2: &str = "capa";
const REPL_CONF2_3: &str = "psync2";
const PSYNC1: &str = "PSYNC";
const PSYNC2: &str = "?";
const PSYNC3: &str = "-1";

const BUFFER_SIZE: usize = 512;
const RDB_HEADER_SIZE: usize = 88;

pub static REPLICA_STREAMS: LazyLock<Arc<Mutex<Vec<ReplicaStream>>>> = LazyLock::new(|| Arc::new(Mutex::new(Vec::new())));

#[derive(Debug, Clone)]
pub struct TXContext {
    pub is_active: bool,
    pub store: Vec<Vec<String>>
}

#[derive(Debug, Clone,Eq,PartialEq)]
pub struct ReplicaInstance {
    pub is_replica: bool,
    pub master_ip: String,
    pub master_port: u16,
    pub own_port: u16,
    role: String,
    master_replid : String,
    master_repl_offset: u16,
    bytes_offset: usize
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
            master_repl_offset: 0,
            bytes_offset: 0
        }
    }

    pub fn get_info(&self) -> String {
        format!("role:{} master_replid:{} master_repl_offset:{}", self.role, self.master_replid, self.master_repl_offset)
    }

    pub fn connect_to_master(&mut self) -> anyhow::Result<()> {
        if !self.is_replica {
            return Ok(());
        }

        let mut stream = TcpStream::connect(format!("{}:{}", self.master_ip, self.master_port))?;

        self.send_ping(&mut stream)?;
        self.send_replconf_listening_port(&mut stream)?;
        self.send_replconf_capabilities(&mut stream)?;
        self.send_psync(&mut stream)?;

        let mut reader = BufReader::new(stream.try_clone()?);
        self.read_rdb_header(&mut reader)?;

        thread::sleep(Duration::from_secs(1));
        self.process_replication_stream(reader, &mut stream)?;

        Ok(())
    }

    fn send_and_receive(&self, stream: &mut TcpStream, command: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        stream.write_all(&command)?;
        let mut buffer = [0; BUFFER_SIZE];
        stream.read(&mut buffer)?;
        Ok(buffer.to_vec())
    }

    fn send_ping(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
        let ping = encode_vec(vec![PING.to_string()]);
        let response = self.send_and_receive(stream, ping)?;
        println!("Received ping response: {:?}", decode_slice_to_value(&response));
        Ok(())
    }

    fn send_replconf_listening_port(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
        let repl_conf = encode_vec(vec![
            REPL_CONF1_1.to_string(),
            REPL_CONF1_2.to_string(),
            self.own_port.to_string(),
        ]);
        let response = self.send_and_receive(stream, repl_conf)?;
        println!("Received REPLCONF listening-port response: {:?}", decode_slice_to_value(&response));
        Ok(())
    }

    fn send_replconf_capabilities(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
        let repl_conf = encode_vec(vec![
            REPL_CONF2_1.to_string(),
            REPL_CONF2_2.to_string(),
            REPL_CONF2_3.to_string(),
        ]);
        stream.write_all(&repl_conf)?;
        Ok(())
    }

    fn send_psync(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
        let psync = encode_vec(vec![
            PSYNC1.to_string(),
            PSYNC2.to_string(),
            PSYNC3.to_string(),
        ]);
        let response = self.send_and_receive(stream, psync)?;
        println!("Received PSYNC response: {:?}", decode_slice_to_value(&response));
        Ok(())
    }

    fn read_rdb_header(&self, reader: &mut BufReader<TcpStream>) -> anyhow::Result<()> {
        let mut buffer: Vec<u8> = vec![];

        reader.read_until(b'\n', &mut buffer)?;
        println!("Read: {:?}", String::from_utf8_lossy(&buffer));

        reader.read_until(b'\n', &mut buffer)?;
        println!("Read: {:?}", String::from_utf8_lossy(&buffer));

        let mut rdb_content = [0; RDB_HEADER_SIZE];
        reader.read_exact(&mut rdb_content)?;
        println!("Read RDB content: {:?}", String::from_utf8_lossy(&rdb_content));

        Ok(())
    }

    fn process_replication_stream(&mut self, mut reader: BufReader<TcpStream>, stream: &mut TcpStream) -> anyhow::Result<()> {
        loop {
            let mut buffer = [0; BUFFER_SIZE];
            reader.read(&mut buffer)?;
            let (offset , commands) = self.parse_buffer_into_commands(&mut buffer);
            commands
                .iter()
                .for_each(|command_bytes| {
                    if let Some(decoded) = decode_resp_array(command_bytes) {
                        println!("Decoded command: {:?}", decoded);
                        let mut handler = Handler::repl_from_command(decoded, self);
                        let result =handler.process_command();
                        if !(handler.to_string().starts_with("Ping")
                            || handler.to_string().starts_with("Set")){
                                stream.write_all(&result);
                        }
                        //todo!(" over assign in the loop ");
                        self.bytes_offset += command_bytes.len();
                    }
                });
        }
    }

    pub fn parse_buffer_into_commands(&mut self, buffer: &mut [u8]) -> (usize,  Vec<Vec<u8>> ) {
        let mut reader = BufReader::new(buffer.as_ref());
        let mut vector: Vec<u8> = vec![];
        reader.read_until(b'\0', &mut vector);
        vector.remove(vector.len() - 1);
        let offset = vector.len();
        // Try to parse the entire buffer as a single RESP array
        if let Some(result) = try_parse_as_resp_array(&vector) {
            if (result.get(0).unwrap().len() >= vector.len()){
                return (offset, result);
            }
        }

        // If that fails, split by delimiter and parse each part
        let commands = split_buffer_by_delimiter(&vector);

        // Debug output for each parsed command
        for command in &commands {
            println!("Parsed command: {:?}", decode_resp_array(command));
        }

        (offset, commands)
    }

}

fn try_parse_as_resp_array(buffer: &[u8]) -> Option<Vec<Vec<u8>>> {
    let decoded = decode_resp_array(buffer);
    if decoded.is_some(){
        let entries_count = buffer.iter().filter(|&n| *n == b'*').count();
        if entries_count > 2 {
            return None;
        }
    }
    decode_resp_array(buffer).map(|_| vec![buffer.to_vec()])
}

fn split_buffer_by_delimiter(buffer: &[u8]) -> Vec<Vec<u8>> {
    let mut result = Vec::new();
    let mut start = 0;
    
    for i in 0..buffer.len() {
        if buffer[i] == b'*' {
            // Check if next byte is a digit
            if i + 1 < buffer.len() && buffer[i + 1].is_ascii_digit() {
                // Found delimiter, add segment if not empty
                if i > start {
                    result.push(buffer[start..i].to_vec());
                }
                start = i; // Start includes the '*' character
            }
        }
    }
    
    // Add remaining segment
    if start < buffer.len() {
        result.push(buffer[start..].to_vec());
    }
    result
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
            master_repl_offset: 0,
            bytes_offset: 0
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
        Err(e) => return None,
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