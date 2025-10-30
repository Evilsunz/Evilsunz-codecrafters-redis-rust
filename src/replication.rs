use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::thread;
use crate::{encode_buf_bulk, encode_int, encode_str, encode_string, encode_vec, generate_master_repl_id, get_send_to_replica, ReplicaInstance, REPLICA_STORE, REPLICA_STREAMS};
use base64::prelude::*;
use resp::{encode, Value};
use tokio::sync::watch;

const FULLRESYNC: &str = "+FULLRESYNC";

pub fn get_info(header: String, ri : ReplicaInstance) -> Vec<u8>{
        encode_string(ri.get_info())
}

pub fn psync(arg1 : String, arg2 : String, ri : ReplicaInstance) -> Vec<u8>{
    let response = format!("{} {} {}",FULLRESYNC, ri.master_replid , ri.master_repl_offset);
    encode_string(response)
}

pub fn wait(arg1: &u64, timeout: &u64) -> Vec<u8> {
    let str = encode_vec(vec!("REPLCONF".to_string(), "GETACK".to_string(), "*".to_string()));
    let replies = Arc::new(Mutex::new(0));
    let timeout_millis = *timeout;

    let streams = REPLICA_STREAMS.lock().unwrap().clone();
    if !get_send_to_replica(){
        return encode_int(&streams.len());
    }
    let handles: Vec<_> = streams.iter().map(|stream| {
        let str = str.clone();
        let replies = Arc::clone(&replies);
        let stream: Arc<Mutex<TcpStream>> = Arc::clone(&stream.stream);

        thread::spawn(move || {
            let mut stream = stream.lock().unwrap();
            if let Ok(_) = stream.set_read_timeout(Some(Duration::from_millis(timeout_millis))) {
                if let Ok(_) = stream.write_all(&str) {
                    let mut buffer = [0; 512];
                    if let Ok(_) = stream.read(&mut buffer) {
                        *replies.lock().unwrap() += 1;
                    }
                }
            }
        })
    }).collect();

    thread::sleep(Duration::from_millis(timeout_millis));
    for handle in handles {
        let _ = handle.join();
    }

    let reply_count = *replies.lock().unwrap();
    encode_int(&reply_count)
}

pub fn repl_conf(arg1 : String, arg2 : String, ri : ReplicaInstance) -> Vec<u8> {
    if (arg1.eq("GETACK")){
        return encode_vec(vec!("REPLCONF".to_string(), "ACK".to_string(), ri.bytes_offset.to_string()));
    }
    encode_str("OK")
}

pub fn get_rdb_file() -> Vec<u8>{
    let rdb = fs::read_to_string("src/empty.rdb").unwrap();
    BASE64_STANDARD.decode(rdb).unwrap()
}

#[derive(Clone)]
pub struct ReplicaStream{
    pub stream: Arc<Mutex<TcpStream>>
}

impl ReplicaStream{
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: Arc::new(Mutex::new(stream)),
        }
    }
}