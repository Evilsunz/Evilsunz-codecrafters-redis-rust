use std::fs;
use crate::{encode_buf_bulk, encode_int, encode_str, encode_string, encode_vec, generate_master_repl_id, ReplicaInstance};
use base64::prelude::*;

const FULLRESYNC: &str = "+FULLRESYNC";

pub fn get_info(header: String, ri : ReplicaInstance) -> Vec<u8>{
        encode_string(ri.get_info())
}

pub fn psync(arg1 : String, arg2 : String, ri : ReplicaInstance) -> Vec<u8>{
    let response = format!("{} {} {}",FULLRESYNC, ri.master_replid , ri.master_repl_offset);
    encode_string(response)
}

pub fn wait(arg1 : &u64, arg2 : &u64) -> Vec<u8>{
    // let response = format!("{} {} {}",FULLRESYNC, ri.master_replid , ri.master_repl_offset);
    encode_int(&(0 as usize))
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