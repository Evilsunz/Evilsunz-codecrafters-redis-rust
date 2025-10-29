use std::fs;
use crate::{encode_buf_bulk, encode_str, encode_string, generate_master_repl_id, ReplicaInstance};
use base64::prelude::*;

const FULLRESYNC: &str = "+FULLRESYNC";

pub fn get_info(header: String, ri : ReplicaInstance) -> Vec<u8>{
        encode_string(ri.get_info())
}

pub fn psync(arg1 : String, arg2 : String, ri : ReplicaInstance) -> Vec<u8>{
    let response = format!("{} {} {}",FULLRESYNC, ri.master_replid , ri.master_repl_offset);
    encode_string(response)
}

pub fn get_rdb_file() -> Vec<u8>{
    let rdb = fs::read_to_string("src/empty.rdb").unwrap();
    BASE64_STANDARD.decode(rdb).unwrap()
    //encode_buf_bulk(decoded)
}