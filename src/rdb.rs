use crate::{encode_error, encode_vec_as_bulk, AOFSettings, RdbSettings};

pub fn get_config(arg2 : String, rdb_settings: Option<RdbSettings>, aof_settings: Option<AOFSettings>) -> Vec<u8>{
    if let Some(rdb_settings) = rdb_settings {
        if arg2.eq("dir") {
            encode_vec_as_bulk(vec!("dir".to_string(), rdb_settings.dir))
        } else if arg2.eq("dbfilename") {
            encode_vec_as_bulk(vec!("dbfilename".to_string(), rdb_settings.filename))
        } else {
            encode_error("wrong arg1 or dbfilename")
        }
    } else if let Some(aof_settings) = aof_settings {
        if arg2.eq("dir") {
            encode_vec_as_bulk(vec!("dir".to_string(), aof_settings.dir))
        } else if arg2.eq("appendonly") {
            encode_vec_as_bulk(vec!("appendonly".to_string(), aof_settings.append_only))
        } else if arg2.eq("appenddirname") {
            encode_vec_as_bulk(vec!("appenddirname".to_string(), aof_settings.append_dir))
        } else if arg2.eq("appendfilename") {
            encode_vec_as_bulk(vec!("appendfilename".to_string(), aof_settings.append_filename))
        } else if arg2.eq("appendfsync") {
            encode_vec_as_bulk(vec!("appendfsync".to_string(), aof_settings.append_fsync))
        } else {
            encode_error("wrong args")
        }
    } else {
        encode_error("wrong args")
    }
}