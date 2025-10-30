use crate::{encode_bulk_string, encode_error, encode_vec, RdbSettings};

pub fn get_config(arg1: String, arg2 : String, rdb_settings: RdbSettings) -> Vec<u8>{
    if arg2.eq("dir") {
        encode_vec(vec!("dir".to_string(), rdb_settings.dir))
    } else if arg2.eq("dbfilename") {
        encode_vec(vec!("dbfilename".to_string(), rdb_settings.filename))
    } else {
        encode_error("wrong arg1 or dbfilename")
    }
}