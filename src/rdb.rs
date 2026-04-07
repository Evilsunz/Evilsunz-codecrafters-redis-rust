use crate::{encode_error, encode_vec_as_bulk, RdbSettings};

pub fn get_config(arg2 : String, rdb_settings: RdbSettings) -> Vec<u8>{
    if arg2.eq("dir") {
        encode_vec_as_bulk(vec!("dir".to_string(), rdb_settings.dir))
    } else if arg2.eq("dbfilename") {
        encode_vec_as_bulk(vec!("dbfilename".to_string(), rdb_settings.filename))
    } else {
        encode_error("wrong arg1 or dbfilename")
    }
}