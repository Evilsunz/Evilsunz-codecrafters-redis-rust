use crate::{encode_string};

pub fn watch() -> Vec<u8> {
    encode_string("OK".to_string())
}