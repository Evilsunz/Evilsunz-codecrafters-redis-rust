use std::cell::RefCell;
use crate::{encode_error, encode_string, TXContext};

const ERROR_WATCH_INSIDE_MULTI: &str = "ERR WATCH inside MULTI is not allowed";

pub fn watch(txcontext: &RefCell<&mut TXContext>) -> Vec<u8> {
    if txcontext.borrow().is_active {
        return encode_error(ERROR_WATCH_INSIDE_MULTI)
    }
    encode_string("OK".to_string())
}