use std::cell::RefCell;
use crate::{encode_error, encode_string, TXContext};
use crate::versions::VERSIONS;

const ERROR_WATCH_INSIDE_MULTI: &str = "ERR WATCH inside MULTI is not allowed";

pub fn watch(arg: String, txcontext: &RefCell<&mut TXContext>) -> Vec<u8> {
    if txcontext.borrow().is_active {
        return encode_error(ERROR_WATCH_INSIDE_MULTI)
    }
    let version = VERSIONS.lock().unwrap().watch(&arg);
    println!("Watched version of {} is {:?}", arg, version);
    if version.is_some() {
        txcontext.borrow_mut().watches.insert(arg, version.unwrap());
    }
    encode_string("OK".to_string())
}