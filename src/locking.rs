use std::cell::RefCell;
use crate::{encode_error, encode_string, TXContext};
use crate::versions::VERSIONS;

const ERROR_WATCH_INSIDE_MULTI: &str = "ERR WATCH inside MULTI is not allowed";

pub fn watch(args: &Vec<String>, txcontext: &RefCell<&mut TXContext>) -> Vec<u8> {
    if txcontext.borrow().is_active {
        return encode_error(ERROR_WATCH_INSIDE_MULTI)
    }

    for key in args {
        let version = VERSIONS.lock().unwrap().watch(&key);
        println!("Watched version of {} is {:?}", key, version);
        if version.is_some() {
            txcontext.borrow_mut().watches.insert(key.to_string(), version.unwrap());
        }
    }
    encode_string("OK".to_string())
}

pub fn unwatch(txcontext: &RefCell<&mut TXContext>) -> Vec<u8> {
    txcontext.borrow_mut().watches.clear();
    encode_string("OK".to_string())
}