use crate::versions::VERSIONS;
use std::cell::RefCell;
use super::*;
use crate::acl::Auth;
use crate::{decode_to_value, encode_error, encode_vec_of_value, Handler, ReplicaInstance};
use resp::{encode, Value};

const OK: &str = "OK";
const ERROR_EXEC_WITHOUT_MULTI: &str = "ERR EXEC without MULTI";
const ERROR_DISCARD_WITHOUT_MULTI: &str = "ERR DISCARD without MULTI";
const ERROR_WATCH_INSIDE_MULTI: &str = "ERR WATCH inside MULTI is not allowed";

pub fn process(handler: &Handler<'_>) -> Vec<u8> {
    match handler {
        Handler::Multi => encode_str(OK),
        Handler::Queued => encode_str("QUEUED"),
        Handler::Discard(tx_context) => {
            if tx_context.borrow().is_active {
                let mut tx = tx_context.borrow_mut();
                tx.is_active = false;
                tx.store.clear();
                tx.watches.clear();
                encode_str(OK)
            } else {
                encode_error(ERROR_DISCARD_WITHOUT_MULTI)
            }
        }

        Handler::Exec(tx_context) => {
            if !tx_context.borrow().is_active {
                return encode_error(ERROR_EXEC_WITHOUT_MULTI);
            }

            let mut tx = tx_context.borrow_mut();
            tx.is_active = false;

            if has_watch_conflict(&tx.watches) {
                reset_transaction(&mut tx);
                return encode(&Value::NullArray);
            }

            let results: Vec<Value> = tx
                .store
                .iter()
                .cloned()
                .map(run_queued_command)
                .collect();

            reset_transaction(&mut tx);
            encode_vec_of_value(results)
        }

        Handler::Watch(commands, tx_context) => watch(commands, tx_context),
        Handler::Unwatch(tx_context) => unwatch(tx_context),

        _ => encode_str("Command not recognized"),
    }
}


fn has_watch_conflict(watches: &DashMap<String, usize>) -> bool {
    watches.iter().any(|watch| {
        !VERSIONS
            .lock()
            .unwrap()
            .is_version_same(watch.key(), *watch.value())
    })
}

fn reset_transaction(tx: &mut TXContext) {
    tx.store.clear();
    tx.watches.clear();
}

fn run_queued_command(command: Vec<String>) -> Value {
    let output = Handler::from_command(
        command,
        &mut TXContext::default(),
        &mut ReplicaInstance::default(),
        &mut Auth::default(),
        None,
        None
    )
    .process_command();

    decode_to_value(output)
}

pub fn watch(args: &Vec<String>, tx_context: &RefCell<&mut TXContext>) -> Vec<u8> {
    if tx_context.borrow().is_active {
        return encode_error(ERROR_WATCH_INSIDE_MULTI);
    }

    for key in args {
        let version = VERSIONS.lock().unwrap().watch(&key);
        println!("Watched version of {} is {:?}", key, version);
        if version.is_some() {
            tx_context
                .borrow_mut()
                .watches
                .insert(key.to_string(), version.unwrap());
        }
    }
    encode_string("OK".to_string())
}

pub fn unwatch(tx_context: &RefCell<&mut TXContext>) -> Vec<u8> {
    tx_context.borrow_mut().watches.clear();
    encode_string("OK".to_string())
}