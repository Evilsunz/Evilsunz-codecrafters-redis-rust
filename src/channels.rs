use resp::Value;
use crate::{encode_error, encode_int, encode_vec, encode_vec_of_value};

pub fn subscribe(key: String) -> Vec<u8> {
    let mut vector:Vec<Value> = vec!(Value::String("subscribe".to_string()));
    vector.push(Value::String(key));
    vector.push(Value::Integer(1));
    encode_vec_of_value(vector)
}