use resp::Value;
use crate::{encode_null, encode_vec_of_value};

const FLAGS: &str = "flags";
const NOPASS: &str = "nopass";
const PASSWORDS: &str = "passwords";

pub fn get_user() -> Vec<u8> {
    let user_info = vec![Value::Bulk(FLAGS.to_string()),
                                     Value::Array(vec![Value::Bulk(NOPASS.to_string())]),
                                     Value::Bulk(PASSWORDS.to_string()),
                                     Value::Array(vec![]),
                                    ];
    encode_vec_of_value(user_info)
}