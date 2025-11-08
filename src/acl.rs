use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use resp::Value;
use sha2::{Digest, Sha256};
use crate::{encode_bulk_str, encode_error, encode_null, encode_string, encode_vec, encode_vec_as_bulk, encode_vec_of_value};

const FLAGS: &str = "flags";
const NOPASS: &str = "nopass";
const PASSWORDS: &str = "passwords";
const USER_PASS_INCORRECT: &str = "WRONGPASS invalid username-password pair or user is disabled.";
const NON_AUTH: &str = "NOAUTH Authentication required.";
const DEFAULT: &'static str = "default";


#[derive(Debug, Clone)]
pub struct Auth {
    pub authenticated: bool,
    pub username : String,
    pub passwords : Vec<String>,
    pub flags: Vec<String>,
}

impl Default for Auth {
    fn default() -> Self {
        fn create_auth_flags(username: &str) -> Vec<String> {
            let mut flags = Vec::new();
            if !AUTH_STORE.is_user_passworded(username) {
                flags.push("nopass".to_string());
            }
            flags
        }

        Auth {
            authenticated: false,
            username: "default".to_string(),
            passwords: vec!(),
            flags: create_auth_flags("default"),
        }
    }
}

impl Auth {
    pub fn is_nopass(&self) -> bool {
        self.flags.contains(&"nopass".to_string())
    }
}

pub static AUTH_STORE: LazyLock<AuthStore> = LazyLock::new(|| AuthStore::new());

pub struct AuthStore {
    store: Mutex<HashMap<String, String>>,
}

impl AuthStore {
        fn new() -> Self {
            Self {
                store: Mutex::new(HashMap::new()),
            } 
        }

    pub fn whoami(&self, auth : Auth) -> Vec<u8> {
        if !auth.authenticated && !auth.flags.contains(&NOPASS.to_string()) {
            return encode_error(NON_AUTH);
        }
        encode_bulk_str(DEFAULT)
    }

    pub fn get_user(&self, auth : Auth) -> Vec<u8> {
        let user_info = vec![Value::Bulk(FLAGS.to_string()),
                             Value::Array(auth.flags.iter().map(|s|Value::Bulk(s.to_string())).collect::<Vec<Value>>()),
                             Value::Bulk(PASSWORDS.to_string()),
                             Value::Array(auth.passwords.iter().map(|s|Value::Bulk(s.to_string())).collect::<Vec<Value>>()),
        ];
        encode_vec_of_value(user_info)
    }

    pub fn set_user(&self, auth : &RefCell<&mut Auth>, username : &str, password : &str) -> Vec<u8> {
        let mut borrow = auth.borrow_mut();
        let mut store = self.store.lock().unwrap();
        let hash = self.to_sha(password);
        store.insert(username.to_string(), hash.clone());
        borrow.flags.retain(|s| s != NOPASS);
        borrow.passwords.push(hash);
        borrow.authenticated = true;
        encode_string("OK".to_string())
    }

    pub fn auth(&self, auth : &RefCell<&mut Auth>, username : &str, password : &str) -> Vec<u8> {
        let store = self.store.lock().unwrap();
        println!(" +++++++ passwd: {}", password);
        println!(" +++++++ username: {}", username);
        let passwd = match store.get(username) {
            Some(pwd) => pwd.to_string(),
            None => return encode_error(USER_PASS_INCORRECT),
        };

        if self.to_sha(password) != passwd {
            return encode_error(USER_PASS_INCORRECT);
        }
        auth.borrow_mut().authenticated = true;
        encode_string("OK".to_string())
    }

    pub fn is_user_passworded(&self, username : &str) -> bool {
        let mut store = self.store.lock().unwrap();
        store.contains_key(username)
    }

    fn to_sha(&self, password : &str) -> String{
        format!("{:x}", Sha256::digest(password.as_bytes()))
    }

}