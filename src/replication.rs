use crate::{encode_str, encode_string};

#[derive(Debug, Clone)]
pub struct ReplicationInfo{
    role: String   
}

impl ReplicationInfo {
    pub fn new() -> ReplicationInfo {
        ReplicationInfo {
            role: String::from("master")
        }
    }

    pub fn as_string(&self) -> String {
        format!("role:{}", self.role)
    }
    
}

pub fn get_info(header: String) -> Vec<u8>{
    encode_string(ReplicationInfo::new().as_string())
}