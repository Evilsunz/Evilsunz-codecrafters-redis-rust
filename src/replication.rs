use crate::{encode_str, encode_string, ReplicaInstance};

#[derive(Debug, Clone)]
pub struct ReplicationInfo{
    role: String   
}

impl ReplicationInfo {
    pub fn master() -> ReplicationInfo {
        ReplicationInfo {
            role: String::from("master")
        }
    }

    pub fn replica() -> ReplicationInfo {
        ReplicationInfo {
            role: String::from("slave")
        }
    }

    pub fn as_string(&self) -> String {
        format!("role:{}", self.role)
    }
    
}

pub fn get_info(header: String, ri : ReplicaInstance) -> Vec<u8>{
    if ri.is_replica {
        encode_string(ReplicationInfo::replica().as_string())
    } else {
        encode_string(ReplicationInfo::master().as_string())
    }
}