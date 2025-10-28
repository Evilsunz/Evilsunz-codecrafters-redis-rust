use crate::{encode_str, encode_string, generate_master_repl_id, ReplicaInstance};

#[derive(Debug, Clone)]
pub struct ReplicationInfo{
    role: String,
    master_replid: String,
    master_repl_offset: u64
}

impl ReplicationInfo {
    pub fn master() -> ReplicationInfo {
        ReplicationInfo {
            role: String::from("master"),
            master_replid : generate_master_repl_id(),
            master_repl_offset: 0
        }
    }

    pub fn replica() -> ReplicationInfo {
        ReplicationInfo {
            role: String::from("slave"),
            master_replid : String::from("0000000000000000000000000000000000000000"),
            master_repl_offset: 0
        }
    }

    pub fn as_string(&self) -> String {
        format!("role:{} master_replid:{} master_repl_offset:{}", self.role, self.master_replid, self.master_repl_offset)   
    }
    
}

pub fn get_info(header: String, ri : ReplicaInstance) -> Vec<u8>{
    if ri.is_replica {
        encode_string(ReplicationInfo::replica().as_string())
    } else {
        encode_string(ReplicationInfo::master().as_string())
    }
}