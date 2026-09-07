use std::sync::{LazyLock};
use bitvec::order::Msb0;
use bitvec::vec::BitVec;
use dashmap::DashMap;
use crate::{encode_error, encode_int};

pub static BITMAP_STORE: LazyLock<BitMapStore> = LazyLock::new(|| BitMapStore::new());

pub struct BitMapStore {
    bits: DashMap<String, BitVec<u8, Msb0>>,
}

impl BitMapStore {
    fn new() -> Self {
        Self {
            bits: DashMap::new(),
        }
    }

    pub fn set_bit(&self, key : &str, offset: &String, value: &String) -> Vec<u8> {
        let offset = match offset.parse::<usize>() {
            Ok(num) => num,
            Err(_) => return encode_error("Value must be a valid number"),
        };
        let value = match value.parse::<u8>() {
            Ok(num) => num,
            Err(_) => return encode_error("Value must be a valid number (0 or 1)"),
        };
        let mut bit_vec = self.bits.entry(key.to_string()).or_insert_with(BitVec::new);
        match value {
            1 => {
                if offset >= bit_vec.len() {
                    bit_vec.resize(offset + 1, false);
                }
                let old_val = if *bit_vec.get(offset).unwrap() { 1 } else { 0 };

                bit_vec.set(offset, true);
                encode_int(&old_val)
            },
            0 => {
                if offset >= bit_vec.len() {
                    bit_vec.resize(offset + 1, false);
                }
                let old_val = if *bit_vec.get(offset).unwrap() { 1 } else { 0 };

                bit_vec.set(offset, false);
                encode_int(&old_val)
            },
            _ => encode_error("ERR bit is not an integer or out of range"),
        }
    }



}