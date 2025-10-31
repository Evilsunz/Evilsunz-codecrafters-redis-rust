use std::io::{Read, Result as IoResult, Error, ErrorKind, BufReader};
use std::collections::HashMap;
use std::fs;
use std::fs::DirEntry;
use std::path::Path;
use crate::RdbSettings;

/// RDB file parser for Redis database files
pub struct RdbParser<R: Read> {
    reader: R,
}

impl<R: Read> RdbParser<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    pub fn parse(&mut self) -> IoResult<RdbFile> {
        let header = self.read_header()?;
        let mut databases = Vec::new();
        let mut aux_fields = HashMap::new();
        let mut current_db_number = 0;
        let mut current_db = Database { entries: HashMap::new() };

        loop {
            let op_code = self.read_byte()?;
            match op_code {
                0xFF => {
                    // End of file - save current database if it has entries
                    if !current_db.entries.is_empty() {
                        databases.push((current_db_number, current_db));
                    }
                    let _checksum = self.read_checksum()?;
                    break;
                }
                0xFE => {
                    // Database selector - save previous database if it has entries
                    if !current_db.entries.is_empty() {
                        databases.push((current_db_number, current_db));
                    }
                    current_db_number = self.read_length()?;
                    current_db = Database { entries: HashMap::new() };
                }
                0xFD => {
                    // Expire time in seconds
                    let _expire_seconds = self.read_u32()?;
                    let (key, value) = self.read_key_value_pair()?;
                    current_db.entries.insert(key, value);
                }
                0xFC => {
                    // Expire time in milliseconds
                    let _expire_ms = self.read_u64()?;
                    let (key, value) = self.read_key_value_pair()?;
                    current_db.entries.insert(key, value);
                }
                0xFA => {
                    // Auxiliary field
                    let key = self.read_string()?;
                    let value = self.read_string()?;
                    aux_fields.insert(key, value);
                }
                0xFB => {
                    // Resize DB
                    let _db_size = self.read_length()?;
                    let _expires_size = self.read_length()?;
                    // Continue reading key-value pairs
                }
                _ => {
                    // Value type (0-4), read as key-value pair
                    let key = self.read_string()?;
                    let value = self.read_value(op_code)?;
                    current_db.entries.insert(key, value);
                }
            }
        }

        Ok(RdbFile {
            version: header.version,
            databases,
            aux_fields,
        })
    }

    fn read_header(&mut self) -> IoResult<RdbHeader> {
        let mut magic = [0u8; 5];
        self.reader.read_exact(&mut magic)?;

        if &magic != b"REDIS" {
            return Err(Error::new(ErrorKind::InvalidData, "Invalid RDB magic string"));
        }

        let mut version = [0u8; 4];
        self.reader.read_exact(&mut version)?;

        let version_str = String::from_utf8_lossy(&version).to_string();
        let version_num = version_str.parse::<u32>()
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid version"))?;

        Ok(RdbHeader {
            magic: String::from_utf8_lossy(&magic).to_string(),
            version: version_num,
        })
    }

    fn read_database(&mut self) -> IoResult<Database> {
        let mut entries = HashMap::new();

        loop {
            let byte = self.peek_byte()?;

            // Check for special opcodes that end the database section
            if byte == 0xFF || byte == 0xFE || byte == 0xFA || byte == 0xFB {
                break;
            }

            let (key, value) = self.read_key_value_pair()?;
            entries.insert(key, value);
        }

        Ok(Database { entries })
    }

    fn read_key_value_pair(&mut self) -> IoResult<(String, RedisValue)> {
        let value_type = self.read_byte()?;
        let key = self.read_string()?;
        let value = self.read_value(value_type)?;

        Ok((key, value))
    }

    fn read_value(&mut self, value_type: u8) -> IoResult<RedisValue> {
        match value_type {
            0 => {
                // String
                let s = self.read_string()?;
                Ok(RedisValue::String(s))
            }
            1 => {
                // List
                let len = self.read_length()?;
                let mut items = Vec::new();
                for _ in 0..len {
                    items.push(self.read_string()?);
                }
                Ok(RedisValue::List(items))
            }
            2 => {
                // Set
                let len = self.read_length()?;
                let mut items = Vec::new();
                for _ in 0..len {
                    items.push(self.read_string()?);
                }
                Ok(RedisValue::Set(items))
            }
            3 => {
                // Sorted Set (ZSet)
                let len = self.read_length()?;
                let mut items = Vec::new();
                for _ in 0..len {
                    let member = self.read_string()?;
                    let score = self.read_double()?;
                    items.push((member, score));
                }
                Ok(RedisValue::ZSet(items))
            }
            4 => {
                // Hash
                let len = self.read_length()?;
                let mut map = HashMap::new();
                for _ in 0..len {
                    let field = self.read_string()?;
                    let value = self.read_string()?;
                    map.insert(field, value);
                }
                Ok(RedisValue::Hash(map))
            }
            _ => Err(Error::new(ErrorKind::InvalidData, format!("Unknown value type: {}", value_type)))
        }
    }

    /// Read a length-encoded value
    fn read_length(&mut self) -> IoResult<usize> {
        let first_byte = self.read_byte()?;
        let encoding_type = (first_byte & 0xC0) >> 6;

        match encoding_type {
            0 => {
                // 6-bit length
                Ok((first_byte & 0x3F) as usize)
            }
            1 => {
                // 14-bit length
                let second_byte = self.read_byte()?;
                Ok((((first_byte & 0x3F) as usize) << 8) | (second_byte as usize))
            }
            2 => {
                // 32-bit length
                self.read_u32().map(|v| v as usize)
            }
            3 => {
                // Special encoding
                let special_type = first_byte & 0x3F;
                match special_type {
                    0 => Ok(1), // 8-bit integer
                    1 => Ok(2), // 16-bit integer
                    2 => Ok(4), // 32-bit integer
                    3 => {
                        // Compressed string - read compressed length
                        let compressed_len = self.read_length()?;
                        Ok(compressed_len)
                    }
                    _ => Err(Error::new(ErrorKind::InvalidData, "Unknown special encoding"))
                }
            }
            _ => unreachable!()
        }
    }

    fn read_string(&mut self) -> IoResult<String> {
        let len = self.read_length()?;
        let mut buffer = vec![0u8; len];
        self.reader.read_exact(&mut buffer)?;

        Ok(String::from_utf8_lossy(&buffer).to_string())
    }

    fn read_double(&mut self) -> IoResult<f64> {
        let len = self.read_byte()?;
        match len {
            253 => Ok(f64::NAN),
            254 => Ok(f64::INFINITY),
            255 => Ok(f64::NEG_INFINITY),
            _ => {
                let mut buffer = vec![0u8; len as usize];
                self.reader.read_exact(&mut buffer)?;
                let s = String::from_utf8_lossy(&buffer);
                s.parse::<f64>()
                    .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid double"))
            }
        }
    }

    /// Read checksum (8 bytes)
    fn read_checksum(&mut self) -> IoResult<u64> {
        self.read_u64()
    }

    /// Helper: read a single byte
    fn read_byte(&mut self) -> IoResult<u8> {
        let mut buf = [0u8; 1];
        self.reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    /// Helper: peek at the next byte without consuming it
    fn peek_byte(&mut self) -> IoResult<u8> {
        // This is a simplified version - in practice you'd need buffering
        let byte = self.read_byte()?;
        // Note: This doesn't actually "put back" the byte - you'd need a BufferedReader
        Ok(byte)
    }

    /// Helper: read u32 (little-endian)
    fn read_u32(&mut self) -> IoResult<u32> {
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    /// Helper: read u64 (little-endian)
    fn read_u64(&mut self) -> IoResult<u64> {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }
}

#[derive(Debug)]
pub struct RdbFile {
    pub version: u32,
    pub databases: Vec<(usize, Database)>,
    pub aux_fields: HashMap<String, String>,
}

#[derive(Debug)]
pub struct RdbHeader {
    pub magic: String,
    pub version: u32,
}

#[derive(Debug)]
pub struct Database {
    pub entries: HashMap<String, RedisValue>,
}

#[derive(Debug, Clone)]
pub enum RedisValue {
    String(String),
    List(Vec<String>),
    Set(Vec<String>),
    ZSet(Vec<(String, f64)>),
    Hash(HashMap<String, String>),
}

impl RedisValue {
    pub fn as_string(&self) -> Option<String> {
        match self {
            RedisValue::String(s) => Some(s.to_string()),
            _ => None
        }
    }

    pub fn as_list(&self) -> Option<&Vec<String>> {
        match self {
            RedisValue::List(l) => Some(l),
            _ => None
        }
    }

    pub fn as_set(&self) -> Option<&Vec<String>> {
        match self {
            RedisValue::Set(s) => Some(s),
            _ => None
        }
    }

    pub fn as_zset(&self) -> Option<&Vec<(String, f64)>> {
        match self {
            RedisValue::ZSet(z) => Some(z),
            _ => None
        }
    }

    pub fn as_hash(&self) -> Option<&HashMap<String, String>> {
        match self {
            RedisValue::Hash(h) => Some(h),
            _ => None
        }
    }
}

pub fn parse_rdb_by_config(rdb_settings : &RdbSettings) -> IoResult<RdbFile> {
    let path = format!("{}/{}", rdb_settings.dir, rdb_settings.filename);
    parse_rdb_file(&path)
}

pub fn parse_rdb_file(path: &str) -> IoResult<RdbFile> {
    let file = std::fs::File::open(path)?;
    let mut parser = RdbParser::new(file);
    parser.parse()
}