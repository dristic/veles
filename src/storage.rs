use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, Write},
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

use crate::error::VelesError;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct RowHeader {
    timestamp: u32,
    key_size: u32,
    value_size: u32,
    key_type: i16,
    value_type: i16,
}

const HEADER_SIZE: usize = std::mem::size_of::<RowHeader>();

struct VelesStoreEntry {
    pub file: PathBuf,
    pub total_size: u32,
    pub file_offset: u32,
}

pub struct VelesStore {
    cache: HashMap<String, VelesStoreEntry>,
    write_file: File,
}

// Based on https://riak.com/assets/bitcask-intro.pdf
// Simple key/value storage using a structured log format.
impl VelesStore {
    pub fn new() -> Result<VelesStore, VelesError> {
        let mut cache = HashMap::new();

        fs::create_dir_all(".veles")?;
        let mut file = OpenOptions::new().read(true).open(".veles/veles.db")?;
        let size = file.metadata()?.len();

        let mut pos = 0;
        while pos != size {
            // Get the CRC
            let mut crc_buf = [0; 4];
            file.read_exact(&mut crc_buf)?;
            let crc = u32::from_be_bytes(crc_buf);

            // Decode the header to get the keysize, and valuesize.
            let mut header_bytes = vec![0; HEADER_SIZE];
            file.read_exact(&mut header_bytes)?;
            let header: RowHeader = bincode::deserialize_from(header_bytes.as_slice())?;

            // Read the key and value bytes.
            let mut key_bytes = vec![0; header.key_size as usize];
            let mut value_bytes = vec![0; header.value_size as usize];

            file.read_exact(&mut key_bytes)?;
            file.read_exact(&mut value_bytes)?;

            // Now check the crc.
            let data = [header_bytes, key_bytes.clone(), value_bytes].concat();
            let mut hasher = Hasher::new();
            hasher.update(&data);
            if hasher.finalize() != crc {
                return Err(VelesError::CorruptedData);
            }

            // CRC is good. Store this into our cache.
            let key = String::from_utf8(key_bytes).unwrap();
            let total_size = 4 + HEADER_SIZE as u32 + header.key_size + header.value_size;
            cache.insert(
                key,
                VelesStoreEntry {
                    file: PathBuf::from(".veles/veles.db"),
                    total_size,
                    file_offset: pos as u32,
                },
            );

            // Increment our position.
            pos += total_size as u64;
        }

        let write_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(".veles/veles.db")?;

        Ok(VelesStore { cache, write_file })
    }

    pub fn get(&self, key: &str) -> Result<String, VelesError> {
        if let Some(hint) = self.cache.get(key) {
            let mut file = OpenOptions::new().read(true).open(&hint.file)?;

            let mut data = vec![0; hint.total_size as usize];
            file.seek(std::io::SeekFrom::Start(hint.file_offset.into()))?;
            file.read_exact(&mut data)?;

            // Check the crc.
            let crc = u32::from_be_bytes(data[..4].try_into()?);
            let mut hasher = Hasher::new();
            hasher.update(&data[4..]);
            if hasher.finalize() != crc {
                return Err(VelesError::CorruptedData);
            }

            // Get the value offset and return the value.
            let header: RowHeader = bincode::deserialize_from(&data[4..HEADER_SIZE + 4])?;
            let value_offset = 4 + HEADER_SIZE + header.key_size as usize;
            let value = String::from_utf8(data[value_offset..].to_vec()).unwrap();
            return Ok(value);
        }

        Err(VelesError::NotFound)
    }

    pub fn put(&mut self, key: &str, value: &str) -> Result<(), VelesError> {
        let epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

        let key_bytes = key.as_bytes();
        let value_bytes = value.as_bytes();

        let key_size = key_bytes.len() as u32;
        let value_size = value_bytes.len() as u32;

        let header = RowHeader {
            timestamp: epoch.as_secs() as u32,
            key_size,
            value_size,
            key_type: 1,
            value_type: 1,
        };

        let header_bytes: Vec<u8> = bincode::serialize(&header).unwrap();
        let data = [&header_bytes, key_bytes, value_bytes].concat();

        let mut hasher = Hasher::new();
        hasher.update(&data);
        let crc = hasher.finalize();

        self.write_file.write_all(&crc.to_be_bytes())?;
        self.write_file.write_all(&data)?;

        Ok(())
    }

    pub fn compact(&mut self) -> Result<(), VelesError> {
        self.write_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(".veles/veles_compact.db")?;

        let keys: Vec<String> = self.cache.keys().cloned().collect();
        for key in keys {
            let value = self.get(&key)?;
            self.put(&key, &value)?;
        }

        fs::rename(".veles/veles_compact.db", ".veles/veles.db")?;

        Ok(())
    }
}
