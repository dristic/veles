use std::{
    array::TryFromSliceError,
    collections::HashMap,
    fs::{self, OpenOptions, File},
    io::{Read, Seek, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    path::{PathBuf, Path},
    time::{SystemTime, UNIX_EPOCH},
};

use clap::{Parser, Subcommand};

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
enum VelesError {
    #[error("IO error")]
    IOError(#[from] std::io::Error),

    #[error("Serialization error")]
    SerializationError(#[from] bincode::Error),

    #[error("Try from slice error")]
    SliceError(#[from] TryFromSliceError),

    #[error("CRC check failed")]
    CorruptedData,

    #[error("Data not found")]
    NotFound,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Client,
    Server,
    Status,
    Storage {
        #[command(subcommand)]
        command: StorageCmd,
    },
}

#[derive(Subcommand)]
enum StorageCmd {
    Get {
        #[arg(short, long)]
        key: String,
    },
    Put {
        #[arg(short, long)]
        key: String,

        #[arg(short, long)]
        value: String,
    },
    Compact,
}

fn main() {
    let args = Args::parse();

    match args.command {
        Command::Client => client(),
        Command::Server => server(),
        Command::Status => status(),
        Command::Storage { command } => storage(&command),
    }
    .unwrap()
}

fn visit_dirs(dir: &std::path::Path, filter: &[PathBuf]) {
    if dir.is_dir() {
        for entry in fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();

            if filter.iter().any(|p| path.starts_with(p)) {
                continue;
            }

            if path.is_dir() {
                visit_dirs(&path, filter);
            } else {
                println!("{:?}", path);
            }
        }
    }
}

fn status() -> Result<(), VelesError> {
    let path = PathBuf::from(".");
    let ignore_path = Path::new(".velesignore");

    let filter: Vec<PathBuf> = if ignore_path.exists() {
        let ignore_data = fs::read_to_string(ignore_path)?;
        ignore_data.lines().map(|line| path.join(line)).collect()
    } else {
        Vec::new()
    };

    visit_dirs(&path, &filter);

    Ok(())
}

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

struct VelesStore {
    cache: HashMap<String, VelesStoreEntry>,
    write_file: File,
}

// Based on https://riak.com/assets/bitcask-intro.pdf
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
            let value = String::from_utf8((&data[value_offset..]).to_vec()).unwrap();
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

        self.write_file.write(&crc.to_be_bytes())?;
        self.write_file.write(&data)?;

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

fn storage(command: &StorageCmd) -> Result<(), VelesError> {
    let mut veles = VelesStore::new()?;

    match command {
        StorageCmd::Get { key } => {
            let value = veles.get(&key)?;
            println!("{}: {}", key, value);
        }
        StorageCmd::Put { key, value } => {
            veles.put(key, value)?;
            println!("Put {}: {}", key, value);
        },
        StorageCmd::Compact => {
            veles.compact()?;
        }
    }

    Ok(())
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Entity {
    x: f32,
    y: f32,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct World(Vec<Entity>);

#[derive(Serialize, Deserialize, PartialEq, Debug)]
enum Message {
    Handshake,
    Heartbeat,
}

// TODO
// - handshake
// - storage of connections
// - reconnect handshake
//

fn client() -> Result<(), VelesError> {
    let world = World(vec![Entity { x: 0.0, y: 4.0 }, Entity { x: 10.0, y: 20.5 }]);

    let encoded: Vec<u8> = bincode::serialize(&world).unwrap();

    let socket = UdpSocket::bind("127.0.0.1:34254")?;

    let src = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    socket.send_to(&encoded, &src)?;

    let mut buf = [0; 200];
    let (_amt, _src) = socket.recv_from(&mut buf)?;

    let decoded: World = bincode::deserialize(&buf[..]).unwrap();

    println!("Got {:?}", decoded);

    Ok(())
}

fn server() -> Result<(), VelesError> {
    let socket = UdpSocket::bind("127.0.0.1:8080")?;

    // Receives a single datagram message on the socket. If `buf` is too small to hold
    // the message, it will be cut off.
    let mut buf = [0; 200];
    let (_amt, src) = socket.recv_from(&mut buf)?;

    let mut decoded: World = bincode::deserialize(&buf[..]).unwrap();

    println!("Got {:?}", decoded);

    decoded.0[0].x = 200.0;

    let encoded: Vec<u8> = bincode::serialize(&decoded).unwrap();

    println!("Sending {:?}", encoded);
    socket.send_to(&encoded, &src)?;

    Ok(())
}
