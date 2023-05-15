use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::{Read, Write, BufWriter},
    path::{Path, PathBuf},
    time::SystemTime, ffi::OsString,
};

use clap::{Parser, Subcommand};
use error::VelesError;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use ring::digest;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use storage::VelesStore;

mod error;
mod storage;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Commit {
        #[arg(short, long)]
        file: String,
    },
    Uncommit {
        #[arg(long)]
        hash: String,

        #[arg(short, long)]
        output: Option<String>,
    },
    Index {
        #[command(subcommand)]
        command: IndexCmd,
    },
    Add {
        #[arg(short, long)]
        file: PathBuf,
    },
    Submit {
        #[arg(short, long)]
        message: String,
    },
    Status,
    Log,
    Manifest,
    Storage {
        #[command(subcommand)]
        command: StorageCmd,
    },
}

#[derive(Subcommand)]
enum IndexCmd {
    Create,
    Status,
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
        Command::Commit { file } => commit(file),
        Command::Uncommit { hash, output } => uncommit(hash, output),
        Command::Status => status(),
        Command::Storage { command } => storage(&command),
        Command::Index { command } => index(&command),
        Command::Add { file } => add(file),
        Command::Submit { message } => submit(message),
        Command::Log => log(),
        Command::Manifest => manifest(),
    }
    .unwrap()
}

#[derive(Clone, Debug, PartialEq)]
struct VelesNode {
    name: OsString,
    items: Vec<VelesNode>,
    hash: Option<String>,
}

fn manifest() -> Result<(), VelesError> {
    let mut nodes = HashMap::new();

    let iter = DirIterator::from_ignorefile(".", ".velesignore", true)?;
    for path in iter {
        if path.is_dir() {
            nodes.insert(path.clone(), VelesNode {
                name: path.file_name().unwrap().to_owned(),
                items: Vec::new(),
                hash: None,
            });
        }

        if let Some(parent) = path.parent() {
            let parent = parent.to_owned();
            if !nodes.contains_key(&parent) {
                nodes.insert(parent.clone(), VelesNode {
                    name: parent.file_name().unwrap_or_default().to_owned(),
                    items: Vec::new(),
                    hash: None,
                });
            }

            let hash = if path.is_file() {
                Some(do_commit(path.clone())?)
            } else {
                None
            };

            let node = nodes.get_mut(&parent).unwrap();
            node.items.push(VelesNode {
                name: path.file_name().unwrap().to_owned(),
                items: Vec::new(),
                hash,
            });
        }
    }

    println!("{:?}", nodes);

    let mut hashes: HashMap<OsString, String> = HashMap::new();
    let mut nodes: Vec<VelesNode> = nodes.into_values().collect();
    let mut i = 0;
    while nodes.len() > 0 {
        let node = &nodes[i];

        let ready = node.items.iter().all(|n| n.hash.is_some() || hashes.contains_key(&n.name));
        if ready {
            let mut contents = String::new();

            for item in &node.items {
                let item_hash = item.hash.as_ref().unwrap_or_else(|| hashes.get(&item.name).unwrap());
                contents.push_str(&format!("{} {}\n", item_hash, item.name.to_string_lossy()));
            }

            let mut context = digest::Context::new(&digest::SHA256);
            context.update(contents.as_bytes());
            let digest = context.finish();
            let hex_digest = hex::encode(&digest);

            hashes.insert(node.name.clone(), hex_digest[..40].to_owned());

            println!("Adding tree {}:\n{}", hex_digest[..40].to_owned(), contents);

            nodes.remove(i);

            if i == nodes.len() {
                i = 0;
            }
        } else {
            i += 1;

            if i == nodes.len() {
                i = 0;
            }
        }
    }

    Ok(())
}

struct VelesChange {
    id: u32,
    description: String,
}

struct VelesFile {
    filename: String,
    revision: u32,
}

fn log() -> Result<(), VelesError> {
    let conn = Connection::open(".veles/veles.db3")?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS files (
            filename TEXT PRIMARY KEY,
            revision INTEGER NOT NULL
        )",
        (),
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS changes (
            id INTEGER PRIMARY KEY,
            description TEXT NOT NULL
        )",
        (),
    )?;

    conn.execute(
        "INSERT OR IGNORE INTO files (filename, revision) VALUES (?1, ?2)",
        ("src/error.rs", 1),
    )?;

    conn.execute(
        "INSERT OR IGNORE INTO files (filename, revision) VALUES (?1, ?2)",
        ("src/main.rs", 2),
    )?;

    conn.execute(
        "INSERT OR IGNORE INTO files (filename, revision) VALUES (?1, ?2)",
        ("src/storage.rs", 2),
    )?;

    conn.execute(
        "INSERT OR IGNORE INTO changes (id, description) VALUES (?1, ?2)",
        (1, "Initial commit."),
    )?;

    conn.execute(
        "INSERT OR IGNORE INTO changes (id, description) VALUES (?1, ?2)",
        (2, "Testing."),
    )?;

    let mut statement = conn.prepare("SELECT id, description FROM changes")?;
    let change_iter = statement.query_map([], |row| {
        Ok(VelesChange {
            id: row.get(0)?,
            description: row.get(1)?,
        })
    })?;

    for change in change_iter {
        let change = change?;
        println!("Change: {} - {}", change.id, change.description);

        let mut statement = conn.prepare("SELECT filename FROM files WHERE revision = ?1")?;
        let file_iter = statement.query_map([change.id], |row| {
            Ok(VelesFile {
                filename: row.get(0)?,
                revision: change.id,
            })
        })?;

        for file in file_iter {
            let file = file?;
            println!("  File: {} {}", file.filename, file.revision);
        }

        println!("====================");
    }

    Ok(())
}

fn submit(message: String) -> Result<(), VelesError> {
    let stage_path = PathBuf::from(".veles/staged.bin");
    if !stage_path.exists() {
        return Err(VelesError::NotFound);
    }

    let hash = do_commit(stage_path)?;

    let mut commits = OpenOptions::new()
        .create(true)
        .write(true)
        .open(".veles/commits.bin")?;
    let line = format!("{}\n{}\n{}\n", "1", message, hash);

    commits.write_all(line.as_bytes())?;

    Ok(())
}

fn add(file: PathBuf) -> Result<(), VelesError> {
    if !file.exists() {
        return Err(VelesError::NotFound);
    }

    let hash = do_commit(file.clone())?;

    let mut stage = OpenOptions::new()
        .create(true)
        .write(true)
        .open(".veles/staged.bin")?;
    let content = format!("{} {}\n", file.as_os_str().to_string_lossy(), hash);

    stage.write_all(content.as_bytes())?;

    Ok(())
}

fn uncommit(hash: String, output: Option<String>) -> Result<(), VelesError> {
    let path = PathBuf::from(".veles/").join(&hash[..2]);
    let file_path = path.join(&hash[2..]);
    let file = OpenOptions::new().read(true).open(file_path)?;
    let mut decoder = GzDecoder::new(file);

    let mut buf = Vec::new();
    decoder.read_to_end(&mut buf)?;

    if let Some(out) = output {
        let mut new_file = OpenOptions::new().create(true).write(true).open(out)?;
        new_file.write_all(&buf)?;
    } else {
        if let Ok(str) = String::from_utf8(buf) {
            println!("{}", str);
        } else {
            println!("Failed to parse data as utf-8");
        }
    }

    Ok(())
}

fn commit(file: String) -> Result<(), VelesError> {
    let hash = do_commit(PathBuf::from(file))?;

    println!("Wrote {}", hash);

    Ok(())
}

fn do_commit(file: PathBuf) -> Result<String, VelesError> {
    let mut file = OpenOptions::new().read(true).open(file)?;

    let mut buffer = [0; 1024];
    let mut context = digest::Context::new(&digest::SHA256);
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());

    loop {
        let read = file.read(&mut buffer)?;

        if read == 0 {
            break;
        }

        context.update(&buffer[..read]);
        encoder.write_all(&buffer[..read])?;
    }

    let digest = context.finish();
    let hex_digest = hex::encode(&digest);
    let compressed = encoder.finish()?;

    let path = PathBuf::from(".veles/").join(&hex_digest[..2]);
    fs::create_dir_all(&path)?;

    let new_file_path = path.join(&hex_digest[2..40]);
    if !new_file_path.exists() {
        let mut new_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(new_file_path)?;
        new_file.write_all(&compressed)?;
    }

    Ok(hex_digest[..40].to_string())
}

struct DirIterator {
    stack: Vec<PathBuf>,
    pos: usize,
}

impl DirIterator {
    pub fn from_ignorefile<P: AsRef<Path>>(base: P, ignore: P, include_dirs: bool) -> Result<DirIterator, VelesError> {
        let mut stack = Vec::new();
        let base_path = base.as_ref().to_path_buf();
        let ignore = ignore.as_ref().to_path_buf();

        let filter: Vec<PathBuf> = if ignore.exists() {
            let ignore_data = fs::read_to_string(ignore)?;
            ignore_data
                .lines()
                .map(|line| base_path.join(line))
                .collect()
        } else {
            Vec::new()
        };

        DirIterator::visit(base.as_ref(), &filter, &mut stack, include_dirs)?;

        Ok(DirIterator { stack, pos: 0 })
    }

    fn visit(path: &Path, filter: &[PathBuf], stack: &mut Vec<PathBuf>, include_dirs: bool) -> Result<(), VelesError> {
        if path.is_dir() {
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let path = entry.path();

                if filter.iter().any(|p| path.starts_with(p)) {
                    continue;
                }

                if path.is_dir() {
                    if include_dirs {
                        stack.push(path.to_path_buf());
                    }
                    DirIterator::visit(&path, filter, stack, include_dirs)?;
                } else {
                    stack.push(path.to_path_buf());
                }
            }
        }

        Ok(())
    }
}

impl Iterator for DirIterator {
    type Item = PathBuf;

    fn next<'a>(&mut self) -> Option<Self::Item> {
        let pos = self.pos;
        self.pos += 1;
        self.stack.get(pos).cloned()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct VelesIndex {
    timestamp: SystemTime,
    index: HashMap<PathBuf, VelesIndexMeta>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct VelesIndexMeta {
    created: SystemTime,
    modified: SystemTime,
}

fn index(command: &IndexCmd) -> Result<(), VelesError> {
    match command {
        IndexCmd::Create => {
            let timestamp = SystemTime::now();
            let mut index = HashMap::new();

            let iter = DirIterator::from_ignorefile(".", ".velesignore", false)?;
            for path in iter {
                let metadata = fs::metadata(&path)?;

                index.insert(
                    path.clone(),
                    VelesIndexMeta {
                        created: metadata.created()?,
                        modified: metadata.modified()?,
                    },
                );

                println!("{:?}", metadata);
            }

            let index = VelesIndex { timestamp, index };
            let data = bincode::serialize(&index)?;
            fs::write(".veles/index", data)?;
        }
        IndexCmd::Status => {
            let mut file = OpenOptions::new().read(true).open(".veles/index")?;
            let mut data = Vec::new();
            file.read_to_end(&mut data)?;

            let index: VelesIndex = bincode::deserialize(&data)?;

            let iter = DirIterator::from_ignorefile(".", ".velesignore", false)?;
            for path in iter {
                let metadata = fs::metadata(&path)?;
                if let Some(indexed) = index.index.get(&path) {
                    if indexed.modified == metadata.modified()? {
                        println!("Unchanged: {:?}", path);
                    } else {
                        println!("Modified: {:?}", path);
                    }
                } else {
                    println!("New: {:?}", path);
                }
            }
        }
    }

    Ok(())
}

fn status() -> Result<(), VelesError> {
    let iter = DirIterator::from_ignorefile(".", ".velesignore", false)?;
    for path in iter {
        println!("{:?}", path);
    }

    Ok(())
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
        }
        StorageCmd::Compact => {
            veles.compact()?;
        }
    }

    Ok(())
}
