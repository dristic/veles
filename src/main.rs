use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::{Read, Write, self, BufRead},
    path::{Path, PathBuf},
    time::SystemTime,
};

use clap::{Parser, Subcommand};
use error::VelesError;
use flate2::{write::GzEncoder, Compression, read::GzDecoder};
use ring::digest;
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
    Submit,
    Status,
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
        Command::Submit => submit(),
    }
    .unwrap()
}

fn submit() -> Result<(), VelesError> {
    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();
    let input = lines.next().unwrap().unwrap();
    println!("Input: {}", input);

    Ok(())
}

fn add(file: PathBuf) -> Result<(), VelesError> {
    if !file.exists() {
        return Err(VelesError::NotFound);
    }

    let mut stage = OpenOptions::new().create(true).write(true).open(".veles/staged.bin")?;
    let content = format!("{}\n", file.as_os_str().to_string_lossy());

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
    let mut new_file = OpenOptions::new().create(true).write(true).open(new_file_path)?;
    new_file.write_all(&compressed)?;

    println!("Wrote: {}", &hex_digest[..40]);

    Ok(())
}

struct DirIterator {
    stack: Vec<PathBuf>,
    pos: usize,
}

impl DirIterator {
    pub fn from_ignorefile<P: AsRef<Path>>(base: P, ignore: P) -> Result<DirIterator, VelesError> {
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

        DirIterator::visit(base.as_ref(), &filter, &mut stack)?;

        Ok(DirIterator { stack, pos: 0 })
    }

    fn visit(path: &Path, filter: &[PathBuf], stack: &mut Vec<PathBuf>) -> Result<(), VelesError> {
        if path.is_dir() {
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let path = entry.path();

                if filter.iter().any(|p| path.starts_with(p)) {
                    continue;
                }

                if path.is_dir() {
                    DirIterator::visit(&path, filter, stack)?;
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

            let iter = DirIterator::from_ignorefile(".", ".velesignore")?;
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

            let iter = DirIterator::from_ignorefile(".", ".velesignore")?;
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
    let iter = DirIterator::from_ignorefile(".", ".velesignore")?;
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
