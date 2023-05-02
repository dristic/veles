use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    time::SystemTime,
};

use clap::{Parser, Subcommand};
use error::VelesError;
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
    Index {
        #[command(subcommand)]
        command: IndexCmd,
    },
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
        Command::Status => status(),
        Command::Storage { command } => storage(&command),
        Command::Index { command } => index(&command),
    }
    .unwrap()
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
