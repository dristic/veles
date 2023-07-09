use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::Read,
    path::PathBuf,
    time::SystemTime,
};

use log::info;
use serde::{Deserialize, Serialize};

use crate::{error::VelesError, DirIterator};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct VelesIndex {
    timestamp: SystemTime,
    index: HashMap<PathBuf, VelesIndexMeta>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct VelesIndexMeta {
    created: SystemTime,
    modified: SystemTime,
    state: IndexState,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
enum IndexState {
    Untracked,
    Tracked,
    Added,
}

impl VelesIndex {
    pub fn load() -> Result<VelesIndex, VelesError> {
        info!("Loading index at .veles/index");

        let index_path = PathBuf::from(".veles/index");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(index_path)?;

        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let mut index: VelesIndex = if data.is_empty() {
            VelesIndex {
                timestamp: SystemTime::now(),
                index: HashMap::new(),
            }
        } else {
            bincode::deserialize(&data)?
        };

        let iter = DirIterator::from_ignorefile(".", ".velesignore", false)?;
        for path in iter {
            let metadata = fs::metadata(&path)?;

            // if let Some(indexed) = index.index.get(&path) {
            //     if indexed.modified == metadata.modified()? {
            //         println!("Unchanged: {:?}", path);
            //     } else {
            //         println!("Modified: {:?}", path);
            //     }
            // } else {
            //     println!("New: {:?}", path);
            // }

            if !index.index.contains_key(&path) {
                index.index.insert(
                    path.clone(),
                    VelesIndexMeta {
                        created: metadata.created()?,
                        modified: metadata.modified()?,
                        state: IndexState::Untracked,
                    },
                );
            }
        }

        Ok(index)
    }

    pub fn save(&self) -> Result<(), VelesError> {
        let data = bincode::serialize(&self)?;
        fs::write(".veles/index", data)?;

        Ok(())
    }
}

pub struct VelesClient {}

impl VelesClient {
    pub fn init() -> Result<(), VelesError> {
        let path = PathBuf::from(".veles");
        Ok(fs::create_dir_all(path)?)
    }

    pub fn new() -> Result<VelesClient, VelesError> {
        let repo_path = PathBuf::from(".veles");

        if !repo_path.exists() {
            return Err(VelesError::NotInitialized);
        }

        Ok(VelesClient {})
    }

    pub fn status(&self) -> Result<(), VelesError> {
        let _ = VelesIndex::load()?;

        println!("Modified files not in a changeset:");

        println!();

        println!("Files not added to the repository:");
        let iter = DirIterator::from_ignorefile(".", ".velesignore", false)?;
        for path in iter {
            println!("  {:?}", path);
        }

        Ok(())
    }

    pub fn add(&self, file: PathBuf) -> Result<(), VelesError> {
        let mut index = VelesIndex::load()?;

        if !file.exists() {
            return Err(VelesError::NotFound);
        }

        if let Some(indexed) = index.index.get_mut(&file) {
            indexed.state = IndexState::Added;
        }

        index.save()?;

        Ok(())
    }

    pub fn changes(&self) -> Result<(), VelesError> {
        let index = VelesIndex::load()?;

        let iter = DirIterator::from_ignorefile(".", ".velesignore", false)?;
        for path in iter {
            if let Some(indexed) = index.index.get(&path) {
                match indexed.state {
                    IndexState::Untracked => println!("\t(U) {:?}", path),
                    IndexState::Tracked => println!("\t(T) {:?}", path),
                    IndexState::Added => println!("\t(A) {:?}", path),
                }
            }
        }

        index.save()?;

        Ok(())
    }

    pub fn changesets(&self) -> Result<(), VelesError> {
        println!("Pending changesets:");

        Ok(())
    }
}
