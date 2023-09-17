use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::Read,
    path::{PathBuf},
    time::SystemTime,
};

use log::info;

use serde::{Deserialize, Serialize};

use crate::{
    config::VelesConfig, error::VelesError, protocol::LocalTransport, Changeset, DirIterator,
    Finalize, VelesChange,
};

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

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
pub enum IndexState {
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

    pub fn reset(&mut self) {
        for item in self.index.values_mut() {
            (*item).state = IndexState::Untracked;
        }
    }

    pub fn save(&self) -> Result<(), VelesError> {
        let data = bincode::serialize(&self)?;
        fs::write(".veles/index", data)?;

        Ok(())
    }
}

pub struct ChangeListEntry {
    pub state: IndexState,
    pub path: PathBuf,
}

pub struct VelesClient {
    config: VelesConfig,
}

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

        let config_path = repo_path.join("config");
        let config = VelesConfig::load(&config_path)?;

        Ok(VelesClient { config })
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

    pub fn config(&mut self, username: Option<String>) -> Result<&VelesConfig, VelesError> {
        if username.is_some() {
            let path = PathBuf::from(".veles/config");
            self.config.user.name = username;
            self.config.save(&path)?;
        }

        Ok(&self.config)
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

    pub fn submit(&self, description: String) -> Result<i64, VelesError> {
        let mut index = VelesIndex::load()?;
        let transport = LocalTransport::new()?;

        let added: Vec<&PathBuf> = index
            .index
            .iter()
            .filter(|(_, value)| value.state == IndexState::Added)
            .map(|(key, _)| key)
            .collect();

        let owner = self.config.user.name.clone().unwrap_or_default();

        let mut changes = Vec::new();
        for path in added {
            let mut writer = transport.send_object()?;
            let mut reader = File::open(path)?;

            std::io::copy(&mut reader, &mut writer)?;

            let hash = writer.finalize()?;
            changes.push((path.to_string_lossy().to_string(), hash));
        }

        let changeset = Changeset {
            owner,
            description,
            changes,
        };

        let changeset_id = transport.submit(&changeset)?;

        index.reset();
        index.save()?;
        
        Ok(changeset_id)
    }

    pub fn changes(&self) -> Result<Vec<ChangeListEntry>, VelesError> {
        let index = VelesIndex::load()?;
        let mut result = Vec::new();

        let iter = DirIterator::from_ignorefile(".", ".velesignore", false)?;
        for path in iter {
            if let Some(indexed) = index.index.get(&path) {
                result.push(ChangeListEntry {
                    state: indexed.state,
                    path,
                });
            }
        }

        index.save()?;

        Ok(result)
    }

    pub fn sync(&self) -> Result<(), VelesError> {
        let index = VelesIndex::load()?;
        let transport = LocalTransport::new()?;

        Ok(())
    }

    pub fn changesets(&self) -> Result<Vec<VelesChange>, VelesError> {
        let transport = LocalTransport::new()?;
        transport.changesets()
    }
}
